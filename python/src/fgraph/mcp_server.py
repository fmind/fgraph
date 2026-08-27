"""Official MCP SDK adapter for auditable fgraph memory."""

from __future__ import annotations

import os
import signal
import subprocess
import threading
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from functools import wraps
from math import isfinite
from typing import Any, BinaryIO, cast
from urllib.parse import quote, urlencode

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ResourceError, ToolError
from mcp.types import ToolAnnotations

from fgraph.errors import FGraphError, ReadOnly, TooLarge
from fgraph.errors import TypeError as FGraphTypeError
from fgraph.models import TxReport
from fgraph.store import GENESIS_TX, Db
from fgraph.values import INT64_MAX, MAX_VALUE_BYTES, _canonical_json_document

_EMBED_TIMEOUT_SECONDS = 60.0
_MAX_TOOL_ITEMS = 100
_MAX_ENTITY_ATTRIBUTES = 32
_MAX_RESPONSE_BYTES = 256 * 1024


def _tool_errors[**P, R](handler: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """Expose only fgraph's deliberate typed failures through the MCP tool boundary."""

    @wraps(handler)
    async def guarded(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await handler(*args, **kwargs)
        except FGraphError as exc:
            raise ToolError(str(exc)) from exc

    return guarded


def _resource_errors[**P, R](handler: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """Expose only fgraph's deliberate typed failures through the MCP resource boundary."""

    @wraps(handler)
    async def guarded(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await handler(*args, **kwargs)
        except FGraphError as exc:
            raise ResourceError(str(exc)) from exc

    return guarded


def _basis(db: Db) -> int:
    return db._as_of if db._as_of is not None else db._latest_tx()  # noqa: SLF001


def _tool_result(value: Any, basis: int) -> dict[str, Any]:
    if hasattr(value, "to_dict"):
        value = value.to_dict()
    envelope = {"ok": True, "basis_tx": basis, "data": value}
    if len(_canonical_json_document(envelope).encode()) > _MAX_RESPONSE_BYTES:
        raise TooLarge("MCP response exceeds 256 KiB; narrow the request or continue with a cursor")
    return envelope


def _report_basis(report: TxReport) -> int:
    """Return the basis actually established or checked by a mutation."""
    return report.tx if report.tx is not None else report.basis_tx


def _pinned_view(db: Db) -> tuple[Db, int]:
    """Pin a read before evaluating it so its envelope cannot race a writer."""
    basis = _basis(db)
    return db.at(basis), basis


def _client_name(ctx: Context[Any, Any] | None) -> str:
    if ctx is None:
        return "unknown"
    try:
        params = ctx.session.client_params
    except ValueError:
        params = None
    return "unknown" if params is None else params.client_info.name


def _resource_result(value: dict[str, Any]) -> dict[str, Any]:
    if len(_canonical_json_document(value).encode()) > _MAX_RESPONSE_BYTES:
        raise TooLarge("MCP resource exceeds 256 KiB; request a narrower page")
    return value


def _compact_entity(value: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for attribute in sorted(value)[:_MAX_ENTITY_ATTRIBUTES]:
        item = value[attribute]
        result[attribute] = item[:32] if isinstance(item, list) else item
    return result


def _selector(value: str) -> Any:
    if value.lstrip("-").isdigit():
        return int(value)
    try:
        return {"eid": str(uuid.UUID(value))}
    except ValueError:
        return value


def _page_cursor(db: Db, resource: str, basis: int, offset: int, **arguments: Any) -> str:
    return db._encode_cursor(  # noqa: SLF001
        {"resource": resource, "basis": basis, "offset": offset, "arguments": arguments}
    )


def _read_page_cursor(db: Db, cursor: str | None, resource: str, **arguments: Any) -> tuple[int | None, int]:
    if cursor is None:
        return None, 0
    payload = db._decode_cursor(cursor)  # noqa: SLF001
    if payload.get("resource") != resource or payload.get("arguments") != arguments:
        raise FGraphTypeError(f"{resource} cursor does not match this resource request; restart pagination")
    basis, offset = payload.get("basis"), payload.get("offset")
    if (
        not isinstance(basis, int)
        or isinstance(basis, bool)
        or not GENESIS_TX <= basis <= _basis(db)
        or not isinstance(offset, int)
        or isinstance(offset, bool)
        or not 0 <= offset <= INT64_MAX
    ):
        raise FGraphTypeError(f"{resource} cursor is invalid; restart pagination")
    return basis, offset


def _changes_cursor(db: Db, basis: int, position: int, since: str) -> str:
    return db._encode_cursor(  # noqa: SLF001
        {
            "resource": "changes",
            "basis": basis,
            "position": position,
            "arguments": {"since": since},
        }
    )


def _read_changes_cursor(db: Db, cursor: str | None, since: str) -> tuple[int | None, int | None]:
    if cursor is None:
        return None, None
    payload = db._decode_cursor(cursor)  # noqa: SLF001
    if payload.get("resource") != "changes" or payload.get("arguments") != {"since": since}:
        raise FGraphTypeError("changes cursor does not match this resource request; restart pagination")
    basis, position = payload.get("basis"), payload.get("position")
    if (
        not isinstance(basis, int)
        or isinstance(basis, bool)
        or not GENESIS_TX <= basis <= _basis(db)
        or not isinstance(position, int)
        or isinstance(position, bool)
        or not int(since) <= position <= basis
    ):
        raise FGraphTypeError("changes cursor is invalid; restart pagination")
    return basis, position


def _continued_resource_uri(authority: str, cursor: str, *, path: str | None = None, **arguments: Any) -> str:
    suffix = "" if path is None else f"/{quote(path, safe='')}"
    query = [(name, str(value)) for name, value in arguments.items() if value is not None]
    query.append(("cursor", cursor))
    return f"fgraph://{authority}{suffix}?{urlencode(query)}"


def _schema_page(
    snapshot: dict[str, Any],
    offset: int,
    limit: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int | None]:
    attributes = cast(list[dict[str, Any]], snapshot["attributes"])
    shapes = cast(list[dict[str, Any]], snapshot["shapes"])
    total = len(attributes) + len(shapes)
    if offset > total:
        raise FGraphTypeError("schema cursor is outside this snapshot; restart pagination")

    attribute_offset = min(offset, len(attributes))
    page_attributes = attributes[attribute_offset : attribute_offset + limit]
    remaining = limit - len(page_attributes)
    shape_offset = max(0, offset - len(attributes))
    page_shapes = shapes[shape_offset : shape_offset + remaining]
    next_offset = offset + len(page_attributes) + len(page_shapes)
    return page_attributes, page_shapes, next_offset if next_offset < total else None


def _kill(process: subprocess.Popen[bytes]) -> None:
    # The embedder owns its isolated POSIX session, so helpers inheriting the
    # stdout pipe cannot outlive the same deadline and hold this request open.
    if os.name == "nt":
        # A Windows process group lets CTRL+BREAK reach descendants that still
        # own the stdout pipe after the configured command has exited.
        with suppress(OSError):
            os.kill(process.pid, signal.CTRL_BREAK_EVENT)
    elif hasattr(os, "killpg"):
        with suppress(OSError):
            os.killpg(process.pid, signal.SIGKILL)
    # This terminates the configured command and is harmless after a group kill.
    with suppress(OSError):
        process.kill()


def _embed_output(arguments: list[str], text: str) -> tuple[int, str]:
    windows_group = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    try:
        process = subprocess.Popen(  # noqa: S603
            arguments,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            creationflags=windows_group,
            start_new_session=os.name != "nt",
        )
    except OSError as exc:
        raise FGraphTypeError(
            f"embed command executable {arguments[0]!r} could not be started; check its path and permissions"
        ) from exc

    timed_out = threading.Event()
    write_error: list[OSError] = []

    def write_input() -> None:
        try:
            if process.stdin is not None:
                process.stdin.write(text.encode())
        except BrokenPipeError:
            pass
        except OSError as exc:
            write_error.append(exc)
        finally:
            if process.stdin is not None:
                process.stdin.close()

    def expire() -> None:
        timed_out.set()
        _kill(process)

    writer = threading.Thread(target=write_input, name="fgraph-embed-stdin", daemon=True)
    timer = threading.Timer(_EMBED_TIMEOUT_SECONDS, expire)
    writer.start()
    timer.start()
    try:
        stdout = cast(BinaryIO, process.stdout)
        # One byte beyond the contract is enough to reject and terminate a
        # continuously emitting child without buffering unbounded output.
        output = stdout.read(MAX_VALUE_BYTES + 1)
        oversized = len(output) > MAX_VALUE_BYTES
        if oversized:
            _kill(process)
        returncode = process.wait()
    finally:
        timer.cancel()
        writer.join()
        if process.stdout is not None:
            process.stdout.close()
        if process.poll() is None:
            _kill(process)
            process.wait()

    if oversized:
        raise FGraphTypeError("embed command output exceeds 1 MiB; emit one compact embedding vector")
    if timed_out.is_set():
        raise FGraphTypeError("embed command timed out after 60 seconds; use a bounded local embedder")
    if write_error:
        raise FGraphTypeError("embed command could not read the input text; check the local embedder") from write_error[
            0
        ]
    try:
        rendered = output.decode()
    except UnicodeDecodeError as exc:
        raise FGraphTypeError("embed command output is not UTF-8 JSON; emit one JSON float array") from exc
    return returncode, rendered


def embed(command: str, text: str) -> list[float]:
    """Run an explicitly configured local embedding command."""
    from fgraph.jsonio import loads

    stripped = command.strip()
    if not stripped:
        raise FGraphTypeError("embed command is empty; provide an executable that reads text and returns a JSON vector")
    if stripped.startswith("["):
        parsed = loads(stripped, context="embed command argv")
        if not isinstance(parsed, list) or not parsed or any(not isinstance(item, str) or not item for item in parsed):
            raise FGraphTypeError("embed command argv must be a non-empty JSON array of non-empty strings")
        arguments = [cast(str, item) for item in parsed]
    else:
        arguments = [stripped]
    returncode, output = _embed_output(arguments, text)
    if returncode:
        raise FGraphTypeError(
            f"embed command exited {returncode}; make it read stdin and emit one JSON float array on stdout"
        )
    value = loads(output, context="embed command output")
    if not isinstance(value, list) or not value:
        raise FGraphTypeError(
            "embed command output is not a non-empty JSON array; emit float values such as [0.1, -0.2]"
        )
    if any(isinstance(item, bool) or not isinstance(item, (int, float)) for item in value):
        raise FGraphTypeError("embed command output contains a non-number; emit only finite float values")
    result = [float(item) for item in value]
    if not all(isfinite(item) for item in result):
        raise FGraphTypeError("embed command output contains a non-finite number; emit only finite float values")
    return result


def create_server(
    db: Db,
    *,
    read_only: bool = True,
    embed_cmd: str | None = None,
) -> MCPServer[None]:
    """Create the normative MCP server around an open database."""
    server: MCPServer[None] = MCPServer(
        "fgraph",
        title="fgraph temporal memory",
        description="Auditable facts, history, provenance, and hybrid recall in one SQLite file.",
        instructions=(
            "Use fgraph as an auditable temporal fact store. Discover schema first, prefer bounded query/datoms "
            "pages, preserve returned basis_tx for follow-up reads, and supply stable operation_id plus "
            "if_basis_tx for retry-safe writes. The server is read-only unless explicitly started with write access."
        ),
        version="1.0.1",
    )
    read_annotations = ToolAnnotations(
        read_only_hint=True,
        destructive_hint=False,
        idempotent_hint=True,
        open_world_hint=False,
    )
    write_annotations = ToolAnnotations(
        read_only_hint=False,
        destructive_hint=False,
        idempotent_hint=True,
        open_world_hint=False,
    )
    destructive_annotations = ToolAnnotations(
        read_only_hint=False,
        destructive_hint=True,
        idempotent_hint=True,
        open_world_hint=False,
    )

    if not read_only:

        @server.tool(
            description=(
                "Remember structured facts and/or a searchable text note with provenance. "
                "A key updates one stable note while retaining history. "
                "Example: remember(key='preference/language', text='User prefers Go', source='conversation:42')."
            ),
            annotations=write_annotations,
            structured_output=True,
        )
        @_tool_errors
        async def remember(
            operation_id: str,
            facts: Any | None = None,
            text: str | None = None,
            source: str | None = None,
            key: str | None = None,
            if_basis_tx: int | None = None,
            ctx: Context[Any, Any] | None = None,
        ) -> dict[str, Any]:
            has_facts = facts is not None and facts != []
            has_text = text is not None and bool(text.strip())
            if text is not None and not has_text:
                raise FGraphTypeError("remember text is blank; provide meaningful text or structured facts")
            if key is not None and not has_text:
                raise FGraphTypeError("remember key requires text; provide a non-blank text note for the stable key")
            if key is not None and not key:
                raise FGraphTypeError("remember key is empty; provide a stable non-empty entity name")
            if not has_facts and not has_text:
                raise FGraphTypeError("remember needs facts or text; provide at least one memory payload")
            data: list[Any] = []
            if has_facts:
                if isinstance(facts, list) and not (
                    facts and isinstance(facts[0], str) and facts[0] in {"assert", "retract"}
                ):
                    data.extend(facts)
                else:
                    data.append(facts)
            if has_text:
                text_value = text
                note: dict[str, Any] = {"memory/text": text_value}
                if key is not None:
                    note["id"] = key
                if embed_cmd is not None:
                    note["memory/embedding"] = {"vector": embed(embed_cmd, text_value)}
                data.append(note)
            report = db.transact(
                data,
                source=source,
                by=f"mcp:{_client_name(ctx)}",
                operation_id=operation_id,
                if_basis_tx=if_basis_tx,
            )
            return _tool_result(report, _report_basis(report))

        @server.tool(
            description=(
                "Retract a fact, attribute, or whole entity while preserving history. "
                "Example: forget(entity='user', attribute='user/editor', value='vim')."
            ),
            annotations=destructive_annotations,
            structured_output=True,
        )
        @_tool_errors
        async def forget(
            entity: str | int,
            operation_id: str,
            if_basis_tx: int,
            attribute: str | None = None,
            value: Any | None = None,
            ctx: Context[Any, Any] | None = None,
        ) -> dict[str, Any]:
            operation: list[Any] = ["retract", entity]
            if attribute is not None:
                operation.append(attribute)
                if value is not None:
                    operation.append(value)
            report = db.transact(
                operation,
                by=f"mcp:{_client_name(ctx)}",
                operation_id=operation_id,
                if_basis_tx=if_basis_tx,
            )
            return _tool_result(report, _report_basis(report))

        @server.tool(
            description="Undo a transaction by audited compensation. Example: undo(tx=70) restores what tx 70 changed.",
            annotations=destructive_annotations,
            structured_output=True,
        )
        @_tool_errors
        async def undo(
            tx: int,
            operation_id: str,
            if_basis_tx: int,
            ctx: Context[Any, Any] | None = None,
        ) -> dict[str, Any]:
            report = db.undo(
                tx,
                by=f"mcp:{_client_name(ctx)}",
                operation_id=operation_id,
                if_basis_tx=if_basis_tx,
            )
            return _tool_result(report, _report_basis(report))

    @server.tool(
        description="Recall ranked text memories. Example: recall(query='preferred editor', k=5, expand=1).",
        annotations=read_annotations,
        structured_output=True,
    )
    @_tool_errors
    async def recall(query: str, k: int = 10, expand: int = 0) -> dict[str, Any]:
        if not isinstance(query, str) or not query.strip():
            raise FGraphTypeError("recall query is blank; provide text to search")
        if not isinstance(k, int) or isinstance(k, bool) or not 1 <= k <= 20:
            raise FGraphTypeError("recall k must be an integer from 1 through 20")
        if not isinstance(expand, int) or isinstance(expand, bool) or not 0 <= expand <= 2:
            raise FGraphTypeError("recall expand must be an integer from zero through two")
        vector = None if embed_cmd is None else embed(embed_cmd, query)
        result = db.search(
            query,
            vector=vector,
            vector_attribute="memory/embedding" if vector is not None else None,
            k=k,
            expand=expand,
        )
        return _tool_result(result, result.basis_tx)

    @server.tool(
        description="Pull current facts about an entity. Example: about(entity='ada', depth=1).",
        annotations=read_annotations,
        structured_output=True,
    )
    @_tool_errors
    async def about(entity: Any, depth: int = 1) -> dict[str, Any]:
        if not isinstance(depth, int) or isinstance(depth, bool) or not 0 <= depth <= 2:
            raise FGraphTypeError("about depth must be an integer from zero through two")
        view, basis = _pinned_view(db)
        return _tool_result(_compact_entity(view.entity(entity, depth)), basis)

    @server.tool(
        description="Explain current facts with full provenance. Example: why(entity='ada', attribute='person/city').",
        annotations=read_annotations,
        structured_output=True,
    )
    @_tool_errors
    async def why(entity: Any, attribute: str | None = None, limit: int = 100) -> dict[str, Any]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= _MAX_TOOL_ITEMS:
            raise FGraphTypeError(f"why limit must be 1 through {_MAX_TOOL_ITEMS}")
        view, basis = _pinned_view(db)
        rows = view.why(entity, attribute)
        return _tool_result({"items": rows[:limit], "truncated": len(rows) > limit}, basis)

    @server.tool(
        description="Read a fact timeline. Example: history(entity='ada', attribute='person/city', limit=100).",
        annotations=read_annotations,
        structured_output=True,
    )
    @_tool_errors
    async def history(entity: Any, attribute: str | None = None, limit: int = 100) -> dict[str, Any]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= _MAX_TOOL_ITEMS:
            raise FGraphTypeError(f"history limit must be 1 through {_MAX_TOOL_ITEMS}")
        view, basis = _pinned_view(db)
        rows = view.history(entity, attribute)
        return _tool_result({"items": rows[:limit], "truncated": len(rows) > limit}, basis)

    @server.tool(
        description=(
            "Run canonical JSON Datalog. Example: query(q={'find':['?n'],'where':[['?e','person/name','?n']]})."
        ),
        annotations=read_annotations,
        structured_output=True,
    )
    @_tool_errors
    async def query(q: dict[str, Any], args: dict[str, Any] | None = None) -> dict[str, Any]:
        bounded = dict(q)
        limit = bounded.setdefault("limit", _MAX_TOOL_ITEMS)
        if not isinstance(limit, int) or isinstance(limit, bool) or not 0 <= limit <= 1000:
            raise FGraphTypeError("MCP query limit must be an integer from zero through 1000")
        view, basis = _pinned_view(db)
        return _tool_result(view.q(bounded, args), basis)

    @server.tool(
        description=(
            "Page low-level datoms by index prefix. "
            "Example: datoms(index='avet', components=['person/name'], source='current', limit=100)."
        ),
        annotations=read_annotations,
        structured_output=True,
    )
    @_tool_errors
    async def datoms(
        index: str = "eavt",
        components: list[Any] | None = None,
        source: str = "current",
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= _MAX_TOOL_ITEMS:
            raise FGraphTypeError(f"MCP datoms limit must be an integer from 1 through {_MAX_TOOL_ITEMS}")
        page = db.datoms(index, components or (), source=source, limit=limit, cursor=cursor)
        return _tool_result(page, int(page["basis_tx"]))

    @server.tool(
        description="Read a stable transaction and operation receipt. Example: receipt(tx=70).",
        annotations=read_annotations,
        structured_output=True,
    )
    @_tool_errors
    async def receipt(tx: int) -> dict[str, Any]:
        view, basis = _pinned_view(db)
        return _tool_result(view.receipt(tx), basis)

    @server.tool(
        description=(
            "Discover known attributes, observed types, and effective schema behavior. "
            "Example: schema(prefix='person/', include_system=False)."
        ),
        annotations=read_annotations,
        structured_output=True,
    )
    @_tool_errors
    async def schema(
        prefix: str | None = None,
        include_system: bool = False,
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= _MAX_TOOL_ITEMS:
            raise FGraphTypeError(f"schema limit must be an integer from 1 through {_MAX_TOOL_ITEMS}")
        basis, offset = _read_page_cursor(
            db,
            cursor,
            "schema-tool",
            prefix=prefix,
            include_system=include_system,
        )
        if basis is None:
            basis = _basis(db)
        snapshot = db.at(basis).schema(prefix, include_system=include_system)
        attributes, shapes, next_offset = _schema_page(snapshot, offset, limit)
        next_cursor = None
        if next_offset is not None:
            next_cursor = _page_cursor(
                db,
                "schema-tool",
                basis,
                next_offset,
                prefix=prefix,
                include_system=include_system,
            )
        data = {
            **snapshot,
            "attributes": attributes,
            "shapes": shapes,
            "next_cursor": next_cursor,
            "truncated": next_cursor is not None,
        }
        return _tool_result(data, basis)

    @server.tool(
        description=(
            "Explain a Datalog access plan without evaluating it. "
            "Example: explain(q={'find':['?n'],'where':[['?e','person/name','?n']]})."
        ),
        annotations=read_annotations,
        structured_output=True,
    )
    @_tool_errors
    async def explain(q: dict[str, Any], args: dict[str, Any] | None = None) -> dict[str, Any]:
        view, basis = _pinned_view(db)
        return _tool_result(view.explain(q, args), basis)

    @server.resource(
        "fgraph://schema{?prefix,cursor}",
        name="fgraph schema",
        description="Bounded, basis-pinned schema snapshot pages.",
        mime_type="application/json",
    )
    @_resource_errors
    async def schema_resource(prefix: str | None = None, cursor: str | None = None) -> dict[str, Any]:
        basis, offset = _read_page_cursor(db, cursor, "schema", prefix=prefix)
        if basis is None:
            basis = _basis(db)
        view = db.at(basis)
        snapshot = view.schema(prefix)
        attributes, shapes, next_offset = _schema_page(snapshot, offset, _MAX_TOOL_ITEMS)
        result = {
            "basis_tx": basis,
            "digest": snapshot["digest"],
            "attributes": attributes,
            "shapes": shapes,
        }
        if next_offset is not None:
            next_cursor = _page_cursor(db, "schema", basis, next_offset, prefix=prefix)
            result["next_uri"] = _continued_resource_uri("schema", next_cursor, prefix=prefix)
        return _resource_result(result)

    @server.resource(
        "fgraph://entity/{selector}{?at,cursor}",
        name="fgraph entity",
        description="Bounded entity attributes at a pinned temporal basis.",
        mime_type="application/json",
    )
    @_resource_errors
    async def entity_resource(selector: str, at: str | None = None, cursor: str | None = None) -> dict[str, Any]:
        view = db if at is None else db.at(int(at) if at.lstrip("-").isdigit() else at)
        page = view.datoms(
            "eavt",
            [_selector(selector)],
            source="current",
            limit=_MAX_TOOL_ITEMS,
            cursor=cursor,
        )
        result = {"basis_tx": page["basis_tx"], "items": page["items"]}
        if page["next_cursor"] is not None:
            result["next_uri"] = _continued_resource_uri(
                "entity",
                cast(str, page["next_cursor"]),
                path=selector,
                at=at,
            )
        return _resource_result(result)

    @server.resource(
        "fgraph://tx/{selector}",
        name="fgraph transaction",
        description="One transaction receipt and bounded metadata.",
        mime_type="application/json",
    )
    @_resource_errors
    async def transaction_resource(selector: str) -> dict[str, Any]:
        view, _basis_tx = _pinned_view(db)
        transaction = view._resolve_read(_selector(selector))  # noqa: SLF001
        if transaction is None:
            raise FGraphTypeError(f"transaction {selector!r} was not found")
        receipt = view.receipt(transaction)
        facts = receipt["facts"]
        receipt["facts"] = facts[:_MAX_TOOL_ITEMS]
        receipt["truncated"] = len(facts) > _MAX_TOOL_ITEMS
        return _resource_result(receipt)

    @server.resource(
        "fgraph://changes{?since,cursor}",
        name="fgraph changes",
        description="Bounded transaction-event pages after a pinned boundary.",
        mime_type="application/json",
    )
    @_resource_errors
    async def changes_resource(since: str = "64", cursor: str | None = None) -> dict[str, Any]:
        if not since.isascii() or not since.isdigit():
            raise FGraphTypeError("changes since must be a transaction at or after genesis")
        try:
            boundary = int(since)
        except ValueError as exc:
            raise FGraphTypeError("changes since must be a transaction at or after genesis") from exc
        if not GENESIS_TX <= boundary <= INT64_MAX:
            raise FGraphTypeError("changes since must be a transaction at or after genesis")
        basis, position = _read_changes_cursor(db, cursor, since)
        if basis is None:
            basis = _basis(db)
            position = boundary
        rows = db._connection.execute(  # noqa: SLF001
            "SELECT tx FROM fgraph_events WHERE tx>? AND tx<=? ORDER BY tx LIMIT ?",
            (position, basis, _MAX_TOOL_ITEMS + 1),
        ).fetchall()
        records = [db._event_record_for_tx(int(row["tx"])) for row in rows]  # noqa: SLF001
        next_cursor = None
        if len(records) > _MAX_TOOL_ITEMS:
            next_cursor = _changes_cursor(db, basis, int(rows[_MAX_TOOL_ITEMS - 1]["tx"]), since)
        result = {"basis_tx": basis, "events": records[:_MAX_TOOL_ITEMS]}
        if next_cursor is not None:
            result["next_uri"] = _continued_resource_uri("changes", next_cursor, since=since)
        return _resource_result(result)

    return server


def run(db: Db, *, read_only: bool = True, embed_cmd: str | None = None) -> None:
    """Serve the database on the official MCP stdio transport."""
    if read_only and not db._read_only:  # noqa: SLF001
        raise ReadOnly("MCP read-only mode requires a read-only SQLite connection; reopen with read_only=True")
    create_server(db, read_only=read_only, embed_cmd=embed_cmd).run("stdio")
