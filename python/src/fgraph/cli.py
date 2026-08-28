"""Typer command-line interface shared by every fgraph implementation."""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Annotated, Any, TextIO

import click
import typer
from typer._click.exceptions import Exit as TyperExit
from typer._click.exceptions import UsageError as TyperUsageError

import fgraph
from fgraph.errors import FGraphError
from fgraph.errors import TypeError as FGraphTypeError
from fgraph.jsonio import loads
from fgraph.store import DEFAULT_QUERY_BUDGET, GENESIS_TX, Db
from fgraph.values import _canonical_json_document

app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False, help="Temporal facts in one SQLite file.")

DbOption = Annotated[str | None, typer.Option("--db", envvar="FGRAPH_DB", help="SQLite database path.")]
JsonOption = Annotated[bool, typer.Option("--json", help="Emit canonical machine-readable JSON.")]
FilterOption = Annotated[list[str] | None, typer.Option("--filter", help="JSON [attribute,value], repeatable.")]
EmbedOption = Annotated[str | None, typer.Option("--embed-cmd")]
ArgsOption = Annotated[str | None, typer.Option("--args", help="Canonical JSON object binding query inputs.")]


@dataclass(frozen=True, slots=True)
class _Options:
    db: str = "fgraph.db"
    json_output: bool = False
    query_budget: int = DEFAULT_QUERY_BUDGET


_CURRENT_OPTIONS: ContextVar[_Options | None] = ContextVar("fgraph_cli_options", default=None)


@app.callback()
def _common(
    ctx: typer.Context,
    db: Annotated[str, typer.Option("--db", envvar="FGRAPH_DB", help="SQLite database path.")] = "fgraph.db",
    json_output: JsonOption = False,
    query_budget: Annotated[
        int,
        typer.Option(
            "--query-budget",
            envvar="FGRAPH_QUERY_BUDGET",
            help="Maximum deterministic query work units.",
        ),
    ] = DEFAULT_QUERY_BUDGET,
) -> None:
    """Configure options shared by every command."""
    options = _Options(db=db, json_output=json_output, query_budget=query_budget)
    ctx.obj = options
    _CURRENT_OPTIONS.set(options)


def _options() -> _Options:
    return _CURRENT_OPTIONS.get() or _Options()


def _open(path: str | None, *, read_only: bool = False) -> Db:
    return fgraph.connect(
        path or _options().db,
        read_only=read_only,
        query_budget=_options().query_budget,
    )


def run_mcp(graph: Any, *, read_only: bool, embed_cmd: str | None) -> None:
    """Load the optional MCP runtime only when the command is invoked."""
    from fgraph.mcp_server import run

    run(graph, read_only=read_only, embed_cmd=embed_cmd)


def _emit(value: Any, _json: bool = True) -> None:
    if hasattr(value, "to_dict"):
        value = value.to_dict()
    if _json or _options().json_output:
        typer.echo(_canonical_json_document(value))
    else:
        typer.echo(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _reference(value: str) -> str | int:
    try:
        return int(value)
    except ValueError:
        return value


def _read_argument(value: str) -> str:
    if value == "-":
        return sys.stdin.read()
    if value.startswith("@"):
        path = Path(value[1:])
        try:
            return path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise fgraph.FormatError(
                f"input file {path!s} cannot be read as UTF-8; check the path, permissions, and encoding"
            ) from exc
    return value


@contextmanager
def _input_lines(source: str, *, context: str) -> Iterator[TextIO]:
    if source == "-":
        yield sys.stdin
        return
    try:
        with Path(source).open(encoding="utf-8") as stream:
            yield stream
    except (OSError, UnicodeError) as exc:
        raise fgraph.FormatError(
            f"{context} file {source!r} cannot be read as UTF-8; check the path and permissions"
        ) from exc


def _payloads(value: str, *, context: str) -> list[Any]:
    raw = _read_argument(value)
    stripped = raw.strip()
    if not stripped:
        raise FGraphTypeError(f"{context} is empty; provide JSON inline, via @file, or on stdin with '-'")
    try:
        return [loads(stripped, context=context)]
    except FGraphTypeError:
        lines = [line for line in raw.splitlines() if line.strip()]
        if len(lines) <= 1:
            raise
        return [loads(line, context=f"{context} line {index}") for index, line in enumerate(lines, start=1)]


@contextmanager
def _batch_payloads(value: str, *, context: str) -> Iterator[Iterator[Any]]:
    stream: TextIO
    close_stream = False
    if value == "-":
        stream = sys.stdin
    elif value.startswith("@"):
        path = Path(value[1:])
        try:
            stream = path.open(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise fgraph.FormatError(
                f"input file {path!s} cannot be read as UTF-8; check the path, permissions, and encoding"
            ) from exc
        close_stream = True
    else:
        stream = StringIO(value)
        close_stream = True

    def decoded() -> Iterator[Any]:
        seen = False
        try:
            for line_number, line in enumerate(stream, start=1):
                if not line.strip():
                    continue
                seen = True
                yield loads(line, context=f"{context} line {line_number}")
        except (OSError, UnicodeError) as exc:
            raise fgraph.FormatError(f"{context} cannot be read as UTF-8") from exc
        if not seen:
            raise FGraphTypeError(f"{context} is empty; provide non-empty NDJSON")

    try:
        yield decoded()
    finally:
        if close_stream:
            stream.close()


@app.command("init")
def init_command(db: DbOption = None, json_output: JsonOption = False) -> None:
    """Initialize a database and print its file information."""
    with _open(db) as graph:
        _emit(graph.stats(), json_output)


@app.command()
def info(db: DbOption = None, json_output: JsonOption = False) -> None:
    """Print format and count statistics."""
    with _open(db, read_only=True) as graph:
        _emit(graph.stats(), json_output)


@app.command()
def add(
    data: str,
    operation_id: Annotated[str | None, typer.Option("--operation-id")] = None,
    operation_id_prefix: Annotated[str | None, typer.Option("--operation-id-prefix")] = None,
    batch_size: Annotated[int | None, typer.Option("--batch-size", min=1, max=10_000)] = None,
    if_basis_tx: Annotated[int | None, typer.Option("--if-basis-tx")] = None,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Transact inline JSON, @file JSON, or stdin JSON/NDJSON with '-'."""
    if operation_id is not None and operation_id_prefix is not None:
        raise FGraphTypeError("choose --operation-id for one transaction or --operation-id-prefix for batches")
    if operation_id_prefix is not None and batch_size is None:
        raise FGraphTypeError("--operation-id-prefix requires --batch-size")
    if batch_size is not None:
        _add_batches(
            data,
            batch_size=batch_size,
            operation_id=operation_id,
            operation_id_prefix=operation_id_prefix,
            if_basis_tx=if_basis_tx,
            db=db,
            json_output=json_output,
        )
        return

    payloads = _payloads(data, context="add input")
    if operation_id is not None and len(payloads) > 1:
        raise FGraphTypeError("--operation-id requires one JSON transaction, not NDJSON")
    with _open(db) as graph:
        reports = [
            graph.transact(
                payload,
                operation_id=operation_id,
                if_basis_tx=if_basis_tx,
            ).to_dict()
            for payload in payloads
        ]
        _emit(reports[0] if len(reports) == 1 else reports, json_output)


def _add_batches(
    data: str,
    *,
    batch_size: int,
    operation_id: str | None,
    operation_id_prefix: str | None,
    if_basis_tx: int | None,
    db: str | None,
    json_output: bool,
) -> None:
    with _batch_payloads(data, context="add input") as payloads, _open(db) as graph:
        iterator = iter(payloads)
        batch = []
        for _ in range(batch_size):
            try:
                batch.append(next(iterator))
            except StopIteration:
                break

        single_batch_option = operation_id is not None or if_basis_tx is not None
        if single_batch_option:
            try:
                next(iterator)
            except StopIteration:
                pass
            else:
                option = "--operation-id" if operation_id is not None else "--if-basis-tx"
                raise FGraphTypeError(f"{option} cannot span multiple batches; use idempotent batch operation ids")

        summary: dict[str, Any] = {
            "batches": 0,
            "items": 0,
            "applied": 0,
            "already_applied": 0,
            "noop": 0,
        }
        last: dict[str, Any] | None = None
        while batch:
            index = summary["batches"]
            batch_operation_id = operation_id
            if operation_id_prefix is not None:
                batch_operation_id = f"{operation_id_prefix}:{index:08d}"
            last = graph.transact(
                batch,
                operation_id=batch_operation_id,
                if_basis_tx=if_basis_tx,
            ).to_dict()
            summary["batches"] += 1
            summary["items"] += len(batch)
            summary[last["status"]] += 1
            batch = []
            for _ in range(batch_size):
                try:
                    batch.append(next(iterator))
                except StopIteration:
                    break

        if last is None:
            raise FGraphTypeError("add input is empty; provide non-empty NDJSON")
        summary["basis_tx"] = last["tx"] if last["tx"] is not None else last["basis_tx"]
        summary["tx"] = last["tx"]
        _emit(summary, json_output)


@app.command()
def retract(
    ref: str,
    attr: Annotated[str | None, typer.Argument()] = None,
    value: Annotated[str | None, typer.Argument()] = None,
    operation_id: Annotated[str | None, typer.Option("--operation-id")] = None,
    if_basis_tx: Annotated[int | None, typer.Option("--if-basis-tx")] = None,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Retract an exact fact, attribute, or whole entity."""
    operation: list[Any] = ["retract", _reference(ref)]
    if attr is not None:
        operation.append(attr)
    if value is not None:
        operation.append(loads(value, context="retract value"))
    with _open(db) as graph:
        _emit(
            graph.transact(
                operation,
                operation_id=operation_id,
                if_basis_tx=if_basis_tx,
            ),
            json_output,
        )


@app.command("get")
def get_command(
    ref: str,
    depth: int = 1,
    at: Annotated[int | None, typer.Option("--at", help="Read at a transaction id.")] = None,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Pull one entity, optionally at a historical transaction."""
    with _open(db, read_only=True) as graph:
        target = graph if at is None else graph.at(at)
        _emit(target.entity(_reference(ref), depth), json_output)


@app.command("q")
def query_command(
    query: str,
    args: ArgsOption = None,
    at: Annotated[int | None, typer.Option("--at", help="Query at a transaction id.")] = None,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Run canonical JSON Datalog inline or from @file."""
    raw = _read_argument(query)
    parsed_args = None if args is None else loads(args, context="query args")
    if parsed_args is not None and not isinstance(parsed_args, dict):
        raise FGraphTypeError("query args must be a JSON object mapping input variables to values")
    with _open(db, read_only=True) as graph:
        target = graph if at is None else graph.at(at)
        _emit(target.q(loads(raw, context="query"), parsed_args), json_output)


@app.command()
def explain(
    query: str,
    args: ArgsOption = None,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Explain the actual bounded Datalog plan without evaluating it."""
    parsed_query = loads(_read_argument(query), context="explain query")
    parsed_args = {} if args is None else loads(args, context="explain args")
    if not isinstance(parsed_query, dict) or not isinstance(parsed_args, dict):
        raise FGraphTypeError("explain query and --args must be JSON objects")
    with _open(db, read_only=True) as graph:
        _emit(graph.explain(parsed_query, parsed_args), json_output)


@app.command()
def datoms(
    index: Annotated[str, typer.Argument(help="Index order: eavt, avet, or vaet.")] = "eavt",
    components: Annotated[str, typer.Option("--components", help="JSON index-prefix array.")] = "[]",
    source: Annotated[str, typer.Option("--source")] = "current",
    limit: Annotated[int, typer.Option("--limit")] = 100,
    cursor: Annotated[str | None, typer.Option("--cursor")] = None,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Page current or historical datoms by EAVT, AVET, or VAET."""
    parsed = loads(components, context="datom components")
    if not isinstance(parsed, list):
        raise FGraphTypeError("datoms --components must be a JSON array")
    with _open(db, read_only=True) as graph:
        _emit(graph.datoms(index, parsed, source=source, limit=limit, cursor=cursor), json_output)


@app.command()
def search(
    text: str | None = None,
    vector: str | None = None,
    k: int = 10,
    expand: int = 0,
    vector_attribute: Annotated[str | None, typer.Option("--vector-attribute")] = None,
    text_attribute: Annotated[list[str] | None, typer.Option("--text-attribute")] = None,
    filter: FilterOption = None,  # noqa: A002
    embed_cmd: EmbedOption = None,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Run keyword, semantic, or hybrid search."""
    parsed_vector = loads(vector, context="search vector") if vector is not None else None
    if parsed_vector is None and embed_cmd is not None and text is not None:
        if not text.strip():
            raise FGraphTypeError("search text is blank; provide text before invoking --embed-cmd")
        # MCP is optional for ordinary CLI use, so keep its sizeable SDK out of
        # every init/get/query process unless embedding was explicitly requested.
        from fgraph.mcp_server import embed

        parsed_vector = embed(embed_cmd, text)
    filters = [loads(item, context="search filter") for item in filter or []]
    with _open(db, read_only=True) as graph:
        _emit(
            graph.search(
                text,
                parsed_vector,
                k,
                expand,
                filters,
                vector_attribute,
                text_attribute or (),
            ),
            json_output,
        )


@app.command()
def history(
    ref: str,
    attr: Annotated[str | None, typer.Argument()] = None,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Print an entity or attribute timeline."""
    with _open(db, read_only=True) as graph:
        _emit(graph.history(_reference(ref), attr), json_output)


@app.command()
def why(
    ref: str,
    attr: Annotated[str | None, typer.Argument()] = None,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Explain current facts with full provenance."""
    with _open(db, read_only=True) as graph:
        _emit(graph.why(_reference(ref), attr), json_output)


@app.command("tx")
def transaction_receipt(
    transaction: int,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Print one durable operation/event receipt."""
    with _open(db, read_only=True) as graph:
        _emit(graph.receipt(transaction), json_output)


@app.command()
def diff(t1: int, t2: int, db: DbOption = None, json_output: JsonOption = False) -> None:
    """Print facts asserted/retracted in a transaction window."""
    with _open(db, read_only=True) as graph:
        _emit(graph.diff(t1, t2), json_output)


@app.command()
def declare(
    attr: str,
    type: str | None = typer.Option(None, "--type"),  # noqa: A002
    ref: bool = False,
    many: bool | None = typer.Option(None, "--many/--one"),
    unique: bool | None = typer.Option(None, "--unique/--not-unique"),
    nohistory: bool | None = typer.Option(None, "--nohistory/--history"),
    dims: int | None = None,
    doc: str | None = None,
    vector_model: Annotated[str | None, typer.Option("--vector-model")] = None,
    operation_id: Annotated[str | None, typer.Option("--operation-id")] = None,
    if_basis_tx: Annotated[int | None, typer.Option("--if-basis-tx")] = None,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Declare optional attribute behavior."""
    with _open(db) as graph:
        _emit(
            graph.declare(
                attr,
                type=type,
                ref=ref,
                many=many,
                unique=unique,
                nohistory=nohistory,
                dims=dims,
                doc=doc,
                vector_model=vector_model,
                operation_id=operation_id,
                if_basis_tx=if_basis_tx,
            ),
            json_output,
        )


@app.command("shape")
def shape_command(
    name: str,
    required: Annotated[list[str] | None, typer.Option("--required")] = None,
    allowed: Annotated[list[str] | None, typer.Option("--allowed")] = None,
    closed: Annotated[bool, typer.Option("--closed/--open")] = False,
    operation_id: Annotated[str | None, typer.Option("--operation-id")] = None,
    if_basis_tx: Annotated[int | None, typer.Option("--if-basis-tx")] = None,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Create or replace a required/allowed attribute shape."""
    with _open(db) as graph:
        _emit(
            graph.declare_shape(
                name,
                required=required or (),
                allowed=allowed or (),
                closed=closed,
                operation_id=operation_id,
                if_basis_tx=if_basis_tx,
            ),
            json_output,
        )


@app.command("validate")
def validate_command(
    ref: str,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Validate one entity against its assigned shapes."""
    with _open(db, read_only=True) as graph:
        _emit(graph.validate(_reference(ref)), json_output)


@app.command("schema")
def schema_command(
    prefix: Annotated[str | None, typer.Argument()] = None,
    system: Annotated[bool, typer.Option("--system", help="Include reserved fgraph attributes.")] = False,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """List known attributes and their effective behavior."""
    with _open(db, read_only=True) as graph:
        _emit(graph.schema(prefix, include_system=system), json_output)


@app.command("schema-export")
def schema_export(db: DbOption = None, json_output: JsonOption = False) -> None:
    """Export portable schema/1 declarations and shapes."""
    with _open(db, read_only=True) as graph:
        _emit(graph.schema_manifest(), json_output)


@app.command("schema-check")
def schema_check(source: str, db: DbOption = None, json_output: JsonOption = False) -> None:
    """Compare a schema/1 manifest with the database."""
    manifest = loads(_read_argument(source), context="schema manifest")
    with _open(db, read_only=True) as graph:
        _emit(graph.check_schema_manifest(manifest), json_output)


@app.command("schema-apply")
def schema_apply(
    source: str,
    operation_id: Annotated[str | None, typer.Option("--operation-id")] = None,
    if_basis_tx: Annotated[int | None, typer.Option("--if-basis-tx")] = None,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Atomically apply a schema/1 manifest."""
    manifest = loads(_read_argument(source), context="schema manifest")
    with _open(db) as graph:
        _emit(
            graph.apply_schema_manifest(manifest, operation_id=operation_id, if_basis_tx=if_basis_tx),
            json_output,
        )


@app.command("apply")
def apply_command(
    source: Annotated[str, typer.Argument()] = "-",
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Idempotently apply portable event/1 NDJSON."""
    with _input_lines(source, context="event stream") as lines, _open(db) as graph:
        _emit(graph.apply_summary(lines), json_output)


@app.command("snapshot")
def snapshot_command(db: DbOption = None) -> None:
    """Write a checksummed exact logical snapshot to stdout."""
    with _open(db, read_only=True) as graph:
        graph.snapshot(sys.stdout)


@app.command("restore")
def restore_command(
    source: Annotated[str, typer.Argument()] = "-",
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Restore a snapshot/1 stream into a pristine database."""
    with _input_lines(source, context="snapshot") as lines, _open(db) as graph:
        graph.restore(lines)
        _emit({"ok": True, "basis_tx": graph.schema()["basis_tx"]}, json_output)


@app.command()
def undo(
    tx: int,
    operation_id: Annotated[str | None, typer.Option("--operation-id")] = None,
    if_basis_tx: Annotated[int | None, typer.Option("--if-basis-tx")] = None,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Apply an audited compensating transaction."""
    with _open(db) as graph:
        _emit(
            graph.undo(
                tx,
                operation_id=operation_id,
                if_basis_tx=if_basis_tx,
            ),
            json_output,
        )


@app.command()
def excise(
    ref: str,
    operation_id: Annotated[str, typer.Option("--operation-id")],
    if_basis_tx: Annotated[int, typer.Option("--if-basis-tx")],
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Irreversibly erase one application entity with an idempotent CAS receipt."""
    with _open(db) as graph:
        _emit(
            graph.excise(
                _reference(ref),
                operation_id=operation_id,
                if_basis_tx=if_basis_tx,
            ),
            json_output,
        )


@app.command()
def tail(
    since: int = GENESIS_TX,
    follow: bool = False,
    db: DbOption = None,
) -> None:
    """Stream portable event/1 NDJSON."""
    with _open(db, read_only=True) as graph:
        if follow:
            for record in graph.follow(since):
                typer.echo(_canonical_json_document(record))
        else:
            for _transaction, record in graph._iter_event_records(since):  # noqa: SLF001
                typer.echo(_canonical_json_document(record))


@app.command()
def backup(dest: str, db: DbOption = None, json_output: JsonOption = False) -> None:
    """Create a consistent hot backup."""
    with _open(db, read_only=True) as graph:
        graph.backup(dest)
    _emit({"path": dest}, json_output)


@app.command()
def doctor(
    repair: Annotated[
        bool, typer.Option("--repair", help="Transactionally rebuild FTS and remove orphan blobs.")
    ] = False,
    db: DbOption = None,
    json_output: JsonOption = False,
) -> None:
    """Check integrity without mutation unless --repair is explicit."""
    with _open(db, read_only=not repair) as graph:
        _emit(graph.doctor(repair=repair), json_output)


@app.command("mcp")
def mcp_command(
    write: bool = typer.Option(False, "--write", help="Opt in to remember/forget/undo mutation tools."),
    embed_cmd: str | None = typer.Option(None, "--embed-cmd"),
    db: DbOption = None,
) -> None:
    """Serve the normative MCP tools over stdio."""
    with _open(db, read_only=not write) as graph:
        run_mcp(graph, read_only=not write, embed_cmd=embed_cmd)


@app.command()
def version() -> None:
    """Print the implementation version."""
    typer.echo(fgraph.__version__)


def main() -> None:
    """Run Typer and map typed fgraph failures to exit code 1."""
    try:
        app(standalone_mode=False)
    except (click.UsageError, TyperUsageError) as exc:
        exc.show()
        raise SystemExit(2) from exc
    except (click.exceptions.Exit, TyperExit) as exc:
        raise SystemExit(exc.exit_code) from exc
    except FGraphError as exc:
        typer.echo(f"{type(exc).__name__}: {exc}", err=True)
        raise SystemExit(1) from exc
