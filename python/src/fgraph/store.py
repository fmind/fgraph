"""SQLite persistence and the public fgraph database API."""

from __future__ import annotations

import base64
import binascii
import contextlib
import hashlib
import json
import math
import os
import re
import sqlite3
import tempfile
import time
import unicodedata
import uuid
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self, TextIO, cast
from urllib.parse import quote

from fgraph.errors import (
    Conflict,
    FormatError,
    NotFound,
    QueryError,
    ReadOnly,
    SchemaError,
    TooLarge,
    Unsupported,
)
from fgraph.errors import (
    TypeError as FGraphTypeError,
)
from fgraph.models import Result, SearchResult, TxReport
from fgraph.values import (
    ATTRIBUTE_PATTERN,
    BLOB_THRESHOLD,
    BOOL,
    BYTES,
    BYTES_REF,
    FLOAT,
    INSTANT,
    INSTANT_MAX,
    INSTANT_MIN,
    INT,
    INT64_MAX,
    INT64_MIN,
    JSON,
    MAX_VALUE_BYTES,
    REF,
    TEXT,
    TEXT_REF,
    TYPE_NAMES,
    VECTOR,
    Cell,
    Encoded,
    _canonical_json_document,
    canonical_json,
    encode,
    indirect_digest,
    type_name,
    value_matches,
    wire_value,
)

APPLICATION_ID = 0x66677261
FORMAT_VERSION = 2
GENESIS_TX = 64
FIRST_USER_ID = 65
DEFAULT_QUERY_BUDGET = 100_000
MAX_EVENT_BYTES = 8 * MAX_VALUE_BYTES + 64 * 1024
_OMITTED = object()
GENESIS_EVENT = uuid.UUID("00000000-0000-4000-8000-000000000040")
_WINDOWS = os.name == "nt"


def _seeded_event_id(seed: str, transaction: int) -> uuid.UUID:
    payload = f"fgraph-event/1\0{seed}\0{transaction}".encode()
    value = bytearray(hashlib.sha256(payload).digest()[:16])
    value[6] = (value[6] & 0x0F) | 0x40
    value[8] = (value[8] & 0x3F) | 0x80
    return uuid.UUID(bytes=bytes(value))


def _derived_entity_id(event: uuid.UUID, ordinal: int) -> uuid.UUID:
    """RFC 4122 UUIDv5 with the frozen unsigned-big-endian ordinal name."""
    if not 0 <= ordinal <= (1 << 64) - 1:
        raise TooLarge("one transaction cannot allocate more than uint64 identities")
    value = bytearray(hashlib.sha1(event.bytes + ordinal.to_bytes(8, "big"), usedforsecurity=False).digest()[:16])
    value[6] = (value[6] & 0x0F) | 0x50
    value[8] = (value[8] & 0x3F) | 0x80
    return uuid.UUID(bytes=bytes(value))


def _canonical_event_data(record: Mapping[str, Any]) -> str:
    """Encode one portable event while enforcing its durable storage bound."""
    try:
        data = _canonical_json_document(record)
        size = len(data.encode())
    except (TypeError, ValueError, UnicodeError) as exc:
        raise FGraphTypeError("event data must be canonical JSON-compatible values") from exc
    if size > MAX_EVENT_BYTES:
        raise TooLarge(
            f"canonical event is {size} bytes; keep one transaction event at or below {MAX_EVENT_BYTES} bytes"
        )
    return data


def _validate_operation_id(value: Any) -> None:
    if value is None:
        return
    try:
        size = len(value.encode()) if isinstance(value, str) else 0
    except UnicodeEncodeError as exc:
        raise FGraphTypeError(
            f"operation_id={value!r} is invalid; use 1-512 UTF-8 bytes without control characters"
        ) from exc
    if not isinstance(value, str) or not 1 <= size <= 512 or any(unicodedata.category(char) == "Cc" for char in value):
        raise FGraphTypeError(f"operation_id={value!r} is invalid; use 1-512 UTF-8 bytes without control characters")


def _portable_selector_key(value: Any) -> tuple[str, str] | None:
    if isinstance(value, str):
        return "name", value
    if isinstance(value, Mapping) and set(value) == {"eid"} and isinstance(value["eid"], str):
        return "eid", value["eid"].lower()
    return None


def _event_mentions_selector(record: Mapping[str, Any], selector: Any) -> bool:
    """Match identity-bearing event positions without inspecting arbitrary values."""
    target = _portable_selector_key(selector)
    if target is None:
        return False

    def matches(candidate: Any) -> bool:
        return _portable_selector_key(candidate) == target

    def references(value: Any, tag: Any) -> bool:
        return tag == "ref" and isinstance(value, Mapping) and set(value) == {"ref"} and matches(value["ref"])

    created = record.get("created")
    if isinstance(created, list) and any(matches(candidate) for candidate in created):
        return True
    for field in ("asserted", "retracted"):
        tuples = record.get(field)
        if not isinstance(tuples, list):
            continue
        for item in tuples:
            if not isinstance(item, list):
                continue
            if len(item) > 0 and matches(item[0]):
                return True
            if len(item) > 1 and matches(item[1]):
                return True
            if len(item) > 3 and references(item[2], item[3]):
                return True
    tx_facts = record.get("tx_facts")
    if not isinstance(tx_facts, list):
        return False
    return any(
        isinstance(item, list)
        and ((len(item) > 0 and matches(item[0])) or (len(item) > 2 and references(item[1], item[2])))
        for item in tx_facts
    )


def _valid_indirect_blob(tag: int, key: Any, data: Any) -> bool:
    if not isinstance(key, bytes) or len(key) != 32:
        return False
    if tag == TEXT_REF:
        if not isinstance(data, str):
            return False
        try:
            raw = data.encode()
        except UnicodeEncodeError:
            return False
        valid_length = BLOB_THRESHOLD < len(raw) <= MAX_VALUE_BYTES
    elif tag == BYTES_REF:
        if not isinstance(data, bytes):
            return False
        raw = data
        valid_length = BLOB_THRESHOLD < len(raw) <= MAX_VALUE_BYTES
    elif tag == VECTOR:
        if not isinstance(data, bytes):
            return False
        raw = data
        valid_length = 0 < len(raw) <= MAX_VALUE_BYTES and len(raw) % 4 == 0
    else:
        return False
    return valid_length and key == indirect_digest(tag, raw)


def _valid_physical_value(tag: int, storage_class: str, scalar: Any, raw: bytes) -> bool:
    """Validate the tag-specific representation stored in the ANY value column."""
    if tag == REF:
        return storage_class == "integer" and isinstance(scalar, int) and scalar > 0
    if tag == BOOL:
        return storage_class == "integer" and scalar in (0, 1)
    if tag == INT:
        return storage_class == "integer" and isinstance(scalar, int)
    if tag == FLOAT:
        return storage_class == "real" and isinstance(scalar, float) and math.isfinite(scalar)
    if tag == TEXT:
        if storage_class != "text" or len(raw) > BLOB_THRESHOLD:
            return False
        try:
            raw.decode()
        except UnicodeDecodeError:
            return False
        return True
    if tag == INSTANT:
        return storage_class == "integer" and isinstance(scalar, int) and INSTANT_MIN <= scalar <= INSTANT_MAX
    if tag == BYTES:
        return storage_class == "blob" and len(raw) <= BLOB_THRESHOLD
    if tag in (VECTOR, TEXT_REF, BYTES_REF):
        # The indirect validator below owns key, blob-domain, length, and digest checks.
        return True
    if tag == JSON:
        if storage_class != "text" or len(raw) > MAX_VALUE_BYTES:
            return False
        try:
            text = raw.decode()
            return canonical_json(json.loads(text)) == text
        except (UnicodeDecodeError, json.JSONDecodeError, FGraphTypeError, TooLarge, RecursionError):
            return False
    return False


SYSTEM_NAMES = (
    "fgraph/at",
    "fgraph/by",
    "fgraph/source",
    "fgraph/meta",
    "fgraph/many",
    "fgraph/unique",
    "fgraph/nohistory",
    "fgraph/type",
    "fgraph/dims",
    "fgraph/doc",
    "fgraph/excised",
    "fgraph/undoes",
    "fgraph/imported-at",
    "fgraph/vector-model",
    "fgraph/shape",
    "fgraph/shape-required",
    "fgraph/shape-allowed",
    "fgraph/shape-closed",
)
SYSTEM_TYPES = (
    "instant",
    "text",
    "text",
    "json",
    "bool",
    "bool",
    "bool",
    "text",
    "int",
    "text",
    "ref",
    "ref",
    "instant",
    "text",
    "ref",
    "ref",
    "ref",
    "bool",
)
SYSTEM_DOCS = (
    "Wall-clock time of the transaction (UTC microseconds).",
    "Author of the transaction (person or agent).",
    "Provenance of the transaction (document, conversation, tool).",
    "Free-form JSON metadata on the transaction.",
    "Schema: attribute holds multiple values per entity.",
    "Schema: live values of this attribute are unique; enables upsert.",
    "Schema: superseded values are deleted instead of kept as history.",
    "Schema: enforced value type (bool,int,float,text,instant,bytes,vector,json,ref).",
    "Schema: vector dimensions for vector attributes.",
    "Schema: human/agent documentation for an attribute.",
    "Audit marker: entity was physically excised at this transaction.",
    "Audit marker: this transaction undoes another transaction.",
    "Original source timestamp retained when an import rebases transaction time.",
    "Schema: opaque identity of the embedding model used by a vector attribute.",
    "Validation: shape assigned to an entity.",
    "Validation: attribute required by a shape.",
    "Validation: attribute allowed by a closed shape.",
    "Validation: reject application attributes not allowed by the shape.",
)

SCHEMA_SQL = """
CREATE TABLE fgraph_meta (
  key TEXT NOT NULL PRIMARY KEY,
  value ANY NOT NULL
) STRICT;
CREATE TABLE fgraph_ids (
  id INTEGER NOT NULL PRIMARY KEY,
  name TEXT UNIQUE,
  gid BLOB UNIQUE,
  created_tx INTEGER NOT NULL,
  CHECK ((name IS NULL) <> (gid IS NULL)),
  CHECK (gid IS NULL OR (typeof(gid) = 'blob' AND length(gid) = 16)),
  CHECK (created_tx >= 64)
) STRICT;
CREATE INDEX fgraph_ids_created ON fgraph_ids (created_tx, id);
CREATE TABLE fgraph_events (
  tx INTEGER NOT NULL PRIMARY KEY,
  event_hash BLOB NOT NULL,
  event_data TEXT,
  operation_id TEXT UNIQUE,
  request_hash BLOB,
  CHECK (typeof(event_hash) = 'blob' AND length(event_hash) = 32),
  CHECK (event_data IS NULL OR typeof(event_data) = 'text'),
  CHECK (request_hash IS NULL OR (typeof(request_hash) = 'blob' AND length(request_hash) = 32)),
  CHECK ((operation_id IS NULL) = (request_hash IS NULL))
) STRICT;
CREATE TABLE fgraph_facts (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  e INTEGER NOT NULL,
  a INTEGER NOT NULL,
  v ANY NOT NULL,
  t INTEGER NOT NULL,
  tx INTEGER NOT NULL,
  rx INTEGER,
  CHECK (t BETWEEN 0 AND 10),
  CHECK (rx IS NULL OR rx > tx)
) STRICT;
CREATE TABLE fgraph_blobs (
  hash BLOB NOT NULL PRIMARY KEY,
  data ANY NOT NULL
) STRICT;
CREATE VIRTUAL TABLE fgraph_fts USING fts5(
  text, tokenize = "unicode61 remove_diacritics 2"
);
CREATE UNIQUE INDEX fgraph_eavt ON fgraph_facts (e, a, v, t) WHERE rx IS NULL;
CREATE INDEX fgraph_avet ON fgraph_facts (a, t, v, e, tx, rx, id);
CREATE INDEX fgraph_vaet ON fgraph_facts (v, a, e) WHERE rx IS NULL AND t = 0;
CREATE INDEX fgraph_hist ON fgraph_facts (e, a, tx);
CREATE INDEX fgraph_txin ON fgraph_facts (tx);
CREATE INDEX fgraph_txout ON fgraph_facts (rx) WHERE rx IS NOT NULL;
CREATE VIEW fgraph_view AS
SELECT f.id, f.e, i.name AS attribute,
       CASE WHEN f.t IN (7, 8, 9)
            THEN (SELECT b.data FROM fgraph_blobs b WHERE b.hash = f.v)
            ELSE f.v END AS value,
       f.t AS tag, f.tx, f.rx
FROM fgraph_facts f JOIN fgraph_ids i ON i.id = f.a;
CREATE VIEW fgraph_now AS SELECT * FROM fgraph_view WHERE rx IS NULL;
"""

_EXPLICIT_SCHEMA_OBJECTS = (
    "fgraph_meta",
    "fgraph_ids",
    "fgraph_events",
    "fgraph_facts",
    "fgraph_blobs",
    "fgraph_fts",
    "fgraph_view",
    "fgraph_now",
    "fgraph_eavt",
    "fgraph_avet",
    "fgraph_vaet",
    "fgraph_hist",
    "fgraph_txin",
    "fgraph_txout",
    "fgraph_ids_created",
)
_FTS_INTERNAL_TABLES = frozenset(
    {
        "fgraph_fts_config",
        "fgraph_fts_content",
        "fgraph_fts_data",
        "fgraph_fts_docsize",
        "fgraph_fts_idx",
    }
)


def _normalize_schema_sql(sql: str | None) -> str:
    """Normalize only irrelevant presentation differences in sqlite_schema SQL."""
    return " ".join((sql or "").split()).lower()


def _reference_layout() -> dict[str, tuple[str, str]]:
    """Build canonical DDL through the same SQLite parser used for the file."""
    reference = sqlite3.connect(":memory:")
    try:
        reference.executescript(SCHEMA_SQL)
        rows = reference.execute("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").fetchall()
        return {
            str(name): (str(object_type), _normalize_schema_sql(sql))
            for name, object_type, sql in rows
            if str(name) in _EXPLICIT_SCHEMA_OBJECTS
        }
    finally:
        reference.close()


@dataclass(slots=True)
class _Assertion:
    e: int
    a: int
    value: Encoded


@dataclass(slots=True)
class _Retraction:
    e: int
    a: int | None = None
    value: Encoded | None = None


@dataclass(slots=True)
class _CompareAndSwap:
    e: int
    a: int
    old: Encoded | None
    new: Encoded | None


@dataclass(slots=True)
class _Schema:
    type: str | None = None
    many: bool = False
    unique: bool = False
    nohistory: bool | None = None
    dims: int | None = None
    doc: str | None = None
    vector_model: str | None = None

    @property
    def deletes_history(self) -> bool:
        """Return the effective no-history behavior."""
        # An inferred dimensions declaration is sufficient to identify an
        # undeclared vector attribute and apply the format's vector default.
        return self.nohistory if self.nohistory is not None else self.type == "vector" or self.dims is not None


@dataclass(slots=True)
class _PendingTransaction:
    operations: list[_Assertion | _Retraction | _CompareAndSwap]
    tx_facts: list[tuple[int, Encoded]]
    report_ids: dict[str, int]
    next_id: int
    names: dict[str, int]
    id_names: dict[int, str]
    allocated: dict[int, str | None]
    schemas: dict[int, _Schema]


class _EnvironmentClock:
    """Deterministic clock whose next tick is derived from persisted receipts."""

    def __init__(self, start: int) -> None:
        self.start = start

    def __call__(self) -> int:
        return self.start


def _wall_clock() -> int:
    return time.time_ns() // 1_000


def _configured_clock(clock: Callable[[], int] | int | None) -> Callable[[], int]:
    if clock is None:
        raw = os.environ.get("FGRAPH_CLOCK")
        if raw is None:
            return _wall_clock
        try:
            start = int(raw)
        except ValueError as exc:
            raise FGraphTypeError(
                f"FGRAPH_CLOCK={raw!r} is not integer microseconds; set an integer such as 1767225600000000"
            ) from exc
        return _EnvironmentClock(start)
    if isinstance(clock, int):
        return _EnvironmentClock(clock)
    return clock


def _configured_event_factory(
    factory: Callable[[], str | uuid.UUID] | None,
) -> Callable[[], str | uuid.UUID]:
    return uuid.uuid4 if factory is None else factory


def connect(
    path: str | os.PathLike[str] = "fgraph.db",
    *,
    clock: Callable[[], int] | int | None = None,
    read_only: bool = False,
    query_budget: int = DEFAULT_QUERY_BUDGET,
    event_factory: Callable[[], str | uuid.UUID] | None = None,
) -> Db:
    """Create or open an fgraph SQLite file.

    Python connections intentionally keep sqlite3's one-connection-per-thread
    guard. Open one ``Db`` per worker thread instead of sharing a handle.
    """
    return Db(
        path,
        clock=clock,
        read_only=read_only,
        query_budget=query_budget,
        event_factory=event_factory,
    )


def restore_backup(
    snapshot: str | os.PathLike[str],
    path: str | os.PathLike[str],
    *,
    clock: Callable[[], int] | int | None = None,
    event_factory: Callable[[], str | uuid.UUID] | None = None,
) -> Db:
    """Install a verified physical backup into a new or empty destination."""
    source = Path(snapshot).expanduser().resolve()
    target = Path(path).expanduser().resolve()
    if source == target:
        raise Conflict("snapshot source and restore destination are the same file; choose a new destination")
    if not source.is_file():
        raise NotFound(f"snapshot {os.fspath(snapshot)!r} was not found; provide an existing fgraph snapshot")
    if target.exists() and target.stat().st_size:
        raise Conflict(f"restore destination {os.fspath(path)!r} is not empty; choose a new or empty file")
    if not target.parent.is_dir():
        raise FormatError(f"restore destination parent {os.fspath(target.parent)!r} does not exist")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".restore", dir=target.parent)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        with connect(source, read_only=True) as source_db:
            destination = sqlite3.connect(temporary)
            try:
                source_db._connection.backup(destination)
            finally:
                destination.close()
        with connect(temporary, read_only=True) as restored:
            report = restored.doctor()
            if report["ok"] is not True:
                raise FormatError(
                    f"snapshot {os.fspath(snapshot)!r} violates format invariants: {report['problems']!r}"
                )
        temporary.replace(target)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return connect(target, clock=clock, event_factory=event_factory)


def _sync_backup_file(path: Path) -> None:
    # Windows FlushFileBuffers rejects the read-only handle produced by "rb".
    with path.open("r+b") as backup_file:
        os.fsync(backup_file.fileno())


def _sync_backup_directory(path: Path) -> None:
    # Windows has no portable directory fsync. The verified file itself is
    # flushed before atomic hard-link publication, so only POSIX needs this.
    if _WINDOWS:
        return
    directory = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class Db:
    """One fgraph connection, optionally pinned to a historical transaction."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        clock: Callable[[], int] | int | None = None,
        read_only: bool = False,
        query_budget: int = DEFAULT_QUERY_BUDGET,
        event_factory: Callable[[], str | uuid.UUID] | None = None,
    ) -> None:
        if not isinstance(query_budget, int) or isinstance(query_budget, bool) or query_budget <= 0:
            raise FGraphTypeError(
                f"query_budget={query_budget!r} is invalid; use a positive work-unit limit such as 100000"
            )
        self.path = os.fspath(path)
        self._read_only = read_only
        self._as_of: int | None = None
        self._owns_connection = True
        self._closed = False
        self._speculation_depth = 0
        self._savepoint_counter = 0
        self._cache_version = -1
        self._names: dict[str, int] = {}
        self._id_names: dict[int, str] = {}
        self._gids: dict[str, int] = {}
        self._id_gids: dict[int, str] = {}
        self._clock_source = clock
        # Validate environment configuration before sqlite3 can create or
        # initialize the requested path.
        self._clock = _configured_clock(clock)
        self._event_seed = os.environ.get("FGRAPH_EVENT_SEED") if event_factory is None else None
        self._event_factory = _configured_event_factory(event_factory)
        self._query_budget = query_budget
        self._connection = self._open_connection()
        try:
            self._prepare_connection()
        except sqlite3.DatabaseError as exc:
            if self._immutable_retry_allowed(exc):
                self._connection.close()
                self._connection = self._open_connection(immutable=True)
                try:
                    self._prepare_connection()
                except sqlite3.DatabaseError as immutable_exc:
                    self._connection.close()
                    self._closed = True
                    raise FormatError(
                        f"file {self.path!r} is not a usable SQLite database; restore a valid file or choose a new path"
                    ) from immutable_exc
                except BaseException:
                    self._connection.close()
                    self._closed = True
                    raise
                return
            self._connection.close()
            self._closed = True
            raise FormatError(
                f"file {self.path!r} is not a usable SQLite database; restore a valid file or choose a new path"
            ) from exc
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def _prepare_connection(self) -> None:
        self._configure_connection()
        self._validate_or_initialize(self._clock)
        self._refresh_cache(force=True)

    def _immutable_retry_allowed(self, error: sqlite3.DatabaseError) -> bool:
        if not self._read_only or getattr(error, "sqlite_errorcode", None) != sqlite3.SQLITE_READONLY_DIRECTORY:
            return False
        absolute = Path(self.path).expanduser().resolve()
        for suffix in ("-wal", "-shm"):
            try:
                os.lstat(f"{absolute}{suffix}")
            except FileNotFoundError:
                continue
            except OSError:
                return False
            return False
        return True

    def _open_connection(self, *, immutable: bool = False) -> sqlite3.Connection:
        if self._read_only:
            if self.path == ":memory:":
                raise ReadOnly(
                    "a read-only :memory: database cannot be initialized; open an existing file with read_only=True"
                )
            absolute = Path(self.path).expanduser().resolve()
            query = "mode=ro&immutable=1" if immutable else "mode=ro"
            uri = f"file:{quote(str(absolute))}?{query}"
            try:
                connection = sqlite3.connect(uri, uri=True, isolation_level=None)
            except sqlite3.OperationalError as exc:
                if not absolute.exists():
                    raise NotFound(
                        f"database {self.path!r} does not exist; initialize it before opening read-only"
                    ) from exc
                raise FormatError(
                    f"database {self.path!r} cannot be opened read-only; check that it is a readable SQLite file"
                ) from exc
        else:
            try:
                connection = sqlite3.connect(self.path, isolation_level=None)
            except sqlite3.OperationalError as exc:
                raise FormatError(
                    f"database {self.path!r} cannot be opened; choose a writable SQLite file path"
                ) from exc
        connection.row_factory = sqlite3.Row
        return connection

    def _configure_connection(self) -> None:
        if not self._read_only and self.path != ":memory:":
            mode = str(self._connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]).lower()
            if mode != "wal":
                raise FormatError(f"SQLite selected journal_mode={mode!r}; use a filesystem that supports WAL")
        self._connection.execute("PRAGMA foreign_keys = OFF")
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._connection.execute("PRAGMA trusted_schema = OFF")
        if not self._read_only:
            self._connection.execute("PRAGMA synchronous = FULL")
            self._connection.execute("PRAGMA wal_autocheckpoint = 1000")
        else:
            self._connection.execute("PRAGMA query_only = ON")
        if int(self._connection.execute("PRAGMA trusted_schema").fetchone()[0]) != 0:
            raise FormatError("SQLite did not disable trusted_schema; use SQLite 3.37 or newer")
        if not self._read_only and int(self._connection.execute("PRAGMA synchronous").fetchone()[0]) != 2:
            raise FormatError("SQLite did not enable synchronous=FULL; use a durable SQLite build")

    def _validate_or_initialize(self, clock: Callable[[], int]) -> bool:
        if self._layout_is_initialized():
            return False
        if self._read_only:
            raise FormatError(f"file {self.path!r} is not initialized as fgraph; open it writable once to initialize")
        return self._initialize(clock())

    def _layout_is_initialized(self) -> bool:
        application_id = int(self._connection.execute("PRAGMA application_id").fetchone()[0])
        user_version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        has_meta = self._connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='fgraph_meta'"
        ).fetchone()
        fgraph_objects = {
            str(row[0]) for row in self._connection.execute("SELECT name FROM sqlite_master WHERE name LIKE 'fgraph_%'")
        }
        if has_meta:
            if application_id != APPLICATION_ID or user_version != FORMAT_VERSION:
                raise FormatError(
                    f"file {self.path!r} has application_id={application_id} and user_version={user_version}; "
                    f"open an fgraph format-v{FORMAT_VERSION} file instead"
                )
            self._validate_objects()
            return True
        # Any marker claims ownership of the file. Initializing over an incomplete
        # claimed layout would hide corruption, so only an unmarked 0/0 file is adoptable.
        if application_id != 0 or user_version != 0:
            raise FormatError(
                f"file {self.path!r} has application_id={application_id}, user_version={user_version} but no complete "
                "fgraph layout; restore the claimed database or use an unmarked SQLite file"
            )
        if fgraph_objects:
            raise FormatError(
                f"file {self.path!r} contains a partial fgraph layout {sorted(fgraph_objects)!r}; "
                "restore a complete file or remove the partial objects intentionally"
            )
        foreign_objects = {
            str(row[0])
            for row in self._connection.execute("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")
        }
        if foreign_objects:
            raise FormatError(
                f"file {self.path!r} already contains application objects {sorted(foreign_objects)!r}; "
                "initialize fgraph in a dedicated empty SQLite file"
            )
        return False

    def _initialize(self, genesis_at: int) -> bool:
        genesis_at = int(encode({"instant": genesis_at}).logical)
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            # Another opener may have initialized the pristine file while this
            # handle waited for the writer lock. Re-read the claimed format and
            # accept only the complete canonical winning layout.
            if self._layout_is_initialized():
                self._connection.execute("COMMIT")
                return False
            for statement in SCHEMA_SQL.split(";"):
                if statement.strip():
                    self._connection.execute(statement)
            self._connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")
            self._connection.execute(f"PRAGMA user_version = {FORMAT_VERSION}")
            self._connection.executemany(
                "INSERT INTO fgraph_ids(id, name, gid, created_tx) VALUES (?, ?, NULL, ?)",
                ((identifier, name, GENESIS_TX) for identifier, name in enumerate(SYSTEM_NAMES, start=1)),
            )
            self._connection.execute(
                "INSERT INTO fgraph_ids(id, name, gid, created_tx) VALUES (?, NULL, ?, ?)",
                (GENESIS_TX, GENESIS_EVENT.bytes, GENESIS_TX),
            )
            self._connection.executemany(
                "INSERT INTO fgraph_meta(key, value) VALUES (?, ?)",
                (("next_id", FIRST_USER_ID), ("created_at", genesis_at)),
            )
            self._insert_raw_fact(GENESIS_TX, 1, Encoded(INSTANT, genesis_at, genesis_at), GENESIS_TX)
            for attribute, declared_type in enumerate(SYSTEM_TYPES, start=1):
                self._insert_raw_fact(attribute, 8, Encoded(TEXT, declared_type, declared_type), GENESIS_TX)
            for attribute, doc in enumerate(SYSTEM_DOCS, start=1):
                self._insert_raw_fact(attribute, 10, Encoded(TEXT, doc, doc), GENESIS_TX)
            for attribute in (16, 17):
                self._insert_raw_fact(attribute, 5, Encoded(BOOL, 1, True), GENESIS_TX)
            genesis_record = {
                "asserted": [],
                "at": genesis_at,
                "created": list(SYSTEM_NAMES),
                "event": str(GENESIS_EVENT),
                "fgraph": "event/1",
                "retracted": [],
            }
            genesis_data = _canonical_json_document(genesis_record)
            self._connection.execute(
                "INSERT INTO fgraph_events(tx,event_hash,event_data,operation_id,request_hash) "
                "VALUES (?, ?, ?, NULL, NULL)",
                (GENESIS_TX, hashlib.sha256(genesis_data.encode()).digest(), genesis_data),
            )
            self._connection.execute("COMMIT")
            return True
        except Exception:
            if self._connection.in_transaction:
                self._connection.execute("ROLLBACK")
            raise

    def _validate_objects(self) -> None:
        problems = self._layout_problems()
        if problems:
            raise FormatError(
                f"file {self.path!r} has invalid format-v{FORMAT_VERSION} layout: {problems[0]}; "
                "restore an exact format-v2 snapshot"
            )

    def _layout_problems(self) -> list[str]:
        expected = _reference_layout()
        required = set(_EXPLICIT_SCHEMA_OBJECTS)
        problems: list[str] = []
        rows = self._connection.execute(
            "SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        for row in rows:
            name = str(row["name"])
            object_type = str(row["type"])
            contract = expected.get(name)
            if contract is None:
                if name in _FTS_INTERNAL_TABLES and object_type == "table":
                    continue
                problems.append(f"non-format {object_type} {name!r} is not allowed in a dedicated fgraph file")
                continue
            required.discard(name)
            expected_type, expected_sql = contract
            if object_type != expected_type or _normalize_schema_sql(row["sql"]) != expected_sql:
                problems.append(f"modified {object_type} {name!r} differs from the canonical DDL")
        if required:
            problems.append(f"missing format objects {sorted(required)!r}")
        return problems

    def _next_timestamp(self, override: int | None = None) -> int:
        if override is not None:
            return int(encode({"instant": override}).logical)
        proposed = int(encode({"instant": self._clock()}).logical)
        row = self._connection.execute(
            "SELECT max(v) FROM fgraph_facts WHERE a=1 AND tx=e AND t=5 AND rx IS NULL"
        ).fetchone()
        latest = None if row is None else row[0]
        if latest is not None and proposed <= int(latest):
            # A custom or wall clock may stall or move backwards; receipts may not.
            proposed = int(encode({"instant": int(latest) + 1_000_000}).logical)
        return proposed

    def _ensure_open(self) -> None:
        if self._closed:
            raise FormatError("fgraph connection is closed; call connect() to open a new handle")
        try:
            self._refresh_cache()
        except (TypeError, ValueError, UnicodeError) as exc:
            raise FormatError("fgraph identity registry is malformed; run doctor or restore a valid backup") from exc
        except sqlite3.ProgrammingError as exc:
            # Historical views share their owner's connection, so closing the
            # owner must still surface through fgraph's typed error boundary.
            if "thread" in str(exc).lower():
                raise FormatError(
                    "fgraph connection belongs to another thread; open one Db handle per worker thread"
                ) from exc
            raise FormatError("fgraph connection is closed; call connect() to open a new handle") from exc

    def _ensure_writable(self) -> None:
        self._ensure_open()
        if self._read_only or self._as_of is not None:
            raise ReadOnly("this fgraph view is read-only; write through the live writable database connection")

    @contextlib.contextmanager
    def _atomic(self) -> Generator[None]:
        self._ensure_writable()
        nested = self._connection.in_transaction
        if nested:
            self._savepoint_counter += 1
            name = f"fgraph_write_{self._savepoint_counter}"
        else:
            name = ""
        try:
            self._connection.execute(f"SAVEPOINT {name}" if nested else "BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            detail = str(exc).lower()
            if "locked" in detail or "busy" in detail:
                raise Conflict(
                    "database writer lock is busy; retry the transaction after the current writer commits"
                ) from exc
            raise FormatError("SQLite could not start an atomic write; run doctor or restore a valid backup") from exc
        try:
            yield
            if nested:
                self._connection.execute(f"RELEASE {name}")
            else:
                self._connection.execute("COMMIT")
        except BaseException as exc:
            try:
                if nested:
                    self._connection.execute(f"ROLLBACK TO {name}")
                    self._connection.execute(f"RELEASE {name}")
                else:
                    self._connection.execute("ROLLBACK")
            except sqlite3.DatabaseError as rollback_exc:
                raise FormatError(
                    "SQLite could not roll back an atomic write; close the connection and restore a valid backup"
                ) from rollback_exc
            self._refresh_cache(force=True)
            if isinstance(exc, sqlite3.DatabaseError):
                raise FormatError("SQLite rejected an atomic write; run doctor or restore a valid backup") from exc
            raise
        else:
            if nested:
                self._refresh_cache(force=True)

    @contextlib.contextmanager
    def _read_snapshot(self) -> Generator[None]:
        """Keep multi-statement reads on one SQLite snapshot."""
        self._ensure_open()
        nested = self._connection.in_transaction
        if not nested:
            try:
                self._connection.execute("BEGIN")
            except sqlite3.DatabaseError as exc:
                raise FormatError("SQLite could not start a coherent read; close and reopen the database") from exc
        try:
            # The forced registry read both pins the deferred transaction and
            # keeps name resolution on the same basis as the caller's rows.
            self._refresh_cache(force=True)
            yield
        finally:
            if not nested and self._connection.in_transaction:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.DatabaseError as exc:
                    raise FormatError("SQLite could not finish a coherent read; close and reopen the database") from exc

    def _refresh_cache(self, *, force: bool = False, tolerate_malformed_gids: bool = False) -> None:
        version = int(self._connection.execute("PRAGMA data_version").fetchone()[0])
        if force or version != self._cache_version:
            rows = self._connection.execute("SELECT id, name, gid FROM fgraph_ids").fetchall()
            self._names = {str(row["name"]): int(row["id"]) for row in rows if row["name"] is not None}
            self._id_names = {entity: name for name, entity in self._names.items()}
            gids: dict[str, int] = {}
            for row in rows:
                if row["gid"] is None:
                    continue
                try:
                    gids[str(uuid.UUID(bytes=bytes(row["gid"])))] = int(row["id"])
                except (TypeError, ValueError):
                    if not tolerate_malformed_gids:
                        raise
            self._gids = gids
            self._id_gids = {entity: gid for gid, entity in self._gids.items()}
            self._cache_version = version

    def _validate_name(self, name: str) -> None:
        try:
            encoded = name.encode()
        except UnicodeEncodeError as exc:
            raise FGraphTypeError(
                f"invalid entity name {name!r}; use valid UTF-8 without unpaired surrogate code points"
            ) from exc
        if not encoded or len(encoded) > 512 or any(unicodedata.category(char) == "Cc" for char in name):
            raise FGraphTypeError(f"invalid entity name {name!r}; use 1-512 UTF-8 bytes without control characters")
        if name.startswith("fgraph/") and name not in self._names:
            raise SchemaError(f"name {name!r} uses the reserved fgraph/ namespace; choose an application namespace")

    def _validate_attribute(self, attribute: str) -> None:
        if not ATTRIBUTE_PATTERN.fullmatch(attribute):
            raise SchemaError(f"invalid attribute {attribute!r}; use exactly one slash, for example 'person/name'")
        if attribute.startswith("fgraph/") and attribute not in self._names:
            raise SchemaError(
                f"attribute {attribute!r} uses the reserved fgraph/ namespace; choose an application namespace"
            )

    def _next_available_id(self) -> int:
        row = self._connection.execute("SELECT value FROM fgraph_meta WHERE key='next_id'").fetchone()
        if row is None or not isinstance(row[0], int):
            raise FormatError("fgraph_meta.next_id is missing or non-integer; restore from a valid backup")
        next_id = int(row[0])
        if next_id < FIRST_USER_ID:
            raise FormatError(
                f"fgraph_meta.next_id is {next_id}; restore a valid fgraph file with next_id at least {FIRST_USER_ID}"
            )
        return next_id

    def _allocate(self, pending: _PendingTransaction, name: str | None = None) -> int:
        if pending.next_id == INT64_MAX:
            raise TooLarge(
                "the int64 identity allocator is exhausted; start a new fgraph file and apply retained events"
            )
        result = pending.next_id
        pending.next_id += 1
        pending.allocated[result] = name
        if name is not None:
            pending.names[name] = result
            pending.id_names[result] = name
        return result

    def _resolve_name_write(
        self,
        name: str,
        pending: _PendingTransaction,
        *,
        report: bool = True,
    ) -> int:
        self._validate_name(name)
        pending_known = pending.names.get(name)
        if pending_known is not None:
            return pending_known
        known = self._names.get(name)
        if known is not None:
            return known
        result = self._allocate(pending, name)
        if report and "/" not in name:
            pending.report_ids[name] = result
        return result

    def _resolve_attribute_write(self, attribute: Any, pending: _PendingTransaction) -> int:
        if not isinstance(attribute, str):
            raise SchemaError(f"attribute {attribute!r} is not a string; use a name such as 'person/name'")
        self._validate_attribute(attribute)
        return self._resolve_name_write(attribute, pending, report=False)

    def _resolve_read(self, ref: Any, *, missing_ok: bool = False) -> int | None:
        if isinstance(ref, bool):
            raise FGraphTypeError(f"invalid entity reference {ref!r}; use an integer id, name, or unique lookup")
        if isinstance(ref, int):
            if not INT64_MIN <= ref <= INT64_MAX:
                raise FGraphTypeError(
                    f"invalid entity reference {ref!r}; integer ids must fit the signed 64-bit SQLite domain"
                )
            exists = self._connection.execute(
                "SELECT 1 FROM fgraph_ids WHERE id=? AND (? IS NULL OR created_tx<=?)",
                (ref, self._as_of, self._as_of),
            ).fetchone()
            if exists is not None:
                return ref
        elif isinstance(ref, str):
            entity = self._names.get(ref)
            if entity is not None and self._identity_visible(entity):
                return entity
        elif isinstance(ref, Mapping) and set(ref) == {"eid"}:
            raw = ref["eid"]
            try:
                event = str(uuid.UUID(str(raw)))
            except (ValueError, AttributeError, TypeError) as exc:
                raise FGraphTypeError(
                    f"invalid stable entity id {raw!r}; use a canonical UUID in {{'eid': ...}}"
                ) from exc
            entity = self._gids.get(event)
            if entity is not None and self._identity_visible(entity):
                return entity
        elif isinstance(ref, (list, tuple)) and len(ref) == 2:
            return self._lookup_owner(ref[0], ref[1], missing_ok=missing_ok)
        else:
            raise FGraphTypeError(
                f"invalid entity reference {ref!r}; use an integer id, name, {{'eid': UUID}}, or unique lookup"
            )
        if missing_ok:
            return None
        raise NotFound(f"entity {ref!r} was not found; transact it first or use a known name/id")

    def _identity_visible(self, entity: int) -> bool:
        row = self._connection.execute("SELECT created_tx FROM fgraph_ids WHERE id=?", (entity,)).fetchone()
        return row is not None and (self._as_of is None or int(row["created_tx"]) <= self._as_of)

    def _lookup_owner(self, attribute: Any, value: Any, *, missing_ok: bool = False) -> int | None:
        if not isinstance(attribute, str):
            raise SchemaError(f"lookup attribute {attribute!r} is invalid; use an attribute name string")
        attribute_id = self._names.get(attribute)
        if attribute_id is None:
            if missing_ok:
                return None
            raise NotFound(f"lookup attribute {attribute!r} was not found; declare and transact it before lookup")
        if not self._identity_visible(attribute_id):
            if missing_ok:
                return None
            raise NotFound(f"lookup attribute {attribute!r} does not exist at this historical point")
        schema = self._schema(attribute_id, self._as_of)
        if not schema.unique:
            raise SchemaError(f"lookup attribute {attribute!r} is not unique; declare unique=True before using lookups")
        encoded = self._encode_read_value(value, schema)
        visibility, parameters = self._visibility()
        row = self._connection.execute(
            f"SELECT e FROM fgraph_facts WHERE a=? AND v=? AND t=? AND {visibility}",  # noqa: S608
            (attribute_id, encoded.stored, encoded.tag, *parameters),
        ).fetchone()
        if row is not None:
            return int(row["e"])
        if missing_ok:
            return None
        raise NotFound(
            f"lookup [{attribute!r}, {value!r}] was not found; transact that unique value before referencing it"
        )

    def _resolve_ref_write(self, ref: Any, pending: _PendingTransaction) -> int:
        if isinstance(ref, bool):
            raise FGraphTypeError(f"invalid reference {ref!r}; use an entity id, name, or unique lookup")
        if isinstance(ref, int):
            resolved = self._resolve_read(ref, missing_ok=True)
            if resolved is None:
                raise NotFound(f"referenced entity id {ref} was not found; transact the entity before linking it")
            return resolved
        if isinstance(ref, str):
            return self._resolve_name_write(ref, pending)
        if isinstance(ref, (list, tuple)) and len(ref) == 2:
            return cast(int, self._lookup_owner(ref[0], ref[1]))
        if isinstance(ref, Mapping) and set(ref) in ({"tmp"}, {"eid"}):
            return self._resolve_entity_write_selector(ref, pending)
        raise FGraphTypeError(f"invalid reference {ref!r}; use an entity id, name, or unique lookup")

    def _name_or_id(self, entity: int) -> str | int:
        return self._id_names.get(entity, entity)

    def _identity_selector(self, entity: int) -> str | dict[str, str]:
        name = self._id_names.get(entity)
        if name is not None:
            return name
        gid = self._id_gids.get(entity)
        if gid is None:
            raise FormatError(f"identity {entity} has no stable registry selector; run doctor or restore a snapshot")
        return {"eid": gid}

    def _ensure_user_target(self, entity: int) -> int:
        is_transaction = self._connection.execute(
            "SELECT 1 FROM fgraph_facts WHERE e=? AND a=1 AND tx=e LIMIT 1", (entity,)
        ).fetchone()
        if entity <= GENESIS_TX or is_transaction is not None:
            raise Unsupported(
                f"system/transaction entity {self._name_or_id(entity)!r} cannot be changed; "
                "attach transaction facts in the creating transact(tx=...) call"
            )
        return entity

    def _schema(self, attribute: int, as_of: int | None = None) -> _Schema:
        schema = _Schema()
        visibility, parameters = self._visibility(as_of)
        rows = self._connection.execute(
            # Only the visibility fragment is composed; values remain bound parameters.
            f"SELECT a, v, t FROM fgraph_facts WHERE e=? AND a IN (5,6,7,8,9,10,14) AND {visibility} ORDER BY tx",  # noqa: S608
            (attribute, *parameters),
        ).fetchall()
        for row in rows:
            value = self._logical(int(row["t"]), row["v"])
            match int(row["a"]):
                case 5:
                    schema.many = bool(value)
                case 6:
                    schema.unique = bool(value)
                case 7:
                    schema.nohistory = bool(value)
                case 8:
                    schema.type = str(value)
                case 9:
                    schema.dims = int(value)
                case 10:
                    schema.doc = str(value)
                case 14:
                    schema.vector_model = str(value)
        return schema

    def _schema_with_pending(
        self,
        attribute: int,
        operations: Sequence[_Assertion | _Retraction | _CompareAndSwap],
    ) -> _Schema:
        schema = self._schema(attribute)

        def value(schema_attribute: int) -> Any:
            match schema_attribute:
                case 5:
                    return schema.many
                case 6:
                    return schema.unique
                case 7:
                    return schema.nohistory
                case 8:
                    return schema.type
                case 9:
                    return schema.dims
                case 10:
                    return schema.doc
                case 14:
                    return schema.vector_model
                case _:
                    return None

        def update(schema_attribute: int, logical: Any = None, *, clear: bool = False) -> None:
            match schema_attribute:
                case 5:
                    schema.many = False if clear else bool(logical)
                case 6:
                    schema.unique = False if clear else bool(logical)
                case 7:
                    schema.nohistory = None if clear else bool(logical)
                case 8:
                    schema.type = None if clear else str(logical)
                case 9:
                    schema.dims = None if clear else int(logical)
                case 10:
                    schema.doc = None if clear else str(logical)
                case 14:
                    schema.vector_model = None if clear else str(logical)

        for operation in operations:
            if operation.e != attribute:
                continue
            if isinstance(operation, _CompareAndSwap):
                continue
            if isinstance(operation, _Assertion):
                update(operation.a, operation.value.logical)
                continue
            if operation.a is None:
                schema = _Schema()
                continue
            if operation.a not in {*range(5, 11), 14}:
                continue
            if operation.value is None or value(operation.a) == operation.value.logical:
                update(operation.a, clear=True)
        return schema

    def _visibility(self, as_of: int | None = None, *, alias: str = "") -> tuple[str, tuple[int, ...]]:
        prefix = f"{alias}." if alias else ""
        point = self._as_of if as_of is None else as_of
        if point is None:
            return f"{prefix}rx IS NULL", ()
        return f"{prefix}tx <= ? AND ({prefix}rx IS NULL OR {prefix}rx > ?)", (point, point)

    def _encode_read_value(self, value: Any, schema: _Schema | None = None) -> Encoded:
        def resolve(ref: Any) -> int:
            return cast(int, self._resolve_read(ref))

        encoded = encode(value, resolve)
        if schema is not None and not value_matches(schema.type, encoded):
            raise FGraphTypeError(
                f"value {value!r} has type {type_name(encoded.tag)!r}, but the attribute requires {schema.type!r}; "
                f"write a {schema.type} value or change the declaration"
            )
        return encoded

    def _encode_write_value(self, value: Any, attribute: int, pending: _PendingTransaction) -> Encoded:
        schema = self._pending_schema(attribute, pending)
        encoded = encode(value, lambda ref: self._resolve_ref_write(ref, pending))
        if not value_matches(schema.type, encoded):
            name = self._name_or_id(attribute)
            raise FGraphTypeError(
                f"attribute {name!r} requires {schema.type}, but {value!r} is {type_name(encoded.tag)}; "
                f"write a {schema.type} value or change its declaration"
            )
        if encoded.tag == VECTOR:
            dims = len(encoded.logical)
            fixed = schema.dims if schema.dims is not None else self._inferred_vector_dims(attribute)
            if fixed is not None and fixed != dims:
                raise FGraphTypeError(
                    f"attribute {self._name_or_id(attribute)!r} requires vectors with {fixed} dimensions, got {dims}; "
                    "write a matching vector or declare the intended dims before the first value"
                )
        return encoded

    def _inferred_vector_dims(self, attribute: int) -> int | None:
        row = self._connection.execute(
            "SELECT v FROM fgraph_facts WHERE a=? AND t=? ORDER BY id LIMIT 1", (attribute, VECTOR)
        ).fetchone()
        if row is None:
            return None
        return len(self._logical(VECTOR, row["v"]))

    def _logical(self, tag: int, stored: Any) -> Any:
        if tag not in range(11):
            raise FormatError(f"a fact has unknown physical tag {tag}; run doctor and restore a valid backup")
        if tag == BOOL:
            if type(stored) is not int or stored not in (0, 1):
                raise FormatError("a bool fact is not physical integer 0 or 1; run doctor and restore a valid backup")
            return bool(stored)
        if tag == REF:
            if type(stored) is not int or stored <= 0:
                raise FormatError(
                    "a ref fact is not a positive physical integer; run doctor and restore a valid backup"
                )
            return stored
        if tag == INT:
            if type(stored) is not int:
                raise FormatError("an int fact is not a physical integer; run doctor and restore a valid backup")
            return stored
        if tag == FLOAT:
            if type(stored) is not float or not math.isfinite(stored):
                raise FormatError("a float fact is not a finite physical real; run doctor and restore a valid backup")
            return stored
        if tag == TEXT:
            if not isinstance(stored, str):
                raise FormatError("a text fact is not physical text; run doctor and restore a valid backup")
            try:
                size = len(stored.encode())
            except UnicodeEncodeError as exc:
                raise FormatError("a text fact is not valid UTF-8; run doctor and restore a valid backup") from exc
            if size > BLOB_THRESHOLD:
                raise FormatError("an inline text fact exceeds 256 bytes; run doctor and restore a valid backup")
            return stored
        if tag == INSTANT:
            if type(stored) is not int or not INSTANT_MIN <= stored <= INSTANT_MAX:
                raise FormatError(
                    "an instant fact is outside its physical domain; run doctor and restore a valid backup"
                )
            return stored
        if tag == BYTES:
            if not isinstance(stored, bytes) or len(stored) > BLOB_THRESHOLD:
                raise FormatError(
                    "an inline bytes fact has an invalid physical value; run doctor and restore a valid backup"
                )
            return stored
        if tag in (VECTOR, TEXT_REF, BYTES_REF):
            row = self._connection.execute("SELECT data FROM fgraph_blobs WHERE hash=?", (stored,)).fetchone()
            return self._logical_indirect(
                tag,
                stored,
                None if row is None else row["data"],
                found=row is not None,
            )
        if tag == JSON:
            if not isinstance(stored, str):
                raise FormatError("a JSON fact is not physical text; run doctor and restore a valid backup")
            try:
                from fgraph.jsonio import loads

                decoded = loads(stored, context="stored JSON")
                valid = len(stored.encode()) <= MAX_VALUE_BYTES and canonical_json(decoded) == stored
            except (FGraphTypeError, TooLarge, UnicodeEncodeError):
                valid = False
            if not valid:
                raise FormatError("a JSON fact is not canonical JSON; run doctor and restore a valid backup")
            return stored
        return stored

    def _logical_indirect(self, tag: int, stored: Any, data: Any, *, found: bool) -> Any:
        """Decode an already-loaded indirect payload with the normal integrity checks."""
        if tag not in (VECTOR, TEXT_REF, BYTES_REF):
            raise FormatError(f"physical tag {tag} is not indirect; run doctor and restore a valid backup")
        if not isinstance(stored, bytes) or len(stored) != 32:
            raise FormatError("an indirect fact key is not a 32-byte hash; restore a valid backup")
        if not found:
            raise FormatError("an indirect fact references a missing blob; run doctor or restore a valid backup")
        if tag == VECTOR:
            import struct

            if not isinstance(data, bytes) or not data or len(data) > MAX_VALUE_BYTES or len(data) % 4:
                raise FormatError("a vector blob is not float32 little-endian data; restore a valid backup")
            if not _valid_indirect_blob(tag, stored, data):
                raise FormatError(
                    "an indirect blob does not match its content-addressed hash; run doctor and restore a valid backup"
                )
            return struct.unpack(f"<{len(data) // 4}f", data)
        if tag == TEXT_REF and isinstance(data, str):
            try:
                size = len(data.encode())
            except UnicodeEncodeError as exc:
                raise FormatError("an indirect text blob is not valid UTF-8; restore a valid backup") from exc
            if not BLOB_THRESHOLD < size <= MAX_VALUE_BYTES:
                raise FormatError("an indirect text blob has an invalid byte length; restore a valid backup")
            if not _valid_indirect_blob(tag, stored, data):
                raise FormatError(
                    "an indirect blob does not match its content-addressed hash; run doctor and restore a valid backup"
                )
            return data
        if tag == BYTES_REF and isinstance(data, bytes):
            if not BLOB_THRESHOLD < len(data) <= MAX_VALUE_BYTES:
                raise FormatError("an indirect bytes blob has an invalid byte length; restore a valid backup")
            if not _valid_indirect_blob(tag, stored, data):
                raise FormatError(
                    "an indirect blob does not match its content-addressed hash; run doctor and restore a valid backup"
                )
            return data
        raise FormatError(
            f"an indirect {type_name(tag)} fact has the wrong SQLite storage class; restore a valid backup"
        )

    def _cell(self, tag: int, stored: Any) -> Cell:
        logical = self._logical(tag, stored)
        if tag == JSON:
            logical = str(logical)
        elif tag == VECTOR:
            logical = tuple(logical)
        return Cell(tag, logical)

    def _wire(self, tag: int, stored: Any) -> Any:
        return wire_value(tag, self._logical(tag, stored), self._name_or_id)

    def _render_row(
        self,
        row: sqlite3.Row,
        *,
        rx_override: int | object | None = ...,
        local_names: Mapping[int, str] | None = None,
        local_gids: Mapping[int, str] | None = None,
        logical_override: Any = ...,
    ) -> dict[str, Any]:
        rx = row["rx"] if rx_override is ... else rx_override

        def render_id(entity: int) -> Any:
            if local_names is not None and entity in local_names:
                return local_names[entity]
            if local_gids is not None and entity in local_gids:
                return {"eid": local_gids[entity]}
            return self._name_or_id(entity)

        tag = int(row["t"])
        logical = self._logical(tag, row["v"]) if logical_override is ... else logical_override
        rendered_value = wire_value(tag, logical, render_id)
        return {
            "id": int(row["id"]),
            "e": render_id(int(row["e"])),
            "a": render_id(int(row["a"])),
            "v": rendered_value,
            "tx": int(row["tx"]),
            "rx": None if rx is None else int(cast(int, rx)),
        }

    def _render_view_row(self, row: sqlite3.Row) -> dict[str, Any]:
        """Render a fact without leaking a retraction beyond this view's horizon."""
        future_retraction = self._as_of is not None and row["rx"] is not None and int(row["rx"]) > self._as_of
        return self._render_row(row, rx_override=None) if future_retraction else self._render_row(row)

    def _insert_raw_fact(self, entity: int, attribute: int, value: Encoded, tx: int) -> sqlite3.Row:
        if value.blob is not None:
            self._connection.execute(
                "INSERT OR IGNORE INTO fgraph_blobs(hash, data) VALUES (?, ?)", (value.stored, value.blob)
            )
        cursor = self._connection.execute(
            "INSERT INTO fgraph_facts(e, a, v, t, tx, rx) VALUES (?, ?, ?, ?, ?, NULL)",
            (entity, attribute, value.stored, value.tag, tx),
        )
        fact_id = cast(int, cursor.lastrowid)
        if value.tag in (TEXT, TEXT_REF):
            self._connection.execute("INSERT INTO fgraph_fts(rowid, text) VALUES (?, ?)", (fact_id, value.logical))
        row = self._connection.execute("SELECT * FROM fgraph_facts WHERE id=?", (fact_id,)).fetchone()
        return cast(sqlite3.Row, row)

    def _delete_or_retract(self, row: sqlite3.Row, tx: int) -> dict[str, Any]:
        schema = self._schema(int(row["a"]))
        rendered = self._render_row(row, rx_override=tx)
        self._connection.execute("DELETE FROM fgraph_fts WHERE rowid=?", (row["id"],))
        if schema.deletes_history:
            self._connection.execute("DELETE FROM fgraph_facts WHERE id=?", (row["id"],))
        else:
            self._connection.execute("UPDATE fgraph_facts SET rx=? WHERE id=?", (tx, row["id"]))
        return rendered

    def _gc_blobs(self, candidates: Iterable[bytes] | None = None) -> int:
        if candidates is None:
            cursor = self._connection.execute(
                "DELETE FROM fgraph_blobs WHERE hash NOT IN (SELECT v FROM fgraph_facts WHERE t IN (7,8,9))"
            )
            return max(cursor.rowcount, 0)
        removed = 0
        for digest in set(candidates):
            cursor = self._connection.execute(
                "DELETE FROM fgraph_blobs WHERE hash=? "
                "AND NOT EXISTS (SELECT 1 FROM fgraph_facts WHERE t IN (7,8,9) AND v=? LIMIT 1)",
                (digest, digest),
            )
            removed += max(cursor.rowcount, 0)
        return removed

    def _new_pending(self) -> _PendingTransaction:
        return _PendingTransaction([], [], {}, self._next_available_id(), {}, {}, {}, {})

    def _pending_schema(self, attribute: int, pending: _PendingTransaction) -> _Schema:
        schema = pending.schemas.get(attribute)
        if schema is None:
            schema = self._schema_with_pending(attribute, pending.operations)
            pending.schemas[attribute] = schema
        return schema

    @staticmethod
    def _append_pending(
        pending: _PendingTransaction,
        operation: _Assertion | _Retraction | _CompareAndSwap,
    ) -> None:
        pending.operations.append(operation)
        if operation.a is None or operation.a in {5, 6, 7, 8, 9, 10, 14}:
            pending.schemas.pop(operation.e, None)

    @staticmethod
    def _canonical_request_value(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(key): Db._canonical_request_value(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [Db._canonical_request_value(item) for item in value]
        if isinstance(value, bytes):
            return {"bytes": base64.b64encode(value).decode("ascii")}
        if isinstance(value, uuid.UUID):
            return str(value)
        return value

    def _request_hash(
        self,
        data: Any,
        *,
        source: str | None,
        by: str | None,
        meta: Any,
        tx: Mapping[str, Any] | None,
    ) -> bytes:
        options: dict[str, Any] = {}
        if source is not None:
            options["source"] = source
        if by is not None:
            options["by"] = by
        if meta is not _OMITTED:
            options["meta"] = self._canonical_request_value(meta)
        if tx is not None:
            options["tx"] = self._canonical_request_value(tx)
        request = {"data": self._canonical_request_value(data), "options": options}
        try:
            encoded = _canonical_json_document(request).encode()
        except (TypeError, ValueError, UnicodeError) as exc:
            raise FGraphTypeError(
                "transaction request cannot be canonicalized; use JSON-compatible values and typed wrappers"
            ) from exc
        return hashlib.sha256(encoded).digest()

    def _next_event(self, transaction: int, explicit: str | uuid.UUID | None = None) -> uuid.UUID:
        try:
            if explicit is not None:
                raw = explicit
            elif self._event_seed is None:
                raw = self._event_factory()
            else:
                raw = _seeded_event_id(self._event_seed, transaction)
            event = raw if isinstance(raw, uuid.UUID) else uuid.UUID(str(raw))
        except (UnicodeError, ValueError, AttributeError, TypeError, StopIteration) as exc:
            raise FGraphTypeError(
                "event_factory returned an invalid UUID; return a canonical UUID string or uuid.UUID"
            ) from exc
        if (
            event.variant != uuid.RFC_4122
            or event.version not in (1, 2, 3, 4, 5)
            or (isinstance(raw, str) and raw.lower() != str(event))
        ):
            raise FGraphTypeError(
                f"event_factory returned non-canonical UUID {raw!r}; use lowercase hyphenated UUID text"
            )
        if self._connection.execute("SELECT 1 FROM fgraph_ids WHERE gid=?", (event.bytes,)).fetchone():
            raise Conflict(f"event UUID {event} is already committed; generate a fresh event id and retry")
        return event

    def _persist_allocations(self, pending: _PendingTransaction, transaction: int, event: uuid.UUID) -> dict[int, str]:
        local_gids: dict[int, str] = {}
        for ordinal, (entity, name) in enumerate(sorted(pending.allocated.items())):
            if entity == transaction:
                gid = event
            elif name is None:
                gid = _derived_entity_id(event, ordinal)
            else:
                gid = None
            if gid is not None:
                if self._connection.execute("SELECT 1 FROM fgraph_ids WHERE gid=?", (gid.bytes,)).fetchone():
                    raise Conflict(f"stable entity UUID {gid} is already committed; use a fresh event id and retry")
                local_gids[entity] = str(gid)
            self._connection.execute(
                "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,?,?,?)",
                (entity, name, None if gid is None else gid.bytes, transaction),
            )
            # This connection owns the writer lock. Publishing the exact delta
            # avoids an O(total identities) rescan after every local commit;
            # _atomic force-refreshes after any rollback.
            if name is not None:
                self._names[name] = entity
                self._id_names[entity] = name
            if gid is not None:
                gid_text = str(gid)
                self._gids[gid_text] = entity
                self._id_gids[entity] = gid_text
        return local_gids

    def _event_reference(self, entity: int, local_names: Mapping[int, str], local_gids: Mapping[int, str]) -> Any:
        name = local_names.get(entity, self._id_names.get(entity))
        if name is not None:
            return name
        gid = local_gids.get(entity, self._id_gids.get(entity))
        return {"eid": gid} if gid is not None else entity

    def _portable_value(self, tag: int, stored: Any) -> Any:
        return wire_value(tag, self._logical(tag, stored), self._identity_selector)

    def _portable_fact_tuple(self, row: sqlite3.Row) -> list[Any]:
        return [
            self._identity_selector(int(row["e"])),
            self._identity_selector(int(row["a"])),
            self._portable_value(int(row["t"]), row["v"]),
            type_name(int(row["t"])),
        ]

    def _unique_owners_including_pending(
        self,
        attribute: int,
        value: Encoded,
        pending: _PendingTransaction,
    ) -> set[int]:
        owners = {
            operation.e
            for operation in pending.operations
            if isinstance(operation, _Assertion)
            and operation.a == attribute
            and operation.value.tag == value.tag
            and operation.value.stored == value.stored
        }
        row = self._connection.execute(
            "SELECT e FROM fgraph_facts WHERE a=? AND v=? AND t=? AND rx IS NULL",
            (attribute, value.stored, value.tag),
        ).fetchone()
        if row is not None:
            owners.add(int(row["e"]))
        return owners

    def _upsert_owner_for_map(self, data: Mapping[str, Any], pending: _PendingTransaction) -> int | None:
        owners: set[int] = set()
        for attribute in sorted(key for key in data if key != "id"):
            attribute_id = pending.names.get(attribute, self._names.get(attribute))
            if attribute_id is None:
                continue
            schema = self._pending_schema(attribute_id, pending)
            if not schema.unique:
                continue
            values = data[attribute]
            candidates = values if isinstance(values, list) else [values]
            for value in candidates:
                try:
                    encoded = self._encode_write_value(value, attribute_id, pending)
                except (NotFound, FGraphTypeError):
                    continue
                owners.update(self._unique_owners_including_pending(attribute_id, encoded, pending))
        if len(owners) > 1:
            rendered = sorted((self._name_or_id(owner) for owner in owners), key=str)
            raise Conflict(
                f"unique attributes in one map resolve to different entities {rendered!r}; split or correct the input"
            )
        return next(iter(owners), None)

    def _map_entity(self, data: Mapping[str, Any], pending: _PendingTransaction) -> tuple[int, bool]:
        selector = data.get("id", ...)
        owner = self._upsert_owner_for_map(data, pending)
        if selector is ...:
            return (owner if owner is not None else self._allocate(pending)), False
        if isinstance(selector, dict) and set(selector) == {"tmp"}:
            temp = selector["tmp"]
            if not isinstance(temp, str) or not temp:
                raise FGraphTypeError(f"invalid tempid {temp!r}; use a non-empty string such as {{'tmp':'t1'}}")
            known = pending.report_ids.get(temp)
            if known is not None:
                if owner is not None and owner != known:
                    raise Conflict(
                        f"tempid {temp!r} is already entity {self._name_or_id(known)!r}, but unique data resolves to "
                        f"{self._name_or_id(owner)!r}; use one identity"
                    )
                return known, False
            entity = owner if owner is not None else self._allocate(pending)
            pending.report_ids[temp] = entity
            return entity, False
        if isinstance(selector, str):
            entity = self._resolve_name_write(selector, pending)
            pinned = True
        elif isinstance(selector, int) and not isinstance(selector, bool):
            entity, pinned = cast(int, self._resolve_read(selector)), True
        elif isinstance(selector, (list, tuple)) and len(selector) == 2:
            entity, pinned = cast(int, self._lookup_owner(selector[0], selector[1])), True
        else:
            raise FGraphTypeError(
                f"invalid map id {selector!r}; use a name, integer id, unique lookup, or {{'tmp':'name'}}"
            )
        if owner is not None and owner != entity:
            raise Conflict(
                f"map id {selector!r} pins entity {self._name_or_id(entity)!r}, but a unique value belongs to "
                f"{self._name_or_id(owner)!r}; use that owner or change the unique value"
            )
        return self._ensure_user_target(entity), pinned

    def _append_vector_dims(
        self,
        attribute: int,
        encoded: Encoded,
        pending: _PendingTransaction,
        auto_dims: dict[int, int],
    ) -> None:
        if encoded.tag != VECTOR:
            return
        schema = self._pending_schema(attribute, pending)
        if schema.type is None:
            self._append_pending(pending, _Assertion(attribute, 8, Encoded(TEXT, "vector", "vector")))
            schema.type = "vector"
        if schema.type != "vector":
            raise FGraphTypeError(
                f"attribute {self._name_or_id(attribute)!r} requires {schema.type}, not vector; "
                "change the declaration before writing embeddings"
            )
        if schema.dims is not None:
            return
        dimensions = len(encoded.logical)
        fixed = auto_dims.get(attribute)
        if fixed is None:
            fixed = self._inferred_vector_dims(attribute)
        if fixed is not None:
            if fixed != dimensions:
                raise FGraphTypeError(
                    f"attribute {self._name_or_id(attribute)!r} requires vectors with {fixed} dimensions, got {dimensions}; "
                    "write a matching vector"
                )
            return
        auto_dims[attribute] = dimensions
        self._append_pending(pending, _Assertion(attribute, 9, Encoded(INT, dimensions, dimensions)))

    def _parse_map(
        self,
        data: Mapping[str, Any],
        pending: _PendingTransaction,
        auto_dims: dict[int, int],
    ) -> None:
        if not all(isinstance(key, str) for key in data):
            raise FGraphTypeError(f"map keys must be strings, got {data!r}; use 'id' and attribute names")
        if not data:
            return
        if set(data) == {"id"}:
            selector = data["id"]
            if isinstance(selector, str):
                self._resolve_name_write(selector, pending)
            elif isinstance(selector, int) and not isinstance(selector, bool):
                self._resolve_read(selector)
            elif isinstance(selector, (list, tuple)) and len(selector) == 2:
                self._lookup_owner(selector[0], selector[1])
            elif not (isinstance(selector, Mapping) and set(selector) == {"tmp"}):
                raise FGraphTypeError(
                    f"invalid map id {selector!r}; use a name, integer id, unique lookup, or {{'tmp':'name'}}"
                )
            return
        entity, _pinned = self._map_entity(data, pending)
        for attribute in sorted(key for key in data if key != "id"):
            attribute_id = self._resolve_attribute_write(attribute, pending)
            schema = self._pending_schema(attribute_id, pending)
            raw = data[attribute]
            if isinstance(raw, list):
                if not schema.many:
                    raise Conflict(
                        f"attribute {attribute!r} holds one value per entity; declare it many=True to assert an array, "
                        "or wrap a literal array with {'json': [...]}"
                    )
                values: Iterable[Any] = raw
            else:
                values = (raw,)
            for value in values:
                if isinstance(value, Mapping) and not (
                    len(value) == 1 and next(iter(value)) in {"ref", "instant", "bytes", "vector", "json"}
                ):
                    if schema.type != "ref":
                        raise FGraphTypeError(
                            f"nested map on {attribute!r} requires type='ref'; declare ref=True or wrap domain JSON with "
                            "{'json': ...}"
                        )
                    nested = dict(value)
                    nested_entity, _ = self._map_entity(nested, pending)
                    self._parse_map_for_entity(nested, nested_entity, pending, auto_dims)
                    encoded = Encoded(REF, nested_entity, nested_entity)
                else:
                    encoded = self._encode_write_value(value, attribute_id, pending)
                self._append_vector_dims(attribute_id, encoded, pending, auto_dims)
                self._append_pending(pending, _Assertion(entity, attribute_id, encoded))

    def _parse_map_for_entity(
        self,
        data: Mapping[str, Any],
        entity: int,
        pending: _PendingTransaction,
        auto_dims: dict[int, int],
    ) -> None:
        for attribute in sorted(key for key in data if key != "id"):
            attribute_id = self._resolve_attribute_write(attribute, pending)
            schema = self._pending_schema(attribute_id, pending)
            raw = data[attribute]
            values = raw if isinstance(raw, list) and schema.many else (raw,)
            if isinstance(raw, list) and not schema.many:
                raise Conflict(
                    f"attribute {attribute!r} holds one value per entity; declare it many=True to assert an array"
                )
            for value in values:
                if isinstance(value, Mapping) and not (
                    len(value) == 1 and next(iter(value)) in {"ref", "instant", "bytes", "vector", "json"}
                ):
                    if schema.type != "ref":
                        raise FGraphTypeError(
                            f"nested map on {attribute!r} requires type='ref'; declare ref=True before nesting"
                        )
                    child, _ = self._map_entity(value, pending)
                    self._parse_map_for_entity(value, child, pending, auto_dims)
                    encoded = Encoded(REF, child, child)
                else:
                    encoded = self._encode_write_value(value, attribute_id, pending)
                self._append_vector_dims(attribute_id, encoded, pending, auto_dims)
                self._append_pending(pending, _Assertion(entity, attribute_id, encoded))

    def _resolve_entity_write_selector(self, selector: Any, pending: _PendingTransaction) -> int:
        if isinstance(selector, str):
            entity = self._resolve_name_write(selector, pending)
        elif isinstance(selector, int) and not isinstance(selector, bool):
            entity = cast(int, self._resolve_read(selector))
        elif isinstance(selector, Mapping) and set(selector) == {"tmp"}:
            temp = selector["tmp"]
            if not isinstance(temp, str) or not temp:
                raise FGraphTypeError(f"invalid tempid {temp!r}; use a non-empty string")
            if temp not in pending.report_ids:
                pending.report_ids[temp] = self._allocate(pending)
            entity = pending.report_ids[temp]
        elif isinstance(selector, Mapping) and set(selector) == {"eid"}:
            entity = cast(int, self._resolve_read(selector))
        elif isinstance(selector, (list, tuple)) and len(selector) == 2:
            entity = cast(int, self._lookup_owner(selector[0], selector[1]))
        else:
            raise FGraphTypeError(f"invalid entity selector {selector!r}; use a name, id, lookup, or tempid")
        return self._ensure_user_target(entity)

    def _parse_op(
        self,
        operation: Sequence[Any],
        pending: _PendingTransaction,
        auto_dims: dict[int, int],
    ) -> None:
        if not operation or operation[0] not in {"assert", "retract", "cas"}:
            raise FGraphTypeError(
                f"invalid operation {operation!r}; use ['assert', ...], ['retract', ...], or "
                "['cas', entity, attribute, old, new]"
            )
        if operation[0] == "cas":
            if len(operation) != 5:
                raise FGraphTypeError(
                    f"cas operation has {len(operation) - 1} arguments; use entity, attribute, old, and new"
                )
            entity = self._ensure_user_target(cast(int, self._resolve_read(operation[1])))
            attribute_name = operation[2]
            if not isinstance(attribute_name, str):
                raise FGraphTypeError("CAS attribute must be an existing attribute name")
            self._validate_attribute(attribute_name)
            attribute = self._names.get(attribute_name)
            if attribute is None:
                raise NotFound(f"CAS attribute {attribute_name!r} was not found")
            schema = self._pending_schema(attribute, pending)
            if schema.many:
                raise SchemaError(
                    f"attribute {self._name_or_id(attribute)!r} is cardinality-many; CAS requires one current value"
                )

            def missing(value: Any) -> bool:
                if not isinstance(value, Mapping) or set(value) != {"missing"}:
                    return False
                if value["missing"] is not True:
                    raise FGraphTypeError('CAS missing sentinel must be {"missing":true}')
                return True

            old = None if missing(operation[3]) else self._encode_read_value(operation[3], schema)
            new = None if missing(operation[4]) else self._encode_write_value(operation[4], attribute, pending)
            if new is not None:
                self._append_vector_dims(attribute, new, pending, auto_dims)
            self._append_pending(pending, _CompareAndSwap(entity, attribute, old, new))
            return
        if operation[0] == "assert":
            if len(operation) != 4:
                raise FGraphTypeError(
                    f"assert operation has {len(operation) - 1} arguments; use ['assert', entity, attribute, value]"
                )
            entity = self._resolve_entity_write_selector(operation[1], pending)
            attribute = self._resolve_attribute_write(operation[2], pending)
            encoded = self._encode_write_value(operation[3], attribute, pending)
            self._append_vector_dims(attribute, encoded, pending, auto_dims)
            self._append_pending(pending, _Assertion(entity, attribute, encoded))
            return
        if len(operation) not in (2, 3, 4):
            raise FGraphTypeError(
                f"retract operation has {len(operation) - 1} arguments; use entity, optional attribute, optional value"
            )
        selector = operation[1]
        if isinstance(selector, str) and selector not in self._names and selector not in pending.names:
            return
        entity = self._resolve_entity_write_selector(selector, pending)
        attribute: int | None = None
        value: Encoded | None = None
        if len(operation) >= 3:
            if not isinstance(operation[2], str):
                raise SchemaError(f"retract attribute {operation[2]!r} is invalid; use an attribute name")
            attribute = pending.names.get(operation[2], self._names.get(operation[2]))
            if attribute is None:
                return
        if len(operation) == 4:
            value = self._encode_read_value(operation[3], self._pending_schema(cast(int, attribute), pending))
        self._append_pending(pending, _Retraction(entity, attribute, value))

    def _parse_data(self, data: Any, pending: _PendingTransaction) -> None:
        auto_dims: dict[int, int] = {}
        if isinstance(data, Mapping):
            self._parse_map(data, pending, auto_dims)
            return
        if isinstance(data, (list, tuple)):
            if data and isinstance(data[0], str) and data[0] in {"assert", "retract", "cas"}:
                self._parse_op(data, pending, auto_dims)
                return
            for item in data:
                if isinstance(item, Mapping):
                    self._parse_map(item, pending, auto_dims)
                elif isinstance(item, (list, tuple)):
                    self._parse_op(item, pending, auto_dims)
                else:
                    raise FGraphTypeError(
                        f"transaction item {item!r} is invalid; mix entity maps and assert/retract operations"
                    )
            return
        raise FGraphTypeError(f"transaction {data!r} is invalid; use one map, one operation, or an array of them")

    def _parse_tx_facts(
        self,
        tx_data: Mapping[str, Any] | None,
        pending: _PendingTransaction,
        extra: Sequence[tuple[str, Any]] = (),
    ) -> None:
        auto_dims: dict[int, int] = {}

        def append(attribute: str, value: Any) -> None:
            if attribute in SYSTEM_NAMES[:4]:
                raise SchemaError(
                    f"transaction tx map cannot set {attribute!r}; use the automatic receipt or top-level by/source/meta option"
                )
            attribute_id = self._resolve_attribute_write(attribute, pending)
            schema = self._pending_schema(attribute_id, pending)
            if isinstance(value, Mapping) and not (
                len(value) == 1 and next(iter(value)) in {"ref", "instant", "bytes", "vector", "json"}
            ):
                if schema.type != "ref":
                    raise FGraphTypeError(
                        f"nested transaction map on {attribute!r} requires type='ref'; declare ref=True before nesting"
                    )
                nested = dict(value)
                child, _ = self._map_entity(nested, pending)
                self._parse_map_for_entity(nested, child, pending, auto_dims)
                encoded = Encoded(REF, child, child)
            else:
                encoded = self._encode_write_value(value, attribute_id, pending)
            self._append_vector_dims(attribute_id, encoded, pending, auto_dims)
            existing = [item for item in pending.tx_facts if item[0] == attribute_id]
            if any(item.tag == encoded.tag and item.stored == encoded.stored for _, item in existing):
                return
            if existing and not schema.many:
                raise Conflict(
                    f"transaction attribute {attribute!r} holds one value; provide one value or declare it many=True"
                )
            if schema.unique and self._unique_owners_including_pending(attribute_id, encoded, pending):
                raise Conflict(
                    f"unique transaction fact {attribute!r} value {encoded.logical!r} already belongs to another entity"
                )
            pending.tx_facts.append((attribute_id, encoded))
            # The transaction id is allocated last; entity 0 is a private
            # planner placeholder retargeted immediately before validation.
            self._append_pending(pending, _Assertion(0, attribute_id, encoded))

        if tx_data is not None:
            for attribute in sorted(tx_data):
                if attribute == "id":
                    raise SchemaError(
                        "transaction metadata cannot set id; the transactor allocates the transaction entity last"
                    )
                raw = tx_data[attribute]
                if isinstance(raw, list):
                    attribute_id = self._resolve_attribute_write(attribute, pending)
                    schema = self._pending_schema(attribute_id, pending)
                    if not schema.many:
                        raise Conflict(
                            f"transaction attribute {attribute!r} holds one value; declare it many=True for an array"
                        )
                    for value in raw:
                        append(attribute, value)
                else:
                    append(attribute, raw)
        for attribute, value in extra:
            append(attribute, value)

    @staticmethod
    def _fact_key(entity: int, attribute: int, value: Encoded) -> tuple[int, int, int, Any]:
        return entity, attribute, value.tag, value.stored

    def _current_rows(
        self, operations: Sequence[_Assertion | _Retraction | _CompareAndSwap]
    ) -> dict[tuple[int, int, int, Any], sqlite3.Row]:
        """Load only live rows that can affect this transaction's delta."""
        result: dict[tuple[int, int, int, Any], sqlite3.Row] = {}
        schemas: dict[int, _Schema] = {}

        def final_schema(attribute: int) -> _Schema:
            if attribute not in schemas:
                schemas[attribute] = self._schema_with_pending(attribute, operations)
            return schemas[attribute]

        def remember(rows: Iterable[sqlite3.Row]) -> None:
            for row in rows:
                result[(int(row["e"]), int(row["a"]), int(row["t"]), row["v"])] = row

        entities = sorted({operation.e for operation in operations if operation.e > 0})
        for offset in range(0, len(entities), 400):
            chunk = entities[offset : offset + 400]
            placeholders = ",".join("?" for _ in chunk)
            remember(
                self._connection.execute(
                    f"SELECT * FROM fgraph_facts WHERE rx IS NULL AND e IN ({placeholders}) ORDER BY id",  # noqa: S608
                    chunk,
                )
            )

        removed_entities = sorted(
            operation.e for operation in operations if isinstance(operation, _Retraction) and operation.a is None
        )
        for offset in range(0, len(removed_entities), 400):
            chunk = removed_entities[offset : offset + 400]
            placeholders = ",".join("?" for _ in chunk)
            remember(
                self._connection.execute(
                    f"SELECT * FROM fgraph_facts WHERE rx IS NULL AND t=? AND v IN ({placeholders}) ORDER BY id",  # noqa: S608
                    (REF, *chunk),
                )
            )

        # Unique ownership can live on an otherwise untouched entity. Exact
        # probes preserve locality without weakening the global invariant.
        for operation in operations:
            if isinstance(operation, _Retraction):
                continue
            value = operation.value if isinstance(operation, _Assertion) else operation.new
            if value is None:
                continue
            if not final_schema(operation.a).unique:
                continue
            remember(
                self._connection.execute(
                    "SELECT * FROM fgraph_facts WHERE rx IS NULL AND a=? AND t=? AND v=? ORDER BY id",
                    (operation.a, value.tag, value.stored),
                )
            )
        return result

    def _assertion_conflicts_unique(
        self,
        assertion: _Assertion,
        working: Mapping[tuple[int, int, int, Any], sqlite3.Row | _Assertion],
        schema: _Schema,
    ) -> int | None:
        if not schema.unique:
            return None
        for entity, attribute, tag, stored in working:
            if (
                attribute == assertion.a
                and tag == assertion.value.tag
                and stored == assertion.value.stored
                and entity != assertion.e
            ):
                return entity
        return None

    def _validate_cas_isolation(
        self,
        operations: Sequence[_Assertion | _Retraction | _CompareAndSwap],
    ) -> None:
        cas_targets = {(operation.e, operation.a) for operation in operations if isinstance(operation, _CompareAndSwap)}
        if not cas_targets:
            return

        exact_touches: dict[tuple[int, int], int] = {}
        entity_retractions: set[int] = set()
        for operation in operations:
            if isinstance(operation, _Retraction) and operation.a is None:
                entity_retractions.add(operation.e)
                continue
            target = (operation.e, cast(int, operation.a))
            exact_touches[target] = exact_touches.get(target, 0) + 1

        conflict = next(
            (target for target in cas_targets if exact_touches.get(target) != 1 or target[0] in entity_retractions),
            None,
        )
        if conflict is not None:
            entity, attribute = conflict
            raise Conflict(
                f"CAS target {self._name_or_id(entity)!r}/{self._name_or_id(attribute)!r} "
                "must be isolated from other operations in the same transaction"
            )

    def _plan_diff(
        self, operations: Sequence[_Assertion | _Retraction | _CompareAndSwap]
    ) -> tuple[list[_Assertion], list[sqlite3.Row]]:
        self._validate_cas_isolation(operations)
        schemas: dict[int, _Schema] = {}

        def final_schema(attribute: int) -> _Schema:
            if attribute not in schemas:
                schemas[attribute] = self._schema_with_pending(attribute, operations)
            return schemas[attribute]

        working = cast(
            dict[tuple[int, int, int, Any], sqlite3.Row | _Assertion],
            self._current_rows(operations),
        )
        possible_inbound_owners = sorted({key[0] for key in working if key[2] == REF})
        protected_owners: set[int] = set()
        for offset in range(0, len(possible_inbound_owners), 400):
            chunk = possible_inbound_owners[offset : offset + 400]
            placeholders = ",".join("?" for _ in chunk)
            protected_owners.update(
                int(row["e"])
                for row in self._connection.execute(
                    f"SELECT e FROM fgraph_facts WHERE a=1 AND tx=e AND e IN ({placeholders})",  # noqa: S608
                    chunk,
                )
            )
        inserted: list[_Assertion] = []
        retracted: list[sqlite3.Row] = []
        cancelled: set[int] = set()
        for operation in operations:
            if isinstance(operation, _CompareAndSwap):
                matches = [
                    (key, fact) for key, fact in working.items() if key[0] == operation.e and key[1] == operation.a
                ]
                if len(matches) > 1:
                    raise FormatError(
                        f"cardinality-one CAS found multiple current values for "
                        f"{self._name_or_id(operation.a)!r}; run doctor and restore a valid backup"
                    )
                expected = None if operation.old is None else self._fact_key(operation.e, operation.a, operation.old)
                matched = len(matches) == 0 if expected is None else len(matches) == 1 and matches[0][0] == expected
                if not matched:
                    actual = [self._logical(key[2], key[3]) for key, _fact in matches]
                    raise Conflict(
                        f"CAS on {self._name_or_id(operation.e)!r} "
                        f"{self._name_or_id(operation.a)!r} expected "
                        f"{'missing' if operation.old is None else operation.old.logical!r}, "
                        f"found {actual!r}; refresh the entity and retry"
                    )
                if (
                    operation.old is not None
                    and operation.new is not None
                    and operation.old.tag == operation.new.tag
                    and operation.old.stored == operation.new.stored
                ):
                    continue
                if matches:
                    key, fact = matches[0]
                    del working[key]
                    if isinstance(fact, _Assertion):
                        cancelled.add(id(fact))
                    elif all(int(existing["id"]) != int(fact["id"]) for existing in retracted):
                        retracted.append(fact)
                if operation.new is None:
                    continue
                operation = _Assertion(operation.e, operation.a, operation.new)
            if isinstance(operation, _Retraction):
                matches: list[tuple[tuple[int, int, int, Any], sqlite3.Row | _Assertion]] = []
                for key, fact in working.items():
                    entity, attribute, tag, stored = key
                    own = entity == operation.e
                    inbound = (
                        operation.a is None
                        and tag == REF
                        and stored == operation.e
                        and entity > GENESIS_TX
                        and entity not in protected_owners
                    )
                    if not (own or inbound):
                        continue
                    if operation.a is not None and attribute != operation.a:
                        continue
                    if operation.value is not None and (tag != operation.value.tag or stored != operation.value.stored):
                        continue
                    matches.append((key, fact))
                for key, fact in matches:
                    del working[key]
                    if isinstance(fact, _Assertion):
                        cancelled.add(id(fact))
                    elif all(int(existing["id"]) != int(fact["id"]) for existing in retracted):
                        retracted.append(fact)
                continue
            key = self._fact_key(operation.e, operation.a, operation.value)
            if key in working:
                continue
            schema = final_schema(operation.a)
            if not schema.many:
                conflicts = [
                    (existing_key, fact)
                    for existing_key, fact in working.items()
                    if existing_key[0] == operation.e and existing_key[1] == operation.a
                ]
                for existing_key, fact in conflicts:
                    if isinstance(fact, _Assertion):
                        raise Conflict(
                            f"attribute {self._name_or_id(operation.a)!r} holds one value per entity, but this "
                            "transaction asserts two; declare it many=True or submit one value"
                        )
                    del working[existing_key]
                    if all(int(existing["id"]) != int(fact["id"]) for existing in retracted):
                        retracted.append(fact)
            restored = next(
                (
                    fact
                    for fact in retracted
                    if self._fact_key(int(fact["e"]), int(fact["a"]), Encoded(int(fact["t"]), fact["v"], fact["v"]))
                    == key
                ),
                None,
            )
            if restored is not None:
                retracted = [fact for fact in retracted if int(fact["id"]) != int(restored["id"])]
                working[key] = restored
                continue
            owner = self._assertion_conflicts_unique(operation, working, schema)
            if owner is not None:
                raise Conflict(
                    f"unique value {operation.value.logical!r} for {self._name_or_id(operation.a)!r} already belongs "
                    f"to {self._name_or_id(owner)!r}; use that entity to upsert or choose another value"
                )
            working[key] = operation
            inserted.append(operation)
        return [item for item in inserted if id(item) not in cancelled], retracted

    def _compact_pending_allocations(
        self,
        pending: _PendingTransaction,
        assertions: Sequence[_Assertion],
    ) -> None:
        """Discard canceled anonymous ids and close their allocation gaps."""
        first = self._next_available_id()
        kept = {entity for entity in pending.names.values() if first <= entity < pending.next_id}

        def keep_encoded(value: Encoded) -> None:
            if value.tag == REF and first <= int(value.stored) < pending.next_id:
                kept.add(int(value.stored))

        for assertion in assertions:
            if first <= assertion.e < pending.next_id:
                kept.add(assertion.e)
            if first <= assertion.a < pending.next_id:
                kept.add(assertion.a)
            keep_encoded(assertion.value)
        for attribute, value in pending.tx_facts:
            if first <= attribute < pending.next_id:
                kept.add(attribute)
            keep_encoded(value)

        remap = {old: first + index for index, old in enumerate(sorted(kept))}
        if all(old == new for old, new in remap.items()) and len(kept) == pending.next_id - first:
            return

        def remap_encoded(value: Encoded) -> Encoded:
            if value.tag != REF or int(value.stored) not in remap:
                return value
            target = remap[int(value.stored)]
            return Encoded(REF, target, target)

        for assertion in assertions:
            assertion.e = remap.get(assertion.e, assertion.e)
            assertion.a = remap.get(assertion.a, assertion.a)
            assertion.value = remap_encoded(assertion.value)
        pending.tx_facts = [
            (remap.get(attribute, attribute), remap_encoded(value)) for attribute, value in pending.tx_facts
        ]
        pending.names = {name: remap.get(entity, entity) for name, entity in pending.names.items()}
        pending.id_names = {remap.get(entity, entity): name for entity, name in pending.id_names.items()}
        pending.allocated = {remap[entity]: name for entity, name in pending.allocated.items() if entity in remap}
        pending.report_ids = {
            token: remap.get(entity, entity)
            for token, entity in pending.report_ids.items()
            if entity < first or entity in kept
        }
        pending.next_id = first + len(kept)

    def _validate_schema_changes(
        self,
        assertions: Sequence[_Assertion],
        retractions: Sequence[sqlite3.Row],
    ) -> None:
        retracted_ids = {int(row["id"]) for row in retractions}

        def final_values(attribute: int) -> list[tuple[int, Encoded]]:
            values = [
                (
                    int(row["e"]),
                    Encoded(int(row["t"]), row["v"], self._logical(int(row["t"]), row["v"])),
                )
                for row in self._connection.execute(
                    "SELECT * FROM fgraph_facts WHERE a=? AND rx IS NULL ORDER BY id", (attribute,)
                )
                if int(row["id"]) not in retracted_ids
            ]
            values.extend((candidate.e, candidate.value) for candidate in assertions if candidate.a == attribute)
            return values

        schema_ids = {*range(5, 11), 14}
        affected = {candidate.e for candidate in assertions if candidate.a in schema_ids} | {
            int(row["e"]) for row in retractions if int(row["a"]) in schema_ids
        }
        for target in affected:
            # Rebuild from the final rows because the current cache still
            # contains database facts superseded by this pending transaction.
            schema = _Schema()
            schema_facts = [
                (
                    int(row["a"]),
                    self._logical(int(row["t"]), row["v"]),
                )
                for row in self._connection.execute(
                    "SELECT * FROM fgraph_facts WHERE e=? AND a IN (5,6,7,8,9,10,14) AND rx IS NULL ORDER BY id",
                    (target,),
                )
                if int(row["id"]) not in retracted_ids
            ]
            schema_facts.extend(
                (candidate.a, candidate.value.logical)
                for candidate in assertions
                if candidate.e == target and candidate.a in schema_ids
            )
            for schema_attribute, logical in schema_facts:
                match schema_attribute:
                    case 5:
                        schema.many = bool(logical)
                    case 6:
                        schema.unique = bool(logical)
                    case 7:
                        schema.nohistory = bool(logical)
                    case 8:
                        schema.type = str(logical)
                    case 9:
                        schema.dims = int(logical)
                    case 10:
                        schema.doc = str(logical)
                    case 14:
                        schema.vector_model = str(logical)

            values = final_values(target)
            if schema.type is not None:
                if schema.type not in TYPE_NAMES:
                    raise SchemaError(
                        f"attribute {self._name_or_id(target)!r} declares unknown type {schema.type!r}; use one of "
                        f"{sorted(TYPE_NAMES)!r}"
                    )
                if any(not value_matches(schema.type, value) for _, value in values):
                    raise SchemaError(
                        f"attribute {self._name_or_id(target)!r} already has incompatible live values; retract or "
                        "migrate them before changing type"
                    )
            if not schema.many:
                counts: dict[int, int] = {}
                for entity, _value in values:
                    counts[entity] = counts.get(entity, 0) + 1
                invalid = next((entity for entity, count in counts.items() if count > 1), None)
                if invalid is not None:
                    raise SchemaError(
                        f"attribute {self._name_or_id(target)!r} has multiple live values on entity "
                        f"{self._name_or_id(invalid)!r}; retract extras before declaring many=False"
                    )
            if schema.unique:
                if schema.type is None or schema.type in {"json", "vector"}:
                    raise SchemaError(
                        f"attribute {self._name_or_id(target)!r} requires a non-json, non-vector type before "
                        "unique=True; declare type first"
                    )
                owners: dict[tuple[int, Any], set[int]] = {}
                for entity, value in values:
                    owners.setdefault((value.tag, value.stored), set()).add(entity)
                if any(len(entities) > 1 for entities in owners.values()):
                    raise SchemaError(
                        f"attribute {self._name_or_id(target)!r} has duplicate live values; resolve them before "
                        "unique=True"
                    )
            if schema.dims is not None:
                if schema.dims <= 0:
                    raise SchemaError(
                        f"vector dims {schema.dims!r} for {self._name_or_id(target)!r} must be a positive integer"
                    )
                if schema.type != "vector":
                    raise SchemaError(
                        f"attribute {self._name_or_id(target)!r} declares vector dims but type is {schema.type!r}; "
                        "declare type='vector' in the same transaction"
                    )
                mismatched = next(
                    (
                        len(value.logical)
                        for _, value in values
                        if value.tag == VECTOR and len(value.logical) != schema.dims
                    ),
                    None,
                )
                if mismatched is not None:
                    raise SchemaError(
                        f"attribute {self._name_or_id(target)!r} already contains {mismatched}-dimensional vectors; "
                        f"cannot declare dims={schema.dims}"
                    )
            if schema.vector_model is not None and schema.type != "vector":
                raise SchemaError(
                    f"attribute {self._name_or_id(target)!r} declares vector_model but type is {schema.type!r}; "
                    "declare type='vector' in the same transaction"
                )

    def _validate_shapes(
        self,
        assertions: Sequence[_Assertion],
        retractions: Sequence[sqlite3.Row],
        local_names: Mapping[int, str],
    ) -> None:
        """Validate the final shaped entities without prescribing domain schema."""
        retracted_ids = {int(row["id"]) for row in retractions}

        def final_rows(entity: int) -> list[tuple[int, int, Any]]:
            rows = [
                (int(row["a"]), int(row["t"]), row["v"])
                for row in self._connection.execute(
                    "SELECT id,a,t,v FROM fgraph_facts WHERE e=? AND rx IS NULL ORDER BY id", (entity,)
                )
                if int(row["id"]) not in retracted_ids
            ]
            rows.extend(
                (candidate.a, candidate.value.tag, candidate.value.stored)
                for candidate in assertions
                if candidate.e == entity
            )
            return rows

        changed_shapes = {candidate.e for candidate in assertions if candidate.a in {16, 17, 18}} | {
            int(row["e"]) for row in retractions if int(row["a"]) in {16, 17, 18}
        }
        candidates = {candidate.e for candidate in assertions} | {int(row["e"]) for row in retractions}
        for shape in changed_shapes:
            candidates.update(
                int(row["e"])
                for row in self._connection.execute(
                    "SELECT e FROM fgraph_facts WHERE a=15 AND t=? AND v=? AND rx IS NULL", (REF, shape)
                )
            )

        shape_cache: dict[int, tuple[set[int], set[int], bool]] = {}

        def shape_definition(shape: int) -> tuple[set[int], set[int], bool]:
            cached = shape_cache.get(shape)
            if cached is not None:
                return cached
            required: set[int] = set()
            allowed: set[int] = set()
            closed = False
            for attribute, tag, stored in final_rows(shape):
                if attribute == 16 and tag == REF:
                    required.add(int(stored))
                elif attribute == 17 and tag == REF:
                    allowed.add(int(stored))
                elif attribute == 18 and tag == BOOL:
                    closed = bool(stored)
            for member in required | allowed:
                name = local_names.get(member, self._id_names.get(member))
                if name is None or ATTRIBUTE_PATTERN.fullmatch(name) is None:
                    raise SchemaError(
                        f"shape {self._name_or_id(shape)!r} references non-attribute {self._name_or_id(member)!r}; "
                        "list only namespace/attribute identities"
                    )
            result = required, allowed, closed
            shape_cache[shape] = result
            return result

        for entity in sorted(candidate for candidate in candidates if candidate > GENESIS_TX):
            rows = final_rows(entity)
            shapes = [int(stored) for attribute, tag, stored in rows if attribute == 15 and tag == REF]
            if not shapes:
                continue
            shape = shapes[-1]
            required, allowed, closed = shape_definition(shape)
            present = {attribute for attribute, _tag, _stored in rows}
            missing = sorted(required - present)
            if missing:
                names = [local_names.get(member, self._id_names.get(member, str(member))) for member in missing]
                raise SchemaError(
                    f"entity {self._name_or_id(entity)!r} does not satisfy shape {self._name_or_id(shape)!r}; "
                    f"add required attributes {names!r}"
                )
            unexpected = sorted(
                attribute for attribute in present if attribute >= FIRST_USER_ID and attribute not in required | allowed
            )
            if closed and unexpected:
                names = [local_names.get(member, self._id_names.get(member, str(member))) for member in unexpected]
                raise SchemaError(
                    f"entity {self._name_or_id(entity)!r} violates closed shape {self._name_or_id(shape)!r}; "
                    f"remove or allow attributes {names!r}"
                )

    def transact(
        self,
        data: Any,
        *,
        source: str | None = None,
        by: str | None = None,
        meta: Any = _OMITTED,
        tx: Mapping[str, Any] | None = None,
        operation_id: str | None = None,
        if_basis_tx: int | None = None,
        _at: int | None = None,
        _extra_tx_facts: Sequence[tuple[str, Any]] = (),
        _force: bool = False,
        _event_id: str | uuid.UUID | None = None,
        _event_hash: bytes | None = None,
        _event_data: str | object = _OMITTED,
        _origin_at: int | None = None,
        _preallocated: Sequence[Any] = (),
        _request_hash_override: bytes | None = None,
        _prepare_data: Callable[[], Any] | None = None,
    ) -> TxReport:
        """Atomically assert/retract maps and operations, returning a receipt."""
        _validate_operation_id(operation_id)
        if _request_hash_override is not None and (
            not isinstance(_request_hash_override, bytes) or len(_request_hash_override) != 32
        ):
            raise FGraphTypeError("internal request hash override must be a 32-byte SHA-256 digest")
        if if_basis_tx is not None and (
            not isinstance(if_basis_tx, int)
            or isinstance(if_basis_tx, bool)
            or not GENESIS_TX <= if_basis_tx <= INT64_MAX
        ):
            raise FGraphTypeError(
                f"if_basis_tx={if_basis_tx!r} is invalid; use a committed transaction id at least {GENESIS_TX}"
            )
        request_hash = (
            None
            if operation_id is None
            else (
                _request_hash_override
                if _request_hash_override is not None
                else self._request_hash(data, source=source, by=by, meta=meta, tx=tx)
            )
        )
        with self._atomic():
            if by is not None and not isinstance(by, str):
                raise FGraphTypeError(f"transaction by={by!r} is invalid; use a text author name")
            if source is not None and not isinstance(source, str):
                raise FGraphTypeError(f"transaction source={source!r} is invalid; use a text provenance identifier")
            self._refresh_cache()
            if operation_id is not None:
                existing = self._connection.execute(
                    "SELECT ev.tx,ev.request_hash,i.gid,receipt.v AS at_value,"
                    "(SELECT max(prior.tx) FROM fgraph_events prior WHERE prior.tx<ev.tx) AS basis_tx "
                    "FROM fgraph_events ev JOIN fgraph_ids i ON i.id=ev.tx "
                    "JOIN fgraph_facts receipt ON receipt.e=ev.tx AND receipt.a=1 AND receipt.tx=ev.tx "
                    "WHERE ev.operation_id=?",
                    (operation_id,),
                ).fetchone()
                if existing is not None:
                    if bytes(existing["request_hash"]) != request_hash:
                        raise Conflict(
                            f"operation_id {operation_id!r} was already used for a different request; "
                            "reuse it only for an exact retry"
                        )
                    return TxReport(
                        status="already_applied",
                        event=str(uuid.UUID(bytes=bytes(existing["gid"]))),
                        basis_tx=int(existing["basis_tx"]),
                        tx=int(existing["tx"]),
                        at=int(existing["at_value"]),
                    )
            latest = self._latest_tx()
            if if_basis_tx is not None and latest != if_basis_tx:
                raise Conflict(f"basis transaction changed from {if_basis_tx} to {latest}; refresh state and retry")
            transaction_data = data if _prepare_data is None else _prepare_data()
            pending = self._new_pending()
            for selector in _preallocated:
                if isinstance(selector, str):
                    self._resolve_name_write(selector, pending)
                else:
                    self._resolve_entity_write_selector(selector, pending)
            self._parse_data(transaction_data, pending)
            self._parse_tx_facts(tx, pending, _extra_tx_facts)
            assertions, retractions = self._plan_diff(pending.operations)
            self._compact_pending_allocations(pending, assertions)
            has_metadata = (
                _force
                or operation_id is not None
                or source is not None
                or by is not None
                or meta is not _OMITTED
                or bool(pending.tx_facts)
            )
            has_identities = bool(pending.allocated)
            if not assertions and not retractions and not has_metadata and not has_identities:
                return TxReport(basis_tx=latest, ids=pending.report_ids)
            transaction = self._allocate(pending)
            for assertion in assertions:
                if assertion.e == 0:
                    assertion.e = transaction
            self._validate_schema_changes(assertions, retractions)
            self._validate_shapes(assertions, retractions, pending.id_names)
            metadata: list[tuple[int, Encoded]] = []
            if by is not None:
                metadata.append((2, encode(by)))
            if source is not None:
                metadata.append((3, encode(source)))
            if meta is not _OMITTED:
                metadata.append((4, encode({"json": meta})))
            # Sample a stateful injected clock only after every caller value is
            # validated, so a failed write cannot consume a visible tick.
            at_value = self._next_timestamp(_at)
            metadata.insert(0, (1, Encoded(INSTANT, at_value, at_value)))
            if _event_hash is not None and (not isinstance(_event_hash, bytes) or len(_event_hash) != 32):
                raise FGraphTypeError("internal event_hash must be a 32-byte SHA-256 digest")
            event = self._next_event(transaction, _event_id)
            local_gids = self._persist_allocations(pending, transaction, event)
            asserted_rows: list[sqlite3.Row] = []
            for attribute, value in metadata:
                asserted_rows.append(self._insert_raw_fact(transaction, attribute, value, transaction))
            asserted_rows.extend(
                self._insert_raw_fact(assertion.e, assertion.a, assertion.value, transaction)
                for assertion in assertions
            )
            blob_candidates = [bytes(row["v"]) for row in retractions if int(row["t"]) in (7, 8, 9)]
            rendered_retractions = [self._delete_or_retract(row, transaction) for row in retractions]

            def render_ref(entity: int) -> Any:
                return self._event_reference(entity, pending.id_names, local_gids)

            def event_fact(row: sqlite3.Row) -> list[Any]:
                tag = int(row["t"])
                return [
                    render_ref(int(row["e"])),
                    render_ref(int(row["a"])),
                    wire_value(tag, self._logical(tag, row["v"]), render_ref),
                    type_name(tag),
                ]

            def event_assertion(assertion: _Assertion) -> list[Any]:
                return [
                    render_ref(assertion.e),
                    render_ref(assertion.a),
                    wire_value(assertion.value.tag, assertion.value.logical, render_ref),
                    type_name(assertion.value.tag),
                ]

            event_record = {
                "asserted": [event_assertion(assertion) for assertion in assertions if assertion.e != transaction],
                "at": at_value if _origin_at is None else _origin_at,
                "created": [render_ref(entity) for entity in sorted(pending.allocated) if entity != transaction],
                "event": str(event),
                "fgraph": "event/1",
                "retracted": [event_fact(row) for row in retractions],
            }
            if by is not None:
                event_record["by"] = by
            if source is not None:
                event_record["source"] = source
            if meta is not _OMITTED:
                event_record["meta"] = meta
            if pending.tx_facts:
                event_record["tx_facts"] = [
                    [render_ref(attribute), wire_value(value.tag, value.logical, render_ref), type_name(value.tag)]
                    for attribute, value in pending.tx_facts
                ]
            generated_event_data = _canonical_event_data(event_record)
            if _event_data is _OMITTED:
                stored_event_data = generated_event_data
                stored_event_hash = hashlib.sha256(stored_event_data.encode()).digest()
                if _event_hash is not None and _event_hash != stored_event_hash:
                    raise Conflict("internal event hash differs from the canonical generated event payload")
            else:
                if not isinstance(_event_data, str):
                    raise FGraphTypeError("internal event_data must be canonical JSON text")
                encoded_event_data = _event_data.encode()
                if len(encoded_event_data) > MAX_EVENT_BYTES:
                    raise TooLarge(
                        f"canonical event is {len(encoded_event_data)} bytes; keep one transaction event at or below "
                        f"{MAX_EVENT_BYTES} bytes"
                    )
                try:
                    parsed_event_data = json.loads(_event_data)
                except (json.JSONDecodeError, UnicodeError) as exc:
                    raise FGraphTypeError("internal event_data must be canonical JSON text") from exc
                if _canonical_json_document(parsed_event_data) != _event_data:
                    raise FGraphTypeError("internal event_data must use canonical JSON encoding")
                stored_event_data = _event_data
                stored_event_hash = hashlib.sha256(encoded_event_data).digest()
                if _event_hash is None or _event_hash != stored_event_hash:
                    raise Conflict("internal event hash differs from the supplied canonical event payload")
            self._connection.execute(
                "INSERT INTO fgraph_events(tx,event_hash,event_data,operation_id,request_hash) VALUES (?,?,?,?,?)",
                (
                    transaction,
                    stored_event_hash,
                    stored_event_data,
                    operation_id,
                    request_hash,
                ),
            )
            self._gc_blobs(blob_candidates)
            self._connection.execute("UPDATE fgraph_meta SET value=? WHERE key='next_id'", (pending.next_id,))
            return TxReport(
                status="applied",
                event=str(event),
                basis_tx=latest,
                tx=transaction,
                at=at_value,
                ids=pending.report_ids,
                asserted=[self._render_row(row, local_names=pending.id_names) for row in asserted_rows],
                retracted=rendered_retractions,
            )

    add = transact

    def retract(self, ref: Any, attr: str | None = None, value: Any = None) -> TxReport:
        """Retract an exact fact, an attribute, or an entity plus inbound refs."""
        operation: list[Any] = ["retract", ref]
        if attr is not None:
            operation.append(attr)
            if value is not None:
                operation.append(value)
        elif value is not None:
            raise FGraphTypeError(f"cannot retract value {value!r} without an attribute; provide attr first")
        return self.transact(operation)

    def declare(
        self,
        attr: str,
        *,
        type: str | None = None,  # noqa: A002
        ref: bool = False,
        many: bool | None = None,
        unique: bool | None = None,
        nohistory: bool | None = None,
        dims: int | None = None,
        doc: str | None = None,
        vector_model: str | None = None,
        operation_id: str | None = None,
        if_basis_tx: int | None = None,
    ) -> TxReport:
        """Declare only the optional behaviors an attribute needs."""
        self._validate_attribute(attr)
        if ref and type not in (None, "ref"):
            raise SchemaError(f"attribute {attr!r} cannot declare ref=True and type={type!r}; choose type='ref'")
        declared_type = "ref" if ref else type
        if declared_type is not None and declared_type not in TYPE_NAMES:
            raise SchemaError(
                f"attribute {attr!r} has unknown type {declared_type!r}; use one of {sorted(TYPE_NAMES)!r}"
            )
        if dims is not None and declared_type not in (None, "vector"):
            raise SchemaError(f"attribute {attr!r} declares dims but type={declared_type!r}; use type='vector'")
        if vector_model is not None and declared_type not in (None, "vector"):
            raise SchemaError(f"attribute {attr!r} declares vector_model but type={declared_type!r}; use type='vector'")
        if vector_model is not None and (not isinstance(vector_model, str) or not vector_model.strip()):
            raise SchemaError(f"attribute {attr!r} vector_model must be non-blank text")
        data: dict[str, Any] = {"id": attr}
        if declared_type is not None:
            data["fgraph/type"] = declared_type
        if many is not None:
            data["fgraph/many"] = many
        if unique is not None:
            data["fgraph/unique"] = unique
        if nohistory is not None:
            data["fgraph/nohistory"] = nohistory
        if dims is not None:
            data["fgraph/dims"] = dims
        if doc is not None:
            data["fgraph/doc"] = doc
        if vector_model is not None:
            data["fgraph/vector-model"] = vector_model
        if len(data) == 1:
            raise SchemaError(
                f"declaration for {attr!r} sets no behavior; provide type/ref/many/unique/nohistory/dims/doc/vector_model"
            )
        return self.transact(data, operation_id=operation_id, if_basis_tx=if_basis_tx)

    def declare_shape(
        self,
        name: str,
        *,
        required: Sequence[str] = (),
        allowed: Sequence[str] = (),
        closed: bool = False,
        operation_id: str | None = None,
        if_basis_tx: int | None = None,
    ) -> TxReport:
        """Create or replace one minimal required/allowed attribute shape."""
        self._validate_name(name)
        for label, values in (("required", required), ("allowed", allowed)):
            if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
                raise SchemaError(f"shape {label} must be an array of attribute names")
            if any(not isinstance(attribute, str) for attribute in values):
                raise SchemaError(f"shape {label} contains a non-string attribute; use namespace/name strings")
            for attribute in values:
                self._validate_attribute(attribute)
        if not isinstance(closed, bool):
            raise SchemaError(f"shape closed={closed!r} is invalid; use a boolean")
        required_names = sorted(set(required))
        allowed_names = sorted(set([*required_names, *allowed] if closed else allowed))
        definition: dict[str, Any] = {"id": name, "fgraph/shape-closed": closed}
        if required_names:
            definition["fgraph/shape-required"] = [{"ref": attribute} for attribute in required_names]
        if allowed_names:
            definition["fgraph/shape-allowed"] = [{"ref": attribute} for attribute in allowed_names]
        operations: list[Any] = [
            ["retract", name, "fgraph/shape-required"],
            ["retract", name, "fgraph/shape-allowed"],
            ["retract", name, "fgraph/shape-closed"],
            definition,
        ]
        return self.transact(operations, operation_id=operation_id, if_basis_tx=if_basis_tx)

    def validate(self, ref: Any) -> dict[str, Any]:
        """Validate one entity against all of its assigned shapes at this basis."""
        self._ensure_open()
        entity = cast(int, self._resolve_read(ref))
        facts = self._visible_fact_rows(entity=entity)
        violations: list[dict[str, Any]] = []
        for shape_fact in (row for row in facts if int(row["a"]) == 15 and int(row["t"]) == REF):
            shape = int(shape_fact["v"])
            definition = self._visible_fact_rows(entity=shape)
            required = {int(row["v"]) for row in definition if int(row["a"]) == 16 and int(row["t"]) == REF}
            allowed = {int(row["v"]) for row in definition if int(row["a"]) == 17 and int(row["t"]) == REF}
            closed = any(bool(row["v"]) for row in definition if int(row["a"]) == 18 and int(row["t"]) == BOOL)
            entity_name = self._name_or_id(entity)
            shape_name = self._name_or_id(shape)
            if closed:
                violations.extend(
                    [
                        {
                            "code": "shape_definition",
                            "entity": entity_name,
                            "shape": shape_name,
                            "attribute": self._name_or_id(attribute),
                            "message": "closed shape does not allow one of its required attributes",
                        }
                        for attribute in sorted(required - allowed)
                    ]
                )
            present = {int(row["a"]) for row in facts}
            violations.extend(
                [
                    {
                        "code": "required",
                        "entity": entity_name,
                        "shape": shape_name,
                        "attribute": self._name_or_id(attribute),
                        "message": "required attribute is missing",
                    }
                    for attribute in sorted(required - present)
                ]
            )
            if closed:
                for attribute in sorted(present):
                    name = self._name_or_id(attribute)
                    if not (isinstance(name, str) and name.startswith("fgraph/")) and attribute not in allowed:
                        violations.append(
                            {
                                "code": "allowed",
                                "entity": entity_name,
                                "shape": shape_name,
                                "attribute": name,
                                "message": "attribute is not allowed by the closed shape",
                            }
                        )
        return {
            "basis_tx": self._as_of if self._as_of is not None else self._latest_tx(),
            "valid": not violations,
            "violations": violations,
        }

    def _visible_fact_rows(self, *, entity: int | None = None, attribute: int | None = None) -> list[sqlite3.Row]:
        visibility, parameters = self._visibility()
        conditions = [visibility]
        values: list[Any] = list(parameters)
        if entity is not None:
            conditions.append("e=?")
            values.append(entity)
        if attribute is not None:
            conditions.append("a=?")
            values.append(attribute)
        return self._connection.execute(
            f"SELECT * FROM fgraph_facts WHERE {' AND '.join(conditions)} ORDER BY a, tx, id",  # noqa: S608
            values,
        ).fetchall()

    def _validate_pull_pattern(self, pattern: Any) -> None:
        if not isinstance(pattern, Sequence) or isinstance(pattern, (str, bytes, bytearray)):
            raise QueryError(f"pull pattern {pattern!r} is invalid; use an attribute array")
        for item in pattern:
            if isinstance(item, str):
                if item == "*":
                    continue
                forward = item.replace("/_", "/", 1)
                try:
                    self._validate_attribute(forward)
                except SchemaError as exc:
                    raise QueryError(
                        f"pull attribute {item!r} is invalid; use namespace/name or namespace/_name"
                    ) from exc
                continue
            if not isinstance(item, Mapping) or len(item) != 1:
                raise QueryError(f"pull item {item!r} is invalid; use an attribute, '*', or one nested ref object")
            attribute, subpattern = next(iter(item.items()))
            if not isinstance(attribute, str):
                raise QueryError(f"nested pull attribute {attribute!r} is invalid; use an attribute name")
            if "/_" in attribute:
                raise QueryError(
                    f"nested pull attribute {attribute!r} is reverse; use the reverse attribute string directly"
                )
            try:
                self._validate_attribute(attribute)
            except SchemaError as exc:
                raise QueryError(
                    f"nested pull attribute {attribute!r} is invalid; use namespace/name or namespace/_name"
                ) from exc
            attribute_id = self._names.get(attribute)
            if attribute_id is None:
                raise QueryError(f"nested pull attribute {attribute!r} is unknown; declare or populate a ref attribute")
            schema = self._schema(attribute_id, self._as_of)
            rows = self._visible_fact_rows(attribute=attribute_id)
            if schema.type != "ref" and (not rows or any(int(row["t"]) != REF for row in rows)):
                raise QueryError(f"nested pull attribute {attribute!r} is not a ref; use a ref attribute")
            self._validate_pull_pattern(subpattern)

    def _pull_entity(self, entity: int, pattern: Sequence[Any], depth: int, seen: frozenset[int]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        rows = self._visible_fact_rows(entity=entity)
        requested_all = "*" in pattern
        direct = {item for item in pattern if isinstance(item, str) and item != "*" and "/_" not in item}
        nested = {key: subpattern for item in pattern if isinstance(item, Mapping) for key, subpattern in item.items()}
        for row in rows:
            attribute_id = int(row["a"])
            attribute = self._id_names.get(attribute_id, str(attribute_id))
            if not requested_all and attribute not in direct and attribute not in nested:
                continue
            tag = int(row["t"])
            if tag == REF and attribute in nested and depth > 0:
                target = int(row["v"])
                if target in seen:
                    value: Any = {"ref": self._name_or_id(target)}
                else:
                    value = self._pull_entity(target, nested[attribute], depth - 1, seen | {target})
            elif tag == REF and requested_all and depth > 1:
                target = int(row["v"])
                if target in seen:
                    value = {"ref": self._name_or_id(target)}
                else:
                    value = self._pull_entity(target, ["*"], depth - 1, seen | {target})
            else:
                value = self._wire(tag, row["v"])
            if self._schema(attribute_id, self._as_of).many:
                result.setdefault(attribute, []).append(value)
            else:
                result[attribute] = value
        for item in pattern:
            if not isinstance(item, str) or "/_" not in item:
                continue
            namespace, reverse_name = item.split("/_", 1)
            forward = f"{namespace}/{reverse_name}"
            attribute_id = self._names.get(forward)
            if attribute_id is None:
                result[item] = []
                continue
            visibility, parameters = self._visibility()
            inbound = self._connection.execute(
                f"SELECT e FROM fgraph_facts WHERE a=? AND t=0 AND v=? AND {visibility} ORDER BY id",  # noqa: S608
                (attribute_id, entity, *parameters),
            ).fetchall()
            result[item] = [
                self._pull_entity(int(row["e"]), ["*"], max(depth - 1, 0), seen | {int(row["e"])})
                if depth > 1
                else {"ref": self._name_or_id(int(row["e"]))}
                for row in inbound
            ]
        return result

    def entity(self, ref: Any, depth: int = 1) -> dict[str, Any]:
        """Pull the visible attributes of one entity."""
        self._ensure_open()
        if depth < 0:
            raise QueryError(f"entity depth {depth} is negative; use zero or a positive recursion depth")
        entity = cast(int, self._resolve_read(ref))
        return self._pull_entity(entity, ["*"], depth, frozenset({entity}))

    def pull(self, ref: Any, pattern: Sequence[Any]) -> dict[str, Any]:
        """Pull one entity with an explicit pull pattern."""
        self._ensure_open()
        self._validate_pull_pattern(pattern)
        entity = cast(int, self._resolve_read(ref))
        return self._pull_entity(entity, pattern, 1, frozenset({entity}))

    def q(
        self,
        query: Mapping[str, Any] | None = None,
        args: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Result:
        """Evaluate canonical JSON Datalog over facts visible in this view."""
        from fgraph.query import evaluate

        self._ensure_open()
        if query is not None and kwargs:
            raise QueryError("q() received both a query object and keyword clauses; pass exactly one form")
        canonical = dict(query or kwargs)
        return evaluate(self, canonical, dict(args or {}))

    @staticmethod
    def _encode_cursor(payload: Mapping[str, Any]) -> str:
        return base64.urlsafe_b64encode(_canonical_json_document(payload).encode()).decode().rstrip("=")

    @staticmethod
    def _decode_cursor(cursor: str) -> Mapping[str, Any]:
        if (
            not isinstance(cursor, str)
            or not cursor
            or len(cursor) > 4096
            or re.fullmatch(r"[A-Za-z0-9_-]+", cursor) is None
        ):
            raise QueryError("datoms cursor is invalid; use the opaque next_cursor returned by datoms()")
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            raw = base64.urlsafe_b64decode(padded.encode())
            if base64.urlsafe_b64encode(raw).decode().rstrip("=") != cursor:
                raise ValueError("non-canonical base64url")
            payload = json.loads(raw)
        except (ValueError, UnicodeError, binascii.Error, json.JSONDecodeError) as exc:
            raise QueryError("datoms cursor is invalid; restart pagination without a cursor") from exc
        if not isinstance(payload, Mapping):
            raise QueryError("datoms cursor has the wrong shape; restart pagination without a cursor")
        return payload

    @staticmethod
    def _decode_seek_component(value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        if set(value) != {"bytes"} or not isinstance(value["bytes"], str):
            raise QueryError("datoms cursor has an invalid seek value; restart pagination")
        try:
            return base64.b64decode(value["bytes"], validate=True)
        except (ValueError, binascii.Error) as exc:
            raise QueryError("datoms cursor has invalid base64 seek bytes; restart pagination") from exc

    def datoms(
        self,
        index: str = "eavt",
        components: Sequence[Any] = (),
        *,
        source: str = "current",
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Page deterministic current or historical datoms by an index prefix."""
        self._ensure_open()
        layouts = {
            "eavt": ("e", "a", "v", "tx", "added"),
            "avet": ("a", "v", "e", "tx", "added"),
            "vaet": ("v", "a", "e", "tx", "added"),
        }
        physical = {
            "eavt": ("e", "a", "v", "t", "event_tx", "added", "fact_id"),
            "avet": ("a", "v", "e", "t", "event_tx", "added", "fact_id"),
            "vaet": ("v", "a", "e", "t", "event_tx", "added", "fact_id"),
        }
        if index not in layouts:
            raise QueryError(f"datoms index {index!r} is invalid; use 'eavt', 'avet', or 'vaet'")
        if source not in {"current", "history"}:
            raise QueryError(f"datoms source {source!r} is invalid; use 'current' or 'history'")
        if (
            not isinstance(components, Sequence)
            or isinstance(components, (str, bytes, bytearray))
            or len(components) > 5
        ):
            raise QueryError("datoms components must be an index-prefix array of at most five values")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000:
            raise QueryError(f"datoms limit {limit!r} is invalid; use an integer from 1 through 1000")
        fingerprint = hashlib.sha256(
            _canonical_json_document(self._canonical_request_value(list(components))).encode()
        ).hexdigest()
        basis = self._as_of if self._as_of is not None else self._latest_tx()
        seek: list[Any] | None = None
        if cursor is not None:
            payload = self._decode_cursor(cursor)
            expected = {"v": FORMAT_VERSION, "index": index, "source": source, "args": fingerprint}
            if any(payload.get(key) != value for key, value in expected.items()):
                raise QueryError(
                    "datoms cursor does not match this index/source/components request; restart pagination"
                )
            if not isinstance(payload.get("basis"), int) or not isinstance(payload.get("seek"), list):
                raise QueryError("datoms cursor has invalid bounds; restart pagination")
            basis = int(payload["basis"])
            raw_seek = payload["seek"]
            if len(raw_seek) != len(physical[index]):
                raise QueryError("datoms cursor has an invalid seek key; restart pagination")
            seek = [self._decode_seek_component(value) for value in raw_seek]
            value_position = physical[index].index("v")
            for position, value in enumerate(seek):
                if position == value_position:
                    if isinstance(value, bool) or not isinstance(value, (int, float, str, bytes)):
                        raise QueryError("datoms cursor has an invalid stored value; restart pagination")
                elif not isinstance(value, int) or isinstance(value, bool):
                    raise QueryError("datoms cursor has a non-integer seek coordinate; restart pagination")
            latest = self._latest_tx()
            if basis < GENESIS_TX or basis > latest or (self._as_of is not None and basis > self._as_of):
                raise QueryError("datoms cursor is outside this database view; restart pagination")

        parameters: list[Any]
        if source == "current":
            event_rows = (
                "SELECT id AS fact_id,e,a,v,t,tx AS event_tx,1 AS added FROM fgraph_facts "
                "WHERE tx<=? AND (rx IS NULL OR rx>?)"
            )
            parameters = [basis, basis]
        else:
            event_rows = (
                "SELECT id AS fact_id,e,a,v,t,tx AS event_tx,1 AS added FROM fgraph_facts WHERE tx<=? "
                "UNION ALL "
                "SELECT id AS fact_id,e,a,v,t,rx AS event_tx,0 AS added FROM fgraph_facts "
                "WHERE rx IS NOT NULL AND rx<=?"
            )
            parameters = [basis, basis]
        conditions: list[str] = []
        bound_attribute: int | None = None
        for field, raw in zip(layouts[index], components, strict=False):
            if field in {"e", "a", "tx"}:
                if field == "a" and isinstance(raw, str):
                    identifier = self._names.get(raw)
                    if identifier is None or not self._identity_visible(identifier):
                        return {"basis_tx": basis, "items": [], "next_cursor": None}
                else:
                    identifier = self._resolve_read(raw, missing_ok=True)
                    if identifier is None:
                        return {"basis_tx": basis, "items": [], "next_cursor": None}
                column = "event_tx" if field == "tx" else field
                conditions.append(f"{column}=?")
                parameters.append(identifier)
                if field == "a":
                    bound_attribute = identifier
            elif field == "v" and index == "vaet":
                selector = raw["ref"] if isinstance(raw, Mapping) and set(raw) == {"ref"} else raw
                encoded = self._encode_read_value({"ref": selector})
                conditions.extend(["v=?", "t=?"])
                parameters.extend([encoded.stored, encoded.tag])
            elif field == "added":
                if not isinstance(raw, bool):
                    raise QueryError("datoms added component must be a boolean")
                conditions.append("added=?")
                parameters.append(int(raw))
            else:
                schema = None if bound_attribute is None else self._schema(bound_attribute, basis)
                encoded = self._encode_read_value(raw, schema)
                conditions.extend(["v=?", "t=?"])
                parameters.extend([encoded.stored, encoded.tag])
        if index == "vaet":
            conditions.append("t=?")
            parameters.append(REF)
        if seek is not None:
            columns = ",".join(physical[index])
            conditions.append(f"({columns}) > ({','.join('?' for _ in seek)})")
            parameters.extend(seek)
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        order_columns = ",".join(physical[index])
        rows = self._connection.execute(
            f"SELECT * FROM ({event_rows}){where} ORDER BY {order_columns} LIMIT ?",  # noqa: S608
            (*parameters, limit + 1),
        ).fetchall()
        page = rows[:limit]
        items = [
            {
                "e": self._name_or_id(int(row["e"])),
                "a": self._name_or_id(int(row["a"])),
                "v": self._wire(int(row["t"]), row["v"]),
                "tx": self._name_or_id(int(row["event_tx"])),
                "added": bool(row["added"]),
                "fact_id": int(row["fact_id"]),
            }
            for row in page
        ]
        next_cursor = None
        if len(rows) > limit:
            last = page[-1]
            raw_seek = [last[column] for column in physical[index]]
            next_cursor = self._encode_cursor(
                {
                    "v": FORMAT_VERSION,
                    "basis": basis,
                    "index": index,
                    "source": source,
                    "args": fingerprint,
                    "seek": self._canonical_request_value(raw_seek),
                }
            )
        return {"basis_tx": basis, "items": items, "next_cursor": next_cursor}

    def explain(self, query: Mapping[str, Any], args: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Return a deterministic planning-only explanation for a Datalog query."""
        from fgraph.query import explain

        self._ensure_open()
        return explain(self, query, dict(args or {}))

    def _resolve_time(self, value: Any) -> int:
        if isinstance(value, str):
            encoded = encode({"instant": value})
            timestamp = int(encoded.logical)
        elif isinstance(value, Mapping) and set(value) == {"instant"}:
            encoded = encode(value)
            timestamp = int(encoded.logical)
        elif isinstance(value, int) and not isinstance(value, bool):
            if not INT64_MIN <= value <= INT64_MAX:
                raise FGraphTypeError(
                    f"time {value!r} exceeds signed 64-bit range; use a transaction id or representable UTC microseconds"
                )
            tx_row = self._connection.execute(
                "SELECT 1 FROM fgraph_facts WHERE e=? AND a=1 AND tx=e LIMIT 1", (value,)
            ).fetchone()
            if tx_row is not None:
                return value
            # Transaction identity wins, but a non-transaction integer must
            # still be a timestamp every peer can render as strict RFC 3339.
            timestamp = int(encode({"instant": value}).logical)
        else:
            raise FGraphTypeError(
                f"time {value!r} is invalid; use a transaction id or {{'instant': RFC3339-or-microseconds}}"
            )
        row = self._connection.execute(
            "SELECT e FROM fgraph_facts WHERE a=1 AND tx=e AND t=5 AND v<=? ORDER BY v DESC, e DESC LIMIT 1",
            (timestamp,),
        ).fetchone()
        if row is None:
            raise NotFound(f"time {value!r} precedes the database genesis; choose a later instant or transaction id")
        return int(row["e"])

    def at(self, value: Any) -> Db:
        """Return a read-only view pinned to a transaction or timestamp."""
        self._ensure_open()
        point = self._resolve_time(value)
        if self._as_of is not None:
            point = min(point, self._as_of)
        view = object.__new__(Db)
        view.path = self.path
        view._read_only = True
        view._as_of = point
        view._owns_connection = False
        view._closed = False
        view._speculation_depth = self._speculation_depth
        view._savepoint_counter = self._savepoint_counter
        view._cache_version = self._cache_version
        view._names = self._names
        view._id_names = self._id_names
        view._gids = self._gids
        view._id_gids = self._id_gids
        view._clock_source = self._clock_source
        view._clock = self._clock
        view._event_factory = self._event_factory
        view._event_seed = self._event_seed
        view._query_budget = self._query_budget
        view._connection = self._connection
        return view

    def _tx_metadata(self, transaction: int) -> dict[str, Any]:
        rows = self._connection.execute(
            "SELECT * FROM fgraph_facts WHERE e=? AND tx=? ORDER BY id", (transaction, transaction)
        ).fetchall()
        if not rows:
            raise NotFound(f"transaction {transaction} was not found; use a transaction id returned by transact()")
        metadata: dict[str, Any] = {}
        for row in rows:
            attribute = int(row["a"])
            if attribute == 1:
                metadata["at"] = int(row["v"])
            elif attribute == 2:
                metadata["by"] = self._logical(int(row["t"]), row["v"])
            elif attribute == 3:
                metadata["source"] = self._logical(int(row["t"]), row["v"])
            elif attribute == 4:
                metadata["meta"] = json.loads(str(self._logical(int(row["t"]), row["v"])))
            elif attribute == 13:
                metadata["imported_at"] = int(row["v"])
        return metadata

    def receipt(self, transaction: int) -> dict[str, Any]:
        """Return the durable operation/event receipt for one transaction."""
        self._ensure_open()
        if (
            not isinstance(transaction, int)
            or isinstance(transaction, bool)
            or not INT64_MIN <= transaction <= INT64_MAX
        ):
            raise FGraphTypeError(f"transaction {transaction!r} is invalid; use a signed 64-bit transaction id")
        read_basis = self._as_of if self._as_of is not None else self._latest_tx()
        if transaction > read_basis:
            raise NotFound(f"transaction {transaction} is after this view's basis {read_basis}")
        row = self._connection.execute(
            "SELECT ev.event_hash,ev.operation_id,ev.request_hash,i.gid,"
            "coalesce((SELECT max(prior.tx) FROM fgraph_events prior WHERE prior.tx<ev.tx),ev.tx) AS basis_tx "
            "FROM fgraph_events ev JOIN fgraph_ids i ON i.id=ev.tx WHERE ev.tx=?",
            (transaction,),
        ).fetchone()
        if row is None:
            raise NotFound(f"transaction {transaction} was not found; use a transaction id returned by transact()")
        custom_facts = self._connection.execute(
            "SELECT * FROM fgraph_facts WHERE e=? AND tx=e AND a NOT IN (1,2,3,4,13) ORDER BY id",
            (transaction,),
        ).fetchall()
        return {
            "read_basis_tx": read_basis,
            "basis_tx": int(row["basis_tx"]),
            "tx": transaction,
            "event": str(uuid.UUID(bytes=bytes(row["gid"]))),
            "event_hash": f"sha256:{bytes(row['event_hash']).hex()}",
            "operation_id": row["operation_id"],
            "request_hash": None if row["request_hash"] is None else f"sha256:{bytes(row['request_hash']).hex()}",
            **self._tx_metadata(transaction),
            "facts": [self._render_view_row(fact) for fact in custom_facts],
        }

    def history(self, ref: Any, attr: str | None = None) -> list[dict[str, Any]]:
        """Return the entity timeline with asserting/retracting provenance."""
        self._ensure_open()
        entity = cast(int, self._resolve_read(ref))
        attribute: int | None = None
        if attr is not None:
            attribute = self._names.get(attr)
            if attribute is None:
                raise NotFound(f"attribute {attr!r} was not found; transact or declare it before reading history")
        conditions = ["e=?"]
        parameters: list[Any] = [entity]
        if attribute is not None:
            conditions.append("a=?")
            parameters.append(attribute)
        if self._as_of is not None:
            conditions.append("tx<=?")
            parameters.append(self._as_of)
        rows = self._connection.execute(
            f"SELECT * FROM fgraph_facts WHERE {' AND '.join(conditions)} ORDER BY tx, id",  # noqa: S608
            parameters,
        ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            future_retraction = self._as_of is not None and row["rx"] is not None and int(row["rx"]) > self._as_of
            rendered = self._render_view_row(row)
            start = self._tx_metadata(int(row["tx"]))
            rendered.update({key: start[key] for key in ("at", "by", "source") if key in start})
            if row["rx"] is not None and not future_retraction:
                end = self._tx_metadata(int(row["rx"]))
                rendered.update({f"rx_{key}": end[key] for key in ("at", "by", "source") if key in end})
            result.append(rendered)
        return result

    def _time_window(self, first: Any, second: Any) -> tuple[int, int]:
        start, end = self._resolve_time(first), self._resolve_time(second)
        if start > end:
            raise QueryError(f"time window {start}..{end} is reversed; provide the earlier boundary first")
        return start, end

    def diff(self, t1: Any, t2: Any) -> dict[str, list[dict[str, Any]]]:
        """Return facts asserted and retracted in ``(t1, t2]``."""
        self._ensure_open()
        start, end = self._time_window(t1, t2)
        if self._as_of is not None:
            end = min(end, self._as_of)
        if start >= end:
            return {"asserted": [], "retracted": []}
        asserted = self._connection.execute(
            "SELECT * FROM fgraph_facts WHERE tx>? AND tx<=? ORDER BY tx, id", (start, end)
        ).fetchall()
        retracted = self._connection.execute(
            "SELECT * FROM fgraph_facts WHERE rx>? AND rx<=? ORDER BY rx, id", (start, end)
        ).fetchall()
        return {
            "asserted": [self._render_view_row(row) for row in asserted],
            "retracted": [self._render_view_row(row) for row in retracted],
        }

    def _latest_tx(self) -> int:
        row = self._connection.execute("SELECT max(e) FROM fgraph_facts WHERE a=1 AND tx=e").fetchone()
        return GENESIS_TX if row is None or row[0] is None else int(row[0])

    def changes(self, since: Any, until: Any | None = None) -> dict[str, list[dict[str, Any]]]:
        """Return changes after one boundary through ``until`` or the latest tx."""
        return self.diff(since, self._latest_tx() if until is None else until)

    def _decode_event_data(
        self,
        event: str,
        event_hash: bytes,
        event_data: Any,
    ) -> dict[str, Any]:
        if not isinstance(event_data, str):
            raise FormatError(f"event {event} payload is not physical TEXT")
        raw = event_data.encode()
        if len(raw) > MAX_EVENT_BYTES:
            raise FormatError(f"event {event} payload exceeds the format bound of {MAX_EVENT_BYTES} bytes")
        try:
            parsed = json.loads(event_data)
        except (json.JSONDecodeError, UnicodeError) as exc:
            raise FormatError(f"event {event} payload is not valid JSON") from exc
        if not isinstance(parsed, dict) or _canonical_json_document(parsed) != event_data:
            raise FormatError(f"event {event} payload is not a canonical JSON object")
        if hashlib.sha256(raw).digest() != event_hash:
            raise FormatError(f"event {event} hash differs from its stored canonical payload")
        required = {"fgraph", "event", "at", "created", "asserted", "retracted"}
        if not required <= set(parsed) or parsed.get("fgraph") != "event/1" or parsed.get("event") != event:
            raise FormatError(f"event {event} payload has invalid identity or required fields")
        try:
            encode({"instant": parsed["at"]})
        except FGraphTypeError as exc:
            raise FormatError(f"event {event} payload has an invalid at instant") from exc
        if not all(isinstance(parsed[field], list) for field in ("created", "asserted", "retracted")):
            raise FormatError(f"event {event} payload created/asserted/retracted fields must be arrays")
        if parsed.get("redacted") is True:
            redacted_fields = required | {"redacted", "redacts"}
            redacts = parsed.get("redacts")
            if (
                set(parsed) != redacted_fields
                or parsed["created"]
                or parsed["asserted"]
                or parsed["retracted"]
                or not isinstance(redacts, list)
                or any(not isinstance(target, str) for target in redacts)
                or redacts != sorted(set(redacts))
            ):
                raise FormatError(f"event {event} has a malformed excision redaction payload")
            for target in redacts:
                try:
                    parsed_target = uuid.UUID(target)
                except ValueError as exc:
                    raise FormatError(f"event {event} redacts invalid UUID {target!r}") from exc
                if target != str(parsed_target) or parsed_target.variant != uuid.RFC_4122:
                    raise FormatError(f"event {event} redacts non-canonical UUID {target!r}")
            return cast(dict[str, Any], parsed)
        allowed = required | {"by", "source", "meta", "tx_facts"}
        if set(parsed) - allowed:
            raise FormatError(f"event {event} payload has unknown fields {sorted(set(parsed) - allowed)!r}")
        if "by" in parsed and not isinstance(parsed["by"], str):
            raise FormatError(f"event {event} payload by field is not text")
        if "source" in parsed and not isinstance(parsed["source"], str):
            raise FormatError(f"event {event} payload source field is not text")
        if "tx_facts" in parsed and not isinstance(parsed["tx_facts"], list):
            raise FormatError(f"event {event} payload tx_facts field is not an array")
        return cast(dict[str, Any], parsed)

    def _event_record_for_tx(self, transaction: int) -> dict[str, Any]:
        identity = self._connection.execute(
            "SELECT gid FROM fgraph_ids WHERE id=? AND name IS NULL", (transaction,)
        ).fetchone()
        receipt = self._connection.execute(
            "SELECT event_hash,event_data FROM fgraph_events WHERE tx=?", (transaction,)
        ).fetchone()
        if identity is None or receipt is None:
            raise FormatError(
                f"transaction {transaction} lacks its stable event receipt; restore a valid format-v2 file"
            )
        event = str(uuid.UUID(bytes=bytes(identity["gid"])))
        event_hash = bytes(receipt["event_hash"])
        if receipt["event_data"] is None:
            metadata = self._tx_metadata(transaction)
            return {
                "fgraph": "event/1",
                "event": event,
                "at": metadata.get("imported_at", metadata["at"]),
                "redacted": True,
                "event_hash": event_hash.hex(),
            }
        return self._decode_event_data(event, event_hash, receipt["event_data"])

    def event_records(self, since: int = GENESIS_TX, through: int | None = None) -> list[dict[str, Any]]:
        """Return portable event/1 records after a local transaction cursor."""
        self._ensure_open()
        if not isinstance(since, int) or isinstance(since, bool) or not GENESIS_TX <= since <= INT64_MAX:
            raise FGraphTypeError(f"event cursor {since!r} is invalid; use a transaction id at least {GENESIS_TX}")
        end = self._as_of if through is None and self._as_of is not None else through
        if end is None:
            end = self._latest_tx()
        if not isinstance(end, int) or isinstance(end, bool) or not GENESIS_TX <= end <= INT64_MAX:
            raise FGraphTypeError(f"event through {end!r} is invalid; use a transaction id at least {GENESIS_TX}")
        if self._as_of is not None:
            end = min(end, self._as_of)
        rows = self._connection.execute(
            "SELECT tx FROM fgraph_events WHERE tx>? AND tx<=? ORDER BY tx", (since, end)
        ).fetchall()
        return [self._event_record_for_tx(int(row["tx"])) for row in rows]

    def _apply_selector(self, selector: Any, tokens: dict[str, str]) -> Any:
        if isinstance(selector, str):
            return selector
        if not isinstance(selector, Mapping) or set(selector) != {"eid"} or not isinstance(selector["eid"], str):
            raise FGraphTypeError("event entity selector must be a name or {'eid': canonical-uuid}")
        try:
            parsed = uuid.UUID(selector["eid"])
        except ValueError as exc:
            raise FGraphTypeError(f"event entity id {selector['eid']!r} is invalid; use a canonical UUID") from exc
        canonical = str(parsed)
        if selector["eid"] != canonical:
            raise FGraphTypeError(
                f"event entity id {selector['eid']!r} is not canonical; use lowercase hyphenated UUID text"
            )
        existing = self._connection.execute("SELECT id FROM fgraph_ids WHERE gid=?", (parsed.bytes,)).fetchone()
        if existing is not None:
            return int(existing["id"])
        token = tokens.setdefault(canonical, f"event:{canonical}")
        return {"tmp": token}

    @staticmethod
    def _decode_tagged_wire_value(value: Any, tag: Any) -> Any:
        if not isinstance(tag, str) or tag not in TYPE_NAMES:
            raise FGraphTypeError(f"wire value tag {tag!r} is invalid; use one of {sorted(TYPE_NAMES)!r}")
        if tag == "float":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise FGraphTypeError(f"wire float value {value!r} must be a finite JSON number")
            value = float(value)
        encoded = encode(value)
        if type_name(encoded.tag) != tag:
            raise FGraphTypeError(f"wire value {value!r} does not match logical tag {tag!r}; use its canonical wrapper")
        return value

    def _apply_value(self, value: Any, tag: Any, tokens: dict[str, str]) -> Any:
        if tag == "ref":
            if not isinstance(value, Mapping) or set(value) != {"ref"}:
                raise FGraphTypeError("event ref value must use {'ref': selector}")
            return {"ref": self._apply_selector(value["ref"], tokens)}
        return self._decode_tagged_wire_value(value, tag)

    def apply(self, source: str | Iterable[str] | TextIO) -> list[TxReport]:
        """Idempotently apply portable event/1 NDJSON as locally rebased transactions."""
        return self._apply_events(source)

    def apply_summary(self, source: str | Iterable[str] | TextIO) -> dict[str, int]:
        """Atomically apply an event stream without retaining detailed fact reports."""
        summary = {"events": 0, "applied": 0, "already_applied": 0, "noop": 0, "basis_tx": self._latest_tx()}
        self._apply_events(source, summary=summary)
        summary["basis_tx"] = self._latest_tx()
        return summary

    def _apply_events(
        self,
        source: str | Iterable[str] | TextIO,
        *,
        summary: dict[str, int] | None = None,
    ) -> list[TxReport]:
        self._ensure_writable()
        lines: Iterable[str] = source.splitlines() if isinstance(source, str) else source
        with self._atomic():
            reports: list[TxReport] = []

            def record_report(report: TxReport) -> None:
                if summary is None:
                    reports.append(report)
                    return
                summary["events"] += 1
                summary[report.status] += 1

            for line_number, raw in enumerate(lines, start=1):
                if not raw.strip():
                    continue
                try:
                    line_size = len(raw.encode())
                except UnicodeEncodeError as exc:
                    raise FGraphTypeError(f"event line {line_number} is not valid UTF-8") from exc
                if line_size > MAX_EVENT_BYTES:
                    raise TooLarge(
                        f"event line {line_number} is {line_size} bytes; "
                        f"keep each event at or below {MAX_EVENT_BYTES} portable bytes"
                    )
                from fgraph.jsonio import loads

                record = loads(raw, context=f"event line {line_number}")
                if not isinstance(record, Mapping) or record.get("fgraph") != "event/1":
                    raise FGraphTypeError(f"event line {line_number} must be an fgraph event/1 object")
                allowed = {
                    "fgraph",
                    "event",
                    "at",
                    "created",
                    "by",
                    "source",
                    "meta",
                    "tx_facts",
                    "asserted",
                    "retracted",
                }
                unknown = sorted(set(record) - allowed)
                if unknown:
                    raise FGraphTypeError(f"event line {line_number} has unknown fields {unknown!r}")
                event_text = record.get("event")
                if not isinstance(event_text, str):
                    raise FGraphTypeError(f"event line {line_number} has no UUID event id")
                try:
                    event = uuid.UUID(event_text)
                except ValueError as exc:
                    raise FGraphTypeError(f"event line {line_number} has invalid event UUID {event_text!r}") from exc
                if event_text != str(event) or event.variant != uuid.RFC_4122 or event.version not in (1, 2, 3, 4, 5):
                    raise FGraphTypeError(f"event line {line_number} event id is not a canonical RFC 4122 UUID")
                event_hash = hashlib.sha256(_canonical_json_document(record).encode()).digest()
                existing_identity = self._connection.execute(
                    "SELECT id FROM fgraph_ids WHERE gid=?", (event.bytes,)
                ).fetchone()
                if existing_identity is not None:
                    existing_tx = int(existing_identity["id"])
                    existing = self._connection.execute(
                        "SELECT event_hash FROM fgraph_events WHERE tx=?", (existing_tx,)
                    ).fetchone()
                    if existing is None or bytes(existing["event_hash"]) != event_hash:
                        raise Conflict(f"event {event_text} collides with another identity or payload")
                    receipt = self.receipt(existing_tx)
                    record_report(
                        TxReport(
                            status="already_applied",
                            event=event_text,
                            basis_tx=int(receipt["basis_tx"]),
                            tx=existing_tx,
                            at=int(receipt["at"]),
                        )
                    )
                    continue
                at = record.get("at")
                created = record.get("created")
                asserted = record.get("asserted")
                retracted = record.get("retracted")
                if (
                    not isinstance(at, int)
                    or isinstance(at, bool)
                    or not isinstance(created, list)
                    or not isinstance(asserted, list)
                    or not isinstance(retracted, list)
                ):
                    raise FGraphTypeError(
                        f"event line {line_number} needs integer at and created/asserted/retracted arrays"
                    )
                origin_at = int(encode({"instant": at}).logical)
                tokens: dict[str, str] = {}
                preallocated = [self._apply_selector(selector, tokens) for selector in created]
                data: list[Any] = []

                for kind, tuples in (("retract", retracted), ("assert", asserted)):
                    for item in tuples:
                        if not isinstance(item, list) or len(item) != 4 or not isinstance(item[1], str):
                            raise FGraphTypeError(
                                f"event line {line_number} {kind} tuple must be [selector,attribute,value,tag]"
                            )
                        data.append(
                            [
                                kind,
                                self._apply_selector(item[0], tokens),
                                item[1],
                                self._apply_value(item[2], item[3], tokens),
                            ]
                        )
                raw_tx_facts = record.get("tx_facts", [])
                if not isinstance(raw_tx_facts, list):
                    raise FGraphTypeError(f"event line {line_number} tx_facts must be an array")
                tx_values: dict[str, list[Any]] = {}
                for item in raw_tx_facts:
                    if not isinstance(item, list) or len(item) != 3 or not isinstance(item[0], str):
                        raise FGraphTypeError(f"event line {line_number} tx fact must be [attribute,value,tag]")
                    tx_values.setdefault(item[0], []).append(self._apply_value(item[1], item[2], tokens))
                tx_data = {
                    attribute: values[0] if len(values) == 1 else values for attribute, values in tx_values.items()
                }
                by = record.get("by")
                source_name = record.get("source")
                if by is not None and not isinstance(by, str):
                    raise FGraphTypeError(f"event line {line_number} by must be text")
                if source_name is not None and not isinstance(source_name, str):
                    raise FGraphTypeError(f"event line {line_number} source must be text")
                report = self.transact(
                    data,
                    by=by,
                    source=source_name,
                    meta=record.get("meta", _OMITTED),
                    tx=tx_data or None,
                    _extra_tx_facts=[(SYSTEM_NAMES[12], {"instant": origin_at})],
                    _force=True,
                    _event_id=event_text,
                    _event_hash=event_hash,
                    _event_data=_canonical_json_document(record),
                    _origin_at=origin_at,
                    _preallocated=preallocated,
                )
                for eid, token in tokens.items():
                    identifier = report.ids.get(token)
                    if identifier is None:
                        continue
                    collision = self._connection.execute(
                        "SELECT id FROM fgraph_ids WHERE gid=? AND id<>?", (uuid.UUID(eid).bytes, identifier)
                    ).fetchone()
                    if collision is not None:
                        raise Conflict(f"event entity {eid} collides with an existing stable identity")
                    self._connection.execute(
                        "UPDATE fgraph_ids SET gid=? WHERE id=? AND name IS NULL", (uuid.UUID(eid).bytes, identifier)
                    )
                self._refresh_cache(force=True)
                record_report(report)
            return reports

    def follow(self, since: Any | None = None, *, interval: float = 0.5) -> Generator[dict[str, Any]]:
        """Poll cross-process commits and yield portable event/1 records."""
        self._ensure_open()
        if self._as_of is not None:
            raise Unsupported("follow on a historical view cannot observe future commits; follow the live database")
        if interval <= 0:
            raise FGraphTypeError(f"follow interval {interval!r} must be positive; use seconds such as 0.5")
        cursor = GENESIS_TX if since is None else self._resolve_time(since)
        while True:
            self._refresh_cache()
            latest = self._latest_tx()
            if latest > cursor:
                rows = self._connection.execute(
                    "SELECT tx FROM fgraph_events WHERE tx>? AND tx<=? ORDER BY tx", (cursor, latest)
                ).fetchall()
                for row in rows:
                    transaction = int(row["tx"])
                    record = self._event_record_for_tx(transaction)
                    # The cursor is local transport state; portable records do
                    # not leak file-local transaction identifiers.
                    cursor = transaction
                    yield record
            time.sleep(interval)

    def why(self, ref: Any, attr: str | None = None) -> list[dict[str, Any]]:
        """Return current facts with their full transaction entity maps."""
        self._ensure_open()
        entity = cast(int, self._resolve_read(ref))
        attribute = None
        if attr is not None:
            attribute = self._names.get(attr)
            if attribute is None:
                raise NotFound(f"attribute {attr!r} was not found; use a known attribute")
        rows = self._visible_fact_rows(entity=entity, attribute=attribute)
        result = []
        for row in rows:
            rendered = self._render_view_row(row)
            rendered["provenance"] = self.at(int(row["tx"])).entity(int(row["tx"]))
            result.append(rendered)
        return result

    @contextlib.contextmanager
    def speculate(self) -> Generator[Db]:
        """Allow reads/writes in a savepoint that is always rolled back."""
        self._ensure_writable()
        if self._speculation_depth:
            raise Unsupported("nested speculation is unavailable in API v1; use one speculate() scope")
        self._savepoint_counter += 1
        name = f"fgraph_speculate_{self._savepoint_counter}"
        self._connection.execute(f"SAVEPOINT {name}")
        self._speculation_depth += 1
        try:
            yield self
        finally:
            self._connection.execute(f"ROLLBACK TO {name}")
            self._connection.execute(f"RELEASE {name}")
            self._speculation_depth -= 1
            self._refresh_cache(force=True)

    def undo(
        self,
        transaction: int,
        *,
        by: str | None = None,
        operation_id: str | None = None,
        if_basis_tx: int | None = None,
    ) -> TxReport:
        """Apply the inverse of one transaction as an audited compensation."""
        self._ensure_writable()
        if (
            not isinstance(transaction, int)
            or isinstance(transaction, bool)
            or not INT64_MIN <= transaction <= INT64_MAX
        ):
            raise FGraphTypeError(f"transaction {transaction!r} is invalid; use a signed 64-bit transaction id")
        if transaction <= GENESIS_TX:
            raise Unsupported(f"system transaction {transaction} cannot be undone; choose a user transaction above 64")

        def prepare_operations() -> list[list[Any]]:
            # Undo must inspect the target under the same writer transaction as
            # its inverse, otherwise a newer equal assertion can be retracted.
            self._tx_metadata(transaction)
            asserted = self._connection.execute(
                "SELECT * FROM fgraph_facts WHERE tx=? AND e<>? AND rx IS NULL ORDER BY id",
                (transaction, transaction),
            ).fetchall()
            retracted = self._connection.execute(
                "SELECT * FROM fgraph_facts WHERE rx=? ORDER BY id", (transaction,)
            ).fetchall()
            operations: list[list[Any]] = [
                [
                    "retract",
                    self._name_or_id(int(row["e"])),
                    self._id_names[int(row["a"])],
                    self._wire(int(row["t"]), row["v"]),
                ]
                for row in asserted
            ]
            operations.extend(
                [
                    "assert",
                    self._name_or_id(int(row["e"])),
                    self._id_names[int(row["a"])],
                    self._wire(int(row["t"]), row["v"]),
                ]
                for row in retracted
            )
            return operations

        request: dict[str, Any] = {"operation": "undo", "tx": transaction}
        if by is not None:
            request["by"] = by
        request_hash = hashlib.sha256(_canonical_json_document(request).encode()).digest()
        return self.transact(
            [],
            tx={"fgraph/undoes": {"ref": transaction}},
            by=by,
            operation_id=operation_id,
            if_basis_tx=if_basis_tx,
            _request_hash_override=request_hash,
            _prepare_data=prepare_operations,
        )

    def excise(
        self,
        ref: Any,
        *,
        operation_id: str | None = None,
        if_basis_tx: int | None = None,
    ) -> TxReport:
        """Physically erase an entity and inbound references, then audit the erasure."""
        self._ensure_writable()
        _validate_operation_id(operation_id)
        if if_basis_tx is not None and (
            not isinstance(if_basis_tx, int)
            or isinstance(if_basis_tx, bool)
            or not GENESIS_TX <= if_basis_tx <= INT64_MAX
        ):
            raise FGraphTypeError(
                f"if_basis_tx={if_basis_tx!r} is invalid; use a committed transaction id at least {GENESIS_TX}"
            )
        try:
            request_hash = hashlib.sha256(
                _canonical_json_document(
                    {
                        "operation": "excise",
                        "ref": self._canonical_request_value(ref),
                    }
                ).encode()
            ).digest()
        except (TypeError, ValueError, UnicodeError) as exc:
            raise FGraphTypeError(
                "excision request cannot be canonicalized; use a name, integer id, or typed identity selector"
            ) from exc
        with self._atomic():
            basis = self._latest_tx()
            if operation_id is not None:
                existing = self._connection.execute(
                    "SELECT ev.tx,ev.request_hash,i.gid,receipt.v AS at_value,"
                    "(SELECT max(prior.tx) FROM fgraph_events prior WHERE prior.tx<ev.tx) AS basis_tx "
                    "FROM fgraph_events ev JOIN fgraph_ids i ON i.id=ev.tx "
                    "JOIN fgraph_facts receipt ON receipt.e=ev.tx AND receipt.a=1 AND receipt.tx=ev.tx "
                    "WHERE ev.operation_id=?",
                    (operation_id,),
                ).fetchone()
                if existing is not None:
                    if bytes(existing["request_hash"]) != request_hash:
                        raise Conflict(
                            f"operation_id {operation_id!r} was already used for a different request; "
                            "reuse it only for an exact retry"
                        )
                    return TxReport(
                        status="already_applied",
                        event=str(uuid.UUID(bytes=bytes(existing["gid"]))),
                        basis_tx=int(existing["basis_tx"]),
                        tx=int(existing["tx"]),
                        at=int(existing["at_value"]),
                    )
            if if_basis_tx is not None and basis != if_basis_tx:
                raise Conflict(
                    f"basis transaction changed from {if_basis_tx} to {basis}; refresh state and retry the excision"
                )
            entity = cast(int, self._resolve_read(ref))
            is_transaction = self._connection.execute(
                "SELECT 1 FROM fgraph_facts WHERE e=? AND a=1 AND tx=e", (entity,)
            ).fetchone()
            if entity <= GENESIS_TX or is_transaction is not None:
                raise Unsupported(
                    f"entity {self._name_or_id(entity)!r} is a system/transaction entity and cannot be excised; "
                    "excise only application entities"
                )
            already_excised = self._connection.execute(
                "SELECT 1 FROM fgraph_facts WHERE a=11 AND t=? AND v=? AND rx IS NULL LIMIT 1",
                (REF, entity),
            ).fetchone()
            if already_excised is not None:
                raise Conflict(
                    f"entity {self._name_or_id(entity)!r} was already excised under another operation; "
                    "retry the original operation receipt"
                )
            pending = self._new_pending()
            transaction = self._allocate(pending)
            at_value = self._next_timestamp()
            event = self._next_event(transaction)
            self._persist_allocations(pending, transaction, event)
            asserted: list[sqlite3.Row] = [
                self._insert_raw_fact(transaction, 1, Encoded(INSTANT, at_value, at_value), transaction)
            ]
            erased = self._connection.execute(
                "SELECT * FROM fgraph_facts WHERE e=? OR a=? OR (t=? AND v=?) ORDER BY id",
                (entity, entity, REF, entity),
            ).fetchall()
            redacted_transactions = {
                transaction_id
                for row in erased
                for transaction_id in (int(row["tx"]), None if row["rx"] is None else int(row["rx"]))
                if transaction_id is not None
            }
            selector = self._identity_selector(entity)
            for retained in self._connection.execute(
                "SELECT tx FROM fgraph_events WHERE event_data IS NOT NULL ORDER BY tx"
            ):
                retained_tx = int(retained["tx"])
                if _event_mentions_selector(self._event_record_for_tx(retained_tx), selector):
                    redacted_transactions.add(retained_tx)
            redacts = sorted(self._event_id_for_tx(transaction_id) for transaction_id in redacted_transactions)
            ids = [int(row["id"]) for row in erased]
            blob_candidates = [bytes(row["v"]) for row in erased if int(row["t"]) in (7, 8, 9)]
            rendered_erased = [self._render_row(row, rx_override=transaction) for row in erased]
            self._connection.executemany("DELETE FROM fgraph_fts WHERE rowid=?", ((fact_id,) for fact_id in ids))
            self._connection.execute(
                "DELETE FROM fgraph_facts WHERE e=? OR a=? OR (t=? AND v=?)",
                (entity, entity, REF, entity),
            )
            asserted.append(self._insert_raw_fact(transaction, 11, Encoded(REF, entity, entity), transaction))
            self._connection.executemany(
                "UPDATE fgraph_events SET event_data=NULL WHERE tx=?",
                ((transaction_id,) for transaction_id in sorted(redacted_transactions)),
            )

            event_record = {
                "fgraph": "event/1",
                "event": str(event),
                "at": at_value,
                "created": [],
                "asserted": [],
                "retracted": [],
                "redacted": True,
                "redacts": redacts,
            }
            event_data = _canonical_event_data(event_record)
            self._connection.execute(
                "INSERT INTO fgraph_events(tx,event_hash,event_data,operation_id,request_hash) VALUES (?,?,?,?,?)",
                (
                    transaction,
                    hashlib.sha256(event_data.encode()).digest(),
                    event_data,
                    operation_id,
                    request_hash if operation_id is not None else None,
                ),
            )
            self._gc_blobs(blob_candidates)
            self._connection.execute("UPDATE fgraph_meta SET value=? WHERE key='next_id'", (pending.next_id,))
            return TxReport(
                status="applied",
                event=str(event),
                basis_tx=basis,
                tx=transaction,
                at=at_value,
                asserted=[self._render_row(row) for row in asserted],
                retracted=rendered_erased,
            )

    def _event_id_for_tx(self, transaction: int) -> str:
        row = self._connection.execute(
            "SELECT gid FROM fgraph_ids WHERE id=? AND name IS NULL", (transaction,)
        ).fetchone()
        if row is None or row["gid"] is None:
            raise FormatError(f"transaction {transaction} is missing its stable event identity")
        try:
            return str(uuid.UUID(bytes=bytes(row["gid"])))
        except (TypeError, ValueError) as exc:
            raise FormatError(
                f"transaction {transaction} has an invalid event identity; restore a valid snapshot"
            ) from exc

    def iter_snapshot(self) -> Generator[str]:
        """Yield one checksummed snapshot/1 line at a time."""
        with self._read_snapshot():
            yield from self._iter_snapshot()

    def _iter_snapshot(self) -> Generator[str]:
        basis = self._as_of if self._as_of is not None else self._latest_tx()
        created_at_row = self._connection.execute("SELECT value FROM fgraph_meta WHERE key='created_at'").fetchone()
        if created_at_row is None or not isinstance(created_at_row["value"], int):
            raise FormatError("snapshot cannot read integer fgraph_meta.created_at; run doctor or restore a backup")
        stream_hash = hashlib.sha256()

        def emit(record: Mapping[str, Any]) -> str:
            line = _canonical_json_document(record) + "\n"
            stream_hash.update(line.encode())
            return line

        yield emit(
            {
                "fgraph": "snapshot/1",
                "format": FORMAT_VERSION,
                "created_at": int(created_at_row["value"]),
                "basis": self._event_id_for_tx(basis),
            }
        )
        receipt_count = 0
        receipts = self._connection.execute(
            "SELECT tx,event_hash,event_data,operation_id,request_hash "
            "FROM fgraph_events WHERE tx>? AND tx<=? ORDER BY tx",
            (GENESIS_TX, basis),
        )
        for receipt in receipts:
            receipt_count += 1
            transaction = int(receipt["tx"])
            metadata = self._tx_metadata(transaction)
            created = [
                self._identity_selector(int(row["id"]))
                for row in self._connection.execute(
                    "SELECT id FROM fgraph_ids WHERE created_tx=? AND id<>? ORDER BY id",
                    (transaction, transaction),
                )
            ]
            yield emit(
                {
                    "receipt": {
                        "event": self._event_id_for_tx(transaction),
                        "at": metadata["at"],
                        "origin_at": metadata.get("imported_at", metadata["at"]),
                        "event_hash": bytes(receipt["event_hash"]).hex(),
                        "event_data": (
                            None
                            if receipt["event_data"] is None
                            else self._decode_event_data(
                                self._event_id_for_tx(transaction),
                                bytes(receipt["event_hash"]),
                                receipt["event_data"],
                            )
                        ),
                        "operation_id": receipt["operation_id"],
                        "request_hash": (
                            None if receipt["request_hash"] is None else bytes(receipt["request_hash"]).hex()
                        ),
                        "created": created,
                    }
                }
            )
        fact_count = 0
        facts = self._connection.execute(
            "SELECT * FROM fgraph_facts WHERE tx>? AND tx<=? ORDER BY id", (GENESIS_TX, basis)
        )
        for row in facts:
            fact_count += 1
            fact = self._portable_fact_tuple(row)
            yield emit(
                {
                    "fact": [
                        *fact,
                        self._event_id_for_tx(int(row["tx"])),
                        (
                            None
                            if row["rx"] is None or int(row["rx"]) > basis
                            else self._event_id_for_tx(int(row["rx"]))
                        ),
                    ]
                }
            )
        yield (
            _canonical_json_document(
                {
                    "fgraph": "end",
                    "sha256": stream_hash.hexdigest(),
                    "receipts": receipt_count,
                    "facts": fact_count,
                }
            )
            + "\n"
        )

    def snapshot(self, writer: TextIO | None = None) -> str | None:
        """Produce a checksummed exact logical snapshot at this view's basis."""
        lines = self.iter_snapshot()
        try:
            if writer is not None:
                for line in lines:
                    writer.write(line)
                return None
            return "".join(lines)
        finally:
            lines.close()

    @staticmethod
    def _snapshot_selector_key(selector: Any) -> tuple[str, str]:
        if isinstance(selector, str):
            return "name", selector
        if not isinstance(selector, Mapping) or set(selector) != {"eid"} or not isinstance(selector["eid"], str):
            raise FGraphTypeError("snapshot identity selector must be a name or {'eid': canonical-uuid}")
        try:
            parsed = uuid.UUID(selector["eid"])
        except ValueError as exc:
            raise FGraphTypeError(f"snapshot EID {selector['eid']!r} is not a UUID") from exc
        if selector["eid"] != str(parsed) or parsed.variant != uuid.RFC_4122:
            raise FGraphTypeError(f"snapshot EID {selector['eid']!r} is not a canonical RFC 4122 UUID")
        return "eid", str(parsed)

    def restore(self, source: str | Iterable[str] | TextIO) -> None:
        """Restore one exact snapshot/1 stream into a pristine database."""
        self._ensure_writable()
        from fgraph.jsonio import loads

        raw_lines = source.splitlines() if isinstance(source, str) else list(source)
        lines = [line for line in raw_lines if line.strip()]
        if len(lines) < 2:
            raise FGraphTypeError("snapshot is truncated; header and footer are required")
        parsed = [loads(line, context=f"snapshot line {index}") for index, line in enumerate(lines, start=1)]
        header = parsed[0]
        footer = parsed[-1]
        if (
            not isinstance(header, Mapping)
            or header.get("fgraph") != "snapshot/1"
            or header.get("format") != FORMAT_VERSION
            or not isinstance(footer, Mapping)
            or footer.get("fgraph") != "end"
            or not isinstance(footer.get("sha256"), str)
        ):
            raise FGraphTypeError("snapshot header/footer is invalid or targets another format version")
        canonical_body = [_canonical_json_document(record) for record in parsed[:-1]]
        expected_hash = hashlib.sha256(("\n".join(canonical_body) + "\n").encode()).hexdigest()
        if footer["sha256"] != expected_hash:
            raise Conflict("snapshot digest does not match its body; reject the truncated or modified stream")
        if (
            not isinstance(footer.get("receipts"), int)
            or isinstance(footer.get("receipts"), bool)
            or not isinstance(footer.get("facts"), int)
            or isinstance(footer.get("facts"), bool)
            or int(footer["receipts"]) < 0
            or int(footer["facts"]) < 0
        ):
            raise FGraphTypeError("snapshot footer counts must be non-negative integers")

        body = parsed[1:-1]
        receipt_wrappers = [record for record in body if isinstance(record, Mapping) and set(record) == {"receipt"}]
        fact_wrappers = [record for record in body if isinstance(record, Mapping) and set(record) == {"fact"}]
        if (
            len(receipt_wrappers) != int(footer["receipts"])
            or len(fact_wrappers) != int(footer["facts"])
            or len(receipt_wrappers) + len(fact_wrappers) != len(body)
        ):
            raise FGraphTypeError("snapshot footer counts or record kinds do not match the body")
        expected_basis: Any = str(GENESIS_EVENT)
        if receipt_wrappers:
            last_receipt = receipt_wrappers[-1]["receipt"]
            expected_basis = last_receipt.get("event") if isinstance(last_receipt, Mapping) else None
        if header.get("basis") != expected_basis:
            raise Conflict("snapshot header basis does not match its final transaction receipt")
        try:
            created_at = int(encode({"instant": header.get("created_at")}).logical)
        except FGraphTypeError as exc:
            raise FGraphTypeError("snapshot created_at must be representable integer microseconds") from exc

        with self._atomic():
            if self._latest_tx() != GENESIS_TX:
                raise Conflict("restore requires a pristine database; use apply for an ordered event stream")
            self._connection.execute("UPDATE fgraph_meta SET value=? WHERE key='created_at'", (created_at,))
            self._connection.execute(
                "UPDATE fgraph_facts SET v=? WHERE e=? AND a=1 AND tx=?", (created_at, GENESIS_TX, GENESIS_TX)
            )
            genesis_record = {
                "fgraph": "event/1",
                "event": str(GENESIS_EVENT),
                "at": created_at,
                "created": list(SYSTEM_NAMES),
                "asserted": [],
                "retracted": [],
            }
            genesis_data = _canonical_event_data(genesis_record)
            self._connection.execute(
                "UPDATE fgraph_events SET event_hash=?,event_data=? WHERE tx=?",
                (hashlib.sha256(genesis_data.encode()).digest(), genesis_data, GENESIS_TX),
            )

            identities: dict[tuple[str, str], int] = {}
            for row in self._connection.execute("SELECT id,name,gid FROM fgraph_ids ORDER BY id"):
                selector = str(row["name"]) if row["name"] is not None else {"eid": str(uuid.UUID(bytes=row["gid"]))}
                identities[self._snapshot_selector_key(selector)] = int(row["id"])
            events: dict[str, int] = {}
            receipt_metadata: dict[int, tuple[int, int]] = {}
            next_id = FIRST_USER_ID
            for wrapper in receipt_wrappers:
                receipt = wrapper["receipt"]
                required = {
                    "event",
                    "at",
                    "origin_at",
                    "event_hash",
                    "event_data",
                    "operation_id",
                    "request_hash",
                    "created",
                }
                if not isinstance(receipt, Mapping) or set(receipt) != required:
                    raise FGraphTypeError(f"snapshot receipt is malformed; expected fields {sorted(required)!r}")
                event_key = self._snapshot_selector_key({"eid": receipt["event"]})
                event_text = event_key[1]
                if event_text in events or event_key in identities:
                    raise Conflict(f"snapshot repeats event or identity {event_text}")
                if not isinstance(receipt["created"], list):
                    raise FGraphTypeError("snapshot receipt created must be an identity-selector array")
                reserved: list[tuple[Any, tuple[str, str], int]] = []
                for selector in receipt["created"]:
                    key = self._snapshot_selector_key(selector)
                    if key in identities or any(existing == key for _selector, existing, _identifier in reserved):
                        raise Conflict(f"snapshot repeats identity {key[0]}:{key[1]}")
                    if key[0] == "name":
                        self._validate_name(key[1])
                    reserved.append((selector, key, next_id))
                    next_id += 1
                transaction = next_id
                next_id += 1
                for selector, key, identifier in reserved:
                    gid = None if key[0] == "name" else uuid.UUID(key[1]).bytes
                    name = selector if isinstance(selector, str) else None
                    self._connection.execute(
                        "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,?,?,?)",
                        (identifier, name, gid, transaction),
                    )
                    identities[key] = identifier
                self._connection.execute(
                    "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,NULL,?,?)",
                    (transaction, uuid.UUID(event_text).bytes, transaction),
                )
                identities[event_key] = transaction
                events[event_text] = transaction
                event_hash = receipt["event_hash"]
                event_data = receipt["event_data"]
                request_hash = receipt["request_hash"]
                operation_id = receipt["operation_id"]
                if not isinstance(event_hash, str) or re.fullmatch(r"[0-9a-f]{64}", event_hash) is None:
                    raise FGraphTypeError("snapshot receipt event_hash must be 32-byte lowercase hex")
                stored_event_data: str | None
                if event_data is None:
                    stored_event_data = None
                elif isinstance(event_data, Mapping):
                    stored_event_data = _canonical_event_data(event_data)
                    try:
                        self._decode_event_data(
                            event_text,
                            bytes.fromhex(event_hash),
                            stored_event_data,
                        )
                    except FormatError as exc:
                        raise FGraphTypeError(f"snapshot receipt event_data is invalid: {exc}") from exc
                else:
                    raise FGraphTypeError("snapshot receipt event_data must be an event object or null")
                if operation_id is not None and (
                    not isinstance(operation_id, str) or not 1 <= len(operation_id.encode()) <= 512
                ):
                    raise FGraphTypeError("snapshot receipt operation_id must be null or 1-512 UTF-8 bytes")
                if request_hash is not None and (
                    not isinstance(request_hash, str) or re.fullmatch(r"[0-9a-f]{64}", request_hash) is None
                ):
                    raise FGraphTypeError("snapshot receipt request_hash must be null or 32-byte lowercase hex")
                if (operation_id is None) != (request_hash is None):
                    raise FGraphTypeError("snapshot operation_id and request_hash must both be null or both present")
                try:
                    at = int(encode({"instant": receipt["at"]}).logical)
                    origin_at = int(encode({"instant": receipt["origin_at"]}).logical)
                except FGraphTypeError as exc:
                    raise FGraphTypeError("snapshot receipt at/origin_at must be integer microseconds") from exc
                receipt_metadata[transaction] = (at, origin_at)
                self._connection.execute(
                    "INSERT INTO fgraph_events(tx,event_hash,event_data,operation_id,request_hash) VALUES (?,?,?,?,?)",
                    (
                        transaction,
                        bytes.fromhex(event_hash),
                        stored_event_data,
                        operation_id,
                        None if request_hash is None else bytes.fromhex(request_hash),
                    ),
                )

            def resolve_selector(selector: Any) -> int:
                key = self._snapshot_selector_key(selector)
                identifier = identities.get(key)
                if identifier is None:
                    raise NotFound(f"snapshot fact references unknown identity {key[0]}:{key[1]}")
                return identifier

            for wrapper in fact_wrappers:
                fact = wrapper["fact"]
                if not isinstance(fact, list) or len(fact) != 6 or not isinstance(fact[3], str):
                    raise FGraphTypeError(
                        "snapshot fact must be [entity,attribute,value,tag,assert-event,retract-event]"
                    )
                entity = resolve_selector(fact[0])
                attribute = resolve_selector(fact[1])
                event_key = self._snapshot_selector_key({"eid": fact[4]})
                transaction = events.get(event_key[1])
                if transaction is None:
                    raise NotFound("snapshot fact assertion event is unknown")
                value = fact[2]
                if fact[3] == "ref":
                    if not isinstance(value, Mapping) or set(value) != {"ref"}:
                        raise FGraphTypeError("snapshot ref fact must use {'ref': identity-selector}")
                    value = {"ref": resolve_selector(value["ref"])}
                else:
                    value = self._decode_tagged_wire_value(value, fact[3])
                encoded = encode(value, int)
                if type_name(encoded.tag) != fact[3]:
                    raise FGraphTypeError("snapshot fact value does not match its logical tag")
                inserted = self._insert_raw_fact(entity, attribute, encoded, transaction)
                if fact[5] is not None:
                    retraction_key = self._snapshot_selector_key({"eid": fact[5]})
                    retraction = events.get(retraction_key[1])
                    if retraction is None or retraction <= transaction:
                        raise Conflict("snapshot retraction event is unknown or not later than assertion")
                    self._connection.execute("UPDATE fgraph_facts SET rx=? WHERE id=?", (retraction, inserted["id"]))
                    self._connection.execute("DELETE FROM fgraph_fts WHERE rowid=?", (inserted["id"],))

            self._connection.execute("UPDATE fgraph_meta SET value=? WHERE key='next_id'", (next_id,))
            self._refresh_cache(force=True)
            for transaction, expected in receipt_metadata.items():
                metadata = self._tx_metadata(transaction)
                actual = (int(metadata["at"]), int(metadata.get("imported_at", metadata["at"])))
                if actual != expected:
                    raise Conflict(
                        f"snapshot receipt {self._event_id_for_tx(transaction)} metadata disagrees with its facts"
                    )
            checked, fatal = self._doctor_report()
            if fatal:
                raise FormatError(f"restored snapshot violates format invariants: {fatal!r}")
            if checked["ok"] is not True:
                raise FormatError(f"restored snapshot has derived-state drift: {checked['problems']!r}")

    def backup(self, path: str | os.PathLike[str]) -> None:
        """Create, verify, and durably publish a consistent hot backup."""
        self._ensure_open()
        target = Path(path).expanduser().resolve()
        if self.path != ":memory:" and target == Path(self.path).expanduser().resolve():
            raise Conflict(f"backup destination {os.fspath(path)!r} is the open database; choose a different path")
        if target.exists():
            raise Conflict(f"backup destination {os.fspath(path)!r} already exists; choose a new file")
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                prefix=f".{target.name}.",
                suffix=".tmp",
                dir=target.parent,
                delete=False,
            ) as temporary_file:
                temporary = Path(temporary_file.name)
            destination = sqlite3.connect(temporary)
            try:
                self._connection.backup(destination)
            finally:
                destination.close()
            with connect(temporary, read_only=True) as verified:
                report = verified.doctor()
                if report["ok"] is not True:
                    raise FormatError(f"backup failed verification: {report['problems']!r}")
            _sync_backup_file(temporary)
            try:
                os.link(temporary, target)
            except FileExistsError as exc:
                raise Conflict(f"backup destination {os.fspath(path)!r} already exists; choose a new file") from exc
            temporary.unlink()
            temporary = None
            _sync_backup_directory(target.parent)
        except (OSError, sqlite3.DatabaseError) as exc:
            raise FormatError(
                f"backup destination {os.fspath(path)!r} cannot be created; choose a writable new path"
            ) from exc
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def stats(self) -> dict[str, Any]:
        """Return compact file/format and fact counts."""
        self._ensure_open()
        horizon = self._as_of
        identity_rows = self._connection.execute(
            "SELECT identity.id,identity.name FROM fgraph_ids identity "
            "WHERE (? IS NULL OR identity.created_tx<=?) "
            "AND NOT EXISTS (SELECT 1 FROM fgraph_events event WHERE event.tx=identity.id)",
            (horizon, horizon),
        ).fetchall()
        attributes = sum(
            ATTRIBUTE_PATTERN.fullmatch(str(row["name"])) is not None
            for row in identity_rows
            if row["name"] is not None
        )
        facts = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_facts WHERE ? IS NULL OR tx<=?", (horizon, horizon)
            ).fetchone()[0]
        )
        visibility, parameters = self._visibility()
        live_count_sql = f"SELECT count(*) FROM fgraph_facts WHERE {visibility}"  # noqa: S608
        live_facts = int(self._connection.execute(live_count_sql, parameters).fetchone()[0])
        transactions = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_events WHERE ? IS NULL OR tx<=?",
                (horizon, horizon),
            ).fetchone()[0]
        )
        blobs = int(
            self._connection.execute(
                "SELECT count(DISTINCT b.hash) FROM fgraph_blobs b JOIN fgraph_facts f ON f.v=b.hash "
                "AND f.t IN (7,8,9) WHERE ? IS NULL OR f.tx<=?",
                (horizon, horizon),
            ).fetchone()[0]
        )
        size = 0 if self.path == ":memory:" else Path(self.path).stat().st_size
        return {
            "application_id": int(self._connection.execute("PRAGMA application_id").fetchone()[0]),
            "format_version": int(self._connection.execute("PRAGMA user_version").fetchone()[0]),
            "entities": len(identity_rows),
            "attributes": attributes,
            "facts": facts,
            "live_facts": live_facts,
            "transactions": transactions,
            "blobs": blobs,
            "size": size,
        }

    def attributes(self, prefix: str | None = None, *, include_system: bool = False) -> list[dict[str, Any]]:
        """Describe known attributes and their effective current behavior."""
        self._ensure_open()
        if prefix is not None and not isinstance(prefix, str):
            raise FGraphTypeError(f"attribute prefix {prefix!r} is invalid; use text or None")
        if not isinstance(include_system, bool):
            raise FGraphTypeError(f"include_system={include_system!r} is invalid; use a boolean")
        visibility, parameters = self._visibility()
        result: list[dict[str, Any]] = []
        for row in self._connection.execute(
            "SELECT id,name FROM fgraph_ids WHERE name IS NOT NULL AND (? IS NULL OR created_tx<=?) ORDER BY name",
            (self._as_of, self._as_of),
        ):
            attribute_id, name = int(row["id"]), str(row["name"])
            if ATTRIBUTE_PATTERN.fullmatch(name) is None:
                continue
            if not include_system and name.startswith("fgraph/"):
                continue
            if prefix is not None and not name.startswith(prefix):
                continue
            observed = self._connection.execute(
                f"SELECT t,count(*) AS facts FROM fgraph_facts WHERE a=? AND {visibility} GROUP BY t",  # noqa: S608
                (attribute_id, *parameters),
            ).fetchall()
            schema = self._schema(attribute_id, self._as_of)
            types = {type_name(int(item["t"])) for item in observed}
            if schema.type is not None:
                types.add(schema.type)
            description: dict[str, Any] = {
                "name": name,
                "types": sorted(types),
                "facts": sum(int(item["facts"]) for item in observed),
                "many": schema.many,
                "unique": schema.unique,
                "nohistory": schema.deletes_history,
            }
            if schema.dims is not None:
                description["dims"] = schema.dims
            if schema.doc is not None:
                description["doc"] = schema.doc
            if schema.vector_model is not None:
                description["vector_model"] = schema.vector_model
            result.append(description)
        return result

    def schema(self, prefix: str | None = None, *, include_system: bool = False) -> dict[str, Any]:
        """Return a digestible temporal schema snapshot for humans and agents."""
        self._ensure_open()
        if prefix is not None and not isinstance(prefix, str):
            raise FGraphTypeError(f"attribute prefix {prefix!r} is invalid; use text or None")
        if not isinstance(include_system, bool):
            raise FGraphTypeError(f"include_system={include_system!r} is invalid; use a boolean")

        basis = self._as_of if self._as_of is not None else self._latest_tx()
        visibility, parameters = self._visibility(basis)
        attributes: list[dict[str, Any]] = []
        schema_names = {
            5: "many",
            6: "unique",
            7: "nohistory",
            8: "type",
            9: "dims",
            10: "doc",
            14: "vector_model",
        }
        rows = self._connection.execute(
            "SELECT id,name FROM fgraph_ids WHERE name IS NOT NULL AND created_tx<=? ORDER BY name",
            (basis,),
        ).fetchall()
        for row in rows:
            attribute_id, name = int(row["id"]), str(row["name"])
            if ATTRIBUTE_PATTERN.fullmatch(name) is None:
                continue
            if not include_system and name.startswith("fgraph/"):
                continue
            if prefix is not None and not name.startswith(prefix):
                continue
            declared: dict[str, Any] = {}
            for declaration in self._connection.execute(
                f"SELECT a,t,v FROM fgraph_facts WHERE e=? AND (a BETWEEN 5 AND 10 OR a=14) "  # noqa: S608
                f"AND {visibility} ORDER BY id",
                (attribute_id, *parameters),
            ):
                declared[schema_names[int(declaration["a"])]] = self._logical(int(declaration["t"]), declaration["v"])
            effective_schema = self._schema(attribute_id, basis)
            effective = {
                "type": effective_schema.type,
                "many": effective_schema.many,
                "unique": effective_schema.unique,
                "nohistory": effective_schema.deletes_history,
                "dims": effective_schema.dims,
                "doc": effective_schema.doc,
                "vector_model": effective_schema.vector_model,
            }
            observed_rows = self._connection.execute(
                f"SELECT t,count(*) AS facts "  # noqa: S608
                f"FROM fgraph_facts WHERE a=? AND {visibility} GROUP BY t ORDER BY t",
                (attribute_id, *parameters),
            ).fetchall()
            observed_entities = int(
                self._connection.execute(
                    f"SELECT count(DISTINCT e) FROM fgraph_facts WHERE a=? AND {visibility}",  # noqa: S608
                    (attribute_id, *parameters),
                ).fetchone()[0]
            )
            observed = {
                "types": sorted({type_name(int(item["t"])) for item in observed_rows}),
                "live_facts": sum(int(item["facts"]) for item in observed_rows),
                "entities": observed_entities,
            }
            attributes.append({"name": name, "declared": declared, "effective": effective, "observed": observed})

        shapes: list[dict[str, Any]] = []
        shape_rows = self._connection.execute(
            f"SELECT DISTINCT e FROM fgraph_facts WHERE a BETWEEN 16 AND 18 "  # noqa: S608
            f"AND {visibility} ORDER BY e",
            parameters,
        ).fetchall()
        for shape_row in shape_rows:
            shape_id = int(shape_row["e"])
            required: list[Any] = []
            allowed: list[Any] = []
            closed = False
            for row in self._connection.execute(
                f"SELECT a,t,v FROM fgraph_facts WHERE e=? AND a IN (16,17,18) AND {visibility} ORDER BY a,v",  # noqa: S608
                (shape_id, *parameters),
            ):
                if int(row["a"]) == 16:
                    required.append(self._name_or_id(int(row["v"])))
                elif int(row["a"]) == 17:
                    allowed.append(self._name_or_id(int(row["v"])))
                else:
                    closed = self._logical(int(row["t"]), row["v"]) is True
            shapes.append(
                {
                    "name": self._name_or_id(shape_id),
                    "required": sorted(required, key=str),
                    "allowed": sorted(allowed, key=str),
                    "closed": closed,
                }
            )
        digest_payload = {
            "attributes": [
                {"name": item["name"], "declared": item["declared"], "effective": item["effective"]}
                for item in attributes
            ],
            "shapes": shapes,
        }
        return {
            "basis_tx": basis,
            "digest": "sha256:" + hashlib.sha256(_canonical_json_document(digest_payload).encode()).hexdigest(),
            "attributes": attributes,
            "shapes": shapes,
        }

    def _normalize_schema_manifest(self, manifest: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(manifest, Mapping) or manifest.get("fgraph") != "schema/1":
            raise SchemaError("schema manifest must be an object with fgraph='schema/1'")
        if set(manifest) - {"fgraph", "digest", "attributes", "shapes"}:
            raise SchemaError("schema manifest has unknown fields; use fgraph, digest, attributes, and shapes")
        raw_attributes = manifest.get("attributes", [])
        raw_shapes = manifest.get("shapes", [])
        if not isinstance(raw_attributes, list) or not isinstance(raw_shapes, list):
            raise SchemaError("schema manifest attributes and shapes must be arrays")
        declaration_fields = {"type", "many", "unique", "nohistory", "dims", "doc", "vector_model"}
        attributes: list[dict[str, Any]] = []
        seen_attributes: set[str] = set()
        for item in raw_attributes:
            if not isinstance(item, Mapping) or set(item) != {"name", "declared"}:
                raise SchemaError("schema manifest attributes need exactly name and declared")
            name, declared = item["name"], item["declared"]
            if not isinstance(name, str) or not isinstance(declared, Mapping):
                raise SchemaError("schema manifest attribute name must be text and declared must be an object")
            self._validate_attribute(name)
            if name in seen_attributes:
                raise SchemaError(f"schema manifest repeats attribute {name!r}")
            seen_attributes.add(name)
            if set(declared) - declaration_fields:
                raise SchemaError(f"schema manifest attribute {name!r} has an unknown declaration field")
            normalized = dict(declared)
            if "type" in normalized and normalized["type"] not in TYPE_NAMES:
                raise SchemaError(f"schema manifest attribute {name!r} has an unsupported type")
            for field in ("many", "unique", "nohistory"):
                if field in normalized and not isinstance(normalized[field], bool):
                    raise SchemaError(f"schema manifest attribute {name!r} field {field!r} must be boolean")
            if "dims" in normalized and (
                not isinstance(normalized["dims"], int)
                or isinstance(normalized["dims"], bool)
                or normalized["dims"] <= 0
            ):
                raise SchemaError(f"schema manifest attribute {name!r} dims must be a positive integer")
            for field in ("doc", "vector_model"):
                if field in normalized and not isinstance(normalized[field], str):
                    raise SchemaError(f"schema manifest attribute {name!r} field {field!r} must be text")
            if "vector_model" in normalized and not normalized["vector_model"].strip():
                raise SchemaError(f"schema manifest attribute {name!r} vector_model must be non-blank")
            if normalized:
                attributes.append({"name": name, "declared": normalized})

        shapes: list[dict[str, Any]] = []
        seen_shapes: set[str] = set()
        for item in raw_shapes:
            if not isinstance(item, Mapping) or set(item) != {"name", "required", "allowed", "closed"}:
                raise SchemaError("schema manifest shapes need exactly name, required, allowed, and closed")
            name = item["name"]
            if not isinstance(name, str):
                raise SchemaError("schema manifest shape name must be text")
            self._validate_name(name)
            if name in seen_shapes:
                raise SchemaError(f"schema manifest repeats shape {name!r}")
            seen_shapes.add(name)
            required, allowed, closed = item["required"], item["allowed"], item["closed"]
            if (
                not isinstance(required, list)
                or not isinstance(allowed, list)
                or not isinstance(closed, bool)
                or any(not isinstance(value, str) for value in [*required, *allowed])
            ):
                raise SchemaError("schema manifest shape fields have invalid types")
            for attribute in [*required, *allowed]:
                self._validate_attribute(attribute)
            normalized_required = sorted(set(required))
            normalized_allowed = sorted(set([*required, *allowed] if closed else allowed))
            shapes.append(
                {"name": name, "required": normalized_required, "allowed": normalized_allowed, "closed": closed}
            )
        payload = {
            "fgraph": "schema/1",
            "attributes": sorted(attributes, key=lambda item: item["name"]),
            "shapes": sorted(shapes, key=lambda item: str(item["name"])),
        }
        return {
            **payload,
            "digest": "sha256:" + hashlib.sha256(_canonical_json_document(payload).encode()).hexdigest(),
        }

    def schema_manifest(self) -> dict[str, Any]:
        """Export portable declarations and shapes without observed data."""
        snapshot = self.schema()
        return self._normalize_schema_manifest(
            {
                "fgraph": "schema/1",
                "attributes": [
                    {"name": attribute["name"], "declared": attribute["declared"]}
                    for attribute in snapshot["attributes"]
                    if attribute["declared"]
                ],
                "shapes": snapshot["shapes"],
            }
        )

    def check_schema_manifest(self, manifest: Mapping[str, Any]) -> dict[str, Any]:
        """Compare a manifest with the current declaration/shape control plane."""
        desired = self._normalize_schema_manifest(manifest)
        current = self.schema_manifest()
        current_items = {("attribute", item["name"]): item["declared"] for item in current["attributes"]} | {
            ("shape", str(item["name"])): item for item in current["shapes"]
        }
        desired_items = {("attribute", item["name"]): item["declared"] for item in desired["attributes"]} | {
            ("shape", str(item["name"])): item for item in desired["shapes"]
        }
        changes = [
            {"kind": key[0], "name": key[1], "before": current_items.get(key), "after": desired_items.get(key)}
            for key in sorted(current_items.keys() | desired_items.keys())
            if current_items.get(key) != desired_items.get(key)
        ]
        return {
            "basis_tx": self._as_of if self._as_of is not None else self._latest_tx(),
            "valid": not changes,
            "current_digest": current["digest"],
            "desired_digest": desired["digest"],
            "changes": changes,
        }

    def apply_schema_manifest(
        self,
        manifest: Mapping[str, Any],
        *,
        operation_id: str | None = None,
        if_basis_tx: int | None = None,
    ) -> TxReport:
        """Atomically replace declarations and shape definitions from schema/1."""
        desired = self._normalize_schema_manifest(manifest)
        schema_fields = {
            "many": "fgraph/many",
            "unique": "fgraph/unique",
            "nohistory": "fgraph/nohistory",
            "type": "fgraph/type",
            "dims": "fgraph/dims",
            "doc": "fgraph/doc",
            "vector_model": "fgraph/vector-model",
        }

        def prepare_operations() -> list[Any]:
            # Full replacement discovery must share the writer transaction with
            # planning and commit so a concurrent declaration cannot survive it.
            current = self.schema_manifest()
            declaration_attributes = {item["name"] for item in [*current["attributes"], *desired["attributes"]]}
            operations: list[Any] = [
                ["retract", attribute, system_attribute]
                for attribute in sorted(declaration_attributes)
                for system_attribute in schema_fields.values()
            ]
            for attribute in desired["attributes"]:
                definition = {"id": attribute["name"]}
                definition.update({schema_fields[field]: value for field, value in attribute["declared"].items()})
                operations.append(definition)
            shape_names = {str(item["name"]) for item in [*current["shapes"], *desired["shapes"]]}
            operations.extend(
                ["retract", name, system_attribute]
                for name in sorted(shape_names)
                for system_attribute in ("fgraph/shape-required", "fgraph/shape-allowed", "fgraph/shape-closed")
            )
            for shape in desired["shapes"]:
                definition = {"id": shape["name"], "fgraph/shape-closed": shape["closed"]}
                if shape["required"]:
                    definition["fgraph/shape-required"] = [{"ref": value} for value in shape["required"]]
                if shape["allowed"]:
                    definition["fgraph/shape-allowed"] = [{"ref": value} for value in shape["allowed"]]
                operations.append(definition)
            return operations

        request_hash = hashlib.sha256(
            _canonical_json_document({"operation": "schema-apply", "manifest": desired}).encode()
        ).digest()
        return self.transact(
            [],
            operation_id=operation_id,
            if_basis_tx=if_basis_tx,
            _request_hash_override=request_hash,
            _prepare_data=prepare_operations,
        )

    def _doctor_logical_invariants(self) -> tuple[int, int]:
        schema_problems = 0
        attribute_rows = self._connection.execute(
            "SELECT a AS id FROM fgraph_facts UNION "
            "SELECT e AS id FROM fgraph_facts WHERE a IN (5,6,7,8,9,10,14) ORDER BY id"
        ).fetchall()
        for attribute_row in attribute_rows:
            attribute = int(attribute_row["id"])
            try:
                schema = self._schema(attribute)
                rows = self._connection.execute(
                    "SELECT * FROM fgraph_facts WHERE a=? AND rx IS NULL ORDER BY id", (attribute,)
                ).fetchall()
                values = [
                    (int(row["e"]), Encoded(int(row["t"]), row["v"], self._logical(int(row["t"]), row["v"])))
                    for row in rows
                ]
                if schema.type is not None and (
                    schema.type not in TYPE_NAMES
                    or any(not value_matches(schema.type, value) for _entity, value in values)
                ):
                    schema_problems += 1
                if not schema.many:
                    counts: dict[int, int] = {}
                    for entity, _value in values:
                        counts[entity] = counts.get(entity, 0) + 1
                    if any(count > 1 for count in counts.values()):
                        schema_problems += 1
                if schema.unique:
                    if schema.type in (None, "json", "vector"):
                        schema_problems += 1
                    owners: dict[tuple[int, Any], set[int]] = {}
                    for entity, value in values:
                        owners.setdefault((value.tag, value.stored), set()).add(entity)
                    if any(len(entities) > 1 for entities in owners.values()):
                        schema_problems += 1
                if schema.dims is not None and (
                    schema.type != "vector"
                    or schema.dims <= 0
                    or any(value.tag == VECTOR and len(value.logical) != schema.dims for _entity, value in values)
                ):
                    schema_problems += 1
                if schema.vector_model is not None and schema.type != "vector":
                    schema_problems += 1
            except Exception:
                # Doctor must continue across corrupted logical values so it can
                # return a bounded count instead of leaking an implementation error.
                schema_problems += 1

        shape_violations = 0

        def definition(shape: int) -> tuple[set[int], set[int], bool]:
            nonlocal shape_violations
            required: set[int] = set()
            allowed: set[int] = set()
            closed = False
            rows = self._connection.execute(
                "SELECT a,t,v FROM fgraph_facts WHERE e=? AND a BETWEEN 16 AND 18 AND rx IS NULL ORDER BY id",
                (shape,),
            ).fetchall()
            for row in rows:
                attribute = int(row["a"])
                if attribute == 16 and int(row["t"]) == REF:
                    required.add(int(row["v"]))
                elif attribute == 17 and int(row["t"]) == REF:
                    allowed.add(int(row["v"]))
                elif attribute == 18 and int(row["t"]) == BOOL:
                    closed = bool(row["v"])
            for member in required | allowed:
                name = self._id_names.get(member)
                if name is None or ATTRIBUTE_PATTERN.fullmatch(name) is None:
                    shape_violations += 1
            if closed:
                shape_violations += len(required - allowed)
            return required, allowed, closed

        definitions: dict[int, tuple[set[int], set[int], bool]] = {}
        for row in self._connection.execute(
            "SELECT DISTINCT e FROM fgraph_facts WHERE a BETWEEN 16 AND 18 AND rx IS NULL ORDER BY e"
        ):
            shape = int(row["e"])
            definitions[shape] = definition(shape)
        for row in self._connection.execute("SELECT DISTINCT e FROM fgraph_facts WHERE a=15 AND rx IS NULL ORDER BY e"):
            entity = int(row["e"])
            try:
                live = self._connection.execute(
                    "SELECT a,t,v FROM fgraph_facts WHERE e=? AND rx IS NULL ORDER BY id", (entity,)
                ).fetchall()
                shapes = [int(fact["v"]) for fact in live if int(fact["a"]) == 15 and int(fact["t"]) == REF]
                if not shapes:
                    shape_violations += 1
                    continue
                shape = shapes[-1]
                shape_definition = definitions.get(shape)
                if shape_definition is None:
                    shape_definition = definition(shape)
                    definitions[shape] = shape_definition
                required, allowed, closed = shape_definition
                present = {int(fact["a"]) for fact in live}
                shape_violations += len(required - present)
                if closed:
                    shape_violations += sum(
                        attribute >= FIRST_USER_ID and attribute not in required | allowed for attribute in present
                    )
            except Exception:
                shape_violations += 1
        return schema_problems, shape_violations

    def _doctor_report(self) -> tuple[dict[str, Any], list[str]]:
        integrity_rows = [str(row[0]) for row in self._connection.execute("PRAGMA integrity_check")]
        integrity = integrity_rows[0] if integrity_rows else "missing result"
        fatal = [f"integrity_check: {message}" for message in integrity_rows if message != "ok"]
        layout_problems = self._layout_problems()
        fatal.extend(f"layout: {problem}" for problem in layout_problems)
        if layout_problems:
            # Layout drift is never repairable. Stop before later checks assume
            # canonical columns, views, and indexes still exist.
            return (
                {
                    "ok": False,
                    "integrity": integrity,
                    "problems": fatal,
                    "repair_needed": False,
                    "repaired": False,
                    "fts_rows": 0,
                    "expected_fts_rows": 0,
                    "orphaned_blobs": 0,
                    "unverifiable_event_hashes": 0,
                    "schema_problems": 0,
                    "shape_violations": 0,
                    "fts_rows_rebuilt": 0,
                    "orphaned_blobs_removed": 0,
                },
                fatal,
            )
        metadata = {
            str(row["key"]): row["value"] for row in self._connection.execute("SELECT key, value FROM fgraph_meta")
        }
        maximum = int(
            self._connection.execute(
                "SELECT max(identifier) FROM ("
                "SELECT id AS identifier FROM fgraph_ids UNION ALL "
                "SELECT e FROM fgraph_facts UNION ALL SELECT a FROM fgraph_facts UNION ALL "
                "SELECT tx FROM fgraph_facts UNION ALL SELECT rx FROM fgraph_facts WHERE rx IS NOT NULL "
                "UNION ALL SELECT CAST(v AS INTEGER) FROM fgraph_facts WHERE t=0)"
            ).fetchone()[0]
            or GENESIS_TX
        )
        next_id = metadata.get("next_id")
        if maximum == INT64_MAX:
            fatal.append(
                "allocator exhausted: maximum identifier is int64 max; migrate retained data to a new fgraph file"
            )
        elif not isinstance(next_id, int) or next_id != maximum + 1:
            fatal.append(f"next_id: expected {maximum + 1}, found {next_id!r}")
        invalid_identities = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_ids WHERE id<=0 OR (id>? AND id<?)",
                (len(SYSTEM_NAMES), GENESIS_TX),
            ).fetchone()[0]
        )
        if invalid_identities:
            fatal.append(f"invalid identity ids: {invalid_identities}")
        actual_system_names = {
            int(row["id"]): row["name"]
            for row in self._connection.execute(
                "SELECT id,CAST(name AS BLOB) AS name FROM fgraph_ids WHERE id BETWEEN 1 AND ?",
                (len(SYSTEM_NAMES),),
            )
        }
        invalid_system_identities = sum(
            actual_system_names.get(identifier) != expected.encode()
            for identifier, expected in enumerate(SYSTEM_NAMES, start=1)
        )
        if invalid_system_identities:
            fatal.append(f"invalid system identities: {invalid_system_identities}")
        invalid_fact_ids = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_facts WHERE id<=0 OR e<=0 OR a<=0 OR tx<? "
                "OR (rx IS NOT NULL AND rx<?) OR (t=0 AND CAST(v AS INTEGER)<=0)",
                (GENESIS_TX, GENESIS_TX),
            ).fetchone()[0]
        )
        if invalid_fact_ids:
            fatal.append(f"invalid fact identifiers: {invalid_fact_ids}")
        named_transactions = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_ids i WHERE i.name IS NOT NULL AND EXISTS (SELECT 1 FROM fgraph_facts f "
                "WHERE f.e=i.id AND f.a=1 AND f.tx=f.e AND f.rx IS NULL)"
            ).fetchone()[0]
        )
        if named_transactions:
            fatal.append(f"named identities overlap transaction receipts: {named_transactions}")
        missing_registry = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_facts f "
                "LEFT JOIN fgraph_ids ie ON ie.id=f.e LEFT JOIN fgraph_ids ia ON ia.id=f.a "
                "LEFT JOIN fgraph_ids itx ON itx.id=f.tx LEFT JOIN fgraph_ids irx ON irx.id=f.rx "
                "LEFT JOIN fgraph_ids iv ON f.t=0 AND iv.id=CAST(f.v AS INTEGER) "
                "WHERE ie.id IS NULL OR ia.id IS NULL OR itx.id IS NULL "
                "OR (f.rx IS NOT NULL AND irx.id IS NULL) OR (f.t=0 AND iv.id IS NULL)"
            ).fetchone()[0]
        )
        if missing_registry:
            fatal.append(f"facts reference missing identity registry rows: {missing_registry}")
        identities_from_future = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_facts f "
                "JOIN fgraph_ids ie ON ie.id=f.e JOIN fgraph_ids ia ON ia.id=f.a "
                "LEFT JOIN fgraph_ids iv ON f.t=0 AND iv.id=CAST(f.v AS INTEGER) "
                "WHERE ie.created_tx>f.tx OR ia.created_tx>f.tx OR (f.t=0 AND iv.created_tx>f.tx)"
            ).fetchone()[0]
        )
        if identities_from_future:
            fatal.append(f"facts predate their identity registry rows: {identities_from_future}")
        invalid_created_transactions = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_ids i WHERE NOT EXISTS ("
                "SELECT 1 FROM fgraph_facts receipt WHERE receipt.e=i.created_tx AND receipt.a=1 "
                "AND receipt.tx=receipt.e AND receipt.rx IS NULL)"
            ).fetchone()[0]
        )
        if invalid_created_transactions:
            fatal.append(f"identity rows reference missing creation transactions: {invalid_created_transactions}")
        invalid_events = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_facts receipt "
                "LEFT JOIN fgraph_events ev ON ev.tx=receipt.e LEFT JOIN fgraph_ids i ON i.id=receipt.e "
                "WHERE receipt.a=1 AND receipt.tx=receipt.e AND receipt.rx IS NULL "
                "AND (ev.tx IS NULL OR i.name IS NOT NULL OR i.gid IS NULL)"
            ).fetchone()[0]
        ) + int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_events ev WHERE NOT EXISTS ("
                "SELECT 1 FROM fgraph_facts receipt WHERE receipt.e=ev.tx AND receipt.a=1 "
                "AND receipt.tx=receipt.e AND receipt.rx IS NULL)"
            ).fetchone()[0]
        )
        if invalid_events:
            fatal.append(f"transaction event registry mismatches: {invalid_events}")
        genesis_identity = self._connection.execute(
            "SELECT gid,created_tx FROM fgraph_ids WHERE id=?", (GENESIS_TX,)
        ).fetchone()
        if (
            genesis_identity is None
            or bytes(genesis_identity["gid"]) != GENESIS_EVENT.bytes
            or int(genesis_identity["created_tx"]) != GENESIS_TX
        ):
            fatal.append("genesis event identity is not the canonical format-v2 UUID")
        invalid_derived_gids = 0
        for event_row in self._connection.execute(
            "SELECT ev.tx,i.gid FROM fgraph_events ev JOIN fgraph_ids i ON i.id=ev.tx ORDER BY ev.tx"
        ):
            try:
                namespace = uuid.UUID(bytes=bytes(event_row["gid"]))
            except (ValueError, TypeError):
                invalid_derived_gids += 1
                continue
            created = self._connection.execute(
                "SELECT name,gid FROM fgraph_ids WHERE created_tx=? AND id<>? ORDER BY id",
                (event_row["tx"], event_row["tx"]),
            ).fetchall()
            for ordinal, row in enumerate(created):
                if row["name"] is None and (
                    row["gid"] is None or bytes(row["gid"]) != _derived_entity_id(namespace, ordinal).bytes
                ):
                    invalid_derived_gids += 1
        if invalid_derived_gids:
            fatal.append(f"invalid event-derived anonymous identities: {invalid_derived_gids}")
        unverifiable_event_hashes = 0
        redacted_targets: set[str] = set()
        null_events: dict[str, int] = {}
        event_rows = self._connection.execute(
            "SELECT ev.tx,ev.event_hash,ev.event_data,ev.operation_id,ev.request_hash,i.gid "
            "FROM fgraph_events ev "
            "JOIN fgraph_ids i ON i.id=ev.tx ORDER BY ev.tx"
        ).fetchall()
        for event_row in event_rows:
            transaction = int(event_row["tx"])
            try:
                event = str(uuid.UUID(bytes=bytes(event_row["gid"])))
            except (TypeError, ValueError):
                fatal.append(f"event {transaction} has an invalid event identity")
                continue
            try:
                event_hash = bytes(event_row["event_hash"])
                request_hash = None if event_row["request_hash"] is None else bytes(event_row["request_hash"])
                operation_id = event_row["operation_id"]
                operation_valid = operation_id is None or (
                    isinstance(operation_id, str)
                    and 1 <= len(operation_id.encode()) <= 512
                    and not any(ord(character) < 32 or ord(character) == 127 for character in operation_id)
                )
                receipt_fields_valid = (
                    len(event_hash) == 32
                    and operation_valid
                    and (request_hash is None or len(request_hash) == 32)
                    and ((operation_id is None) == (request_hash is None))
                )
            except (TypeError, UnicodeError):
                receipt_fields_valid = False
                event_hash = b""
            if not receipt_fields_valid:
                fatal.append(f"event {transaction} has malformed physical receipt fields")
                continue
            if event_row["event_data"] is None:
                unverifiable_event_hashes += 1
                null_events[event] = transaction
                continue
            try:
                record = self._decode_event_data(
                    event,
                    event_hash,
                    event_row["event_data"],
                )
                event_metadata = self._tx_metadata(transaction)
                if record["at"] != event_metadata.get("imported_at", event_metadata["at"]):
                    raise FormatError("event at timestamp differs from its transaction receipt")
            except Exception as exc:
                fatal.append(f"event {transaction} payload is invalid: {exc}")
                continue
            is_excision = self._connection.execute(
                "SELECT 1 FROM fgraph_facts WHERE e=? AND tx=e AND a=11 AND t=? AND rx IS NULL",
                (transaction, REF),
            ).fetchone()
            if record.get("redacted") is True:
                if is_excision is None:
                    fatal.append(f"redacted event {transaction} has no audited fgraph/excised marker")
                    continue
                for target in cast(list[str], record["redacts"]):
                    target_row = self._connection.execute(
                        "SELECT ev.tx,ev.event_data FROM fgraph_events ev "
                        "JOIN fgraph_ids i ON i.id=ev.tx WHERE i.gid=?",
                        (uuid.UUID(target).bytes,),
                    ).fetchone()
                    if target_row is None or int(target_row["tx"]) >= transaction:
                        fatal.append(f"redacted event {transaction} names unknown or non-prior event {target}")
                    elif target_row["event_data"] is not None:
                        fatal.append(f"redacted event {transaction} target {target} still has an event payload")
                    else:
                        redacted_targets.add(target)
            elif is_excision is not None:
                fatal.append(f"excision transaction {transaction} does not carry a canonical redaction payload")
        for event, transaction in null_events.items():
            if event not in redacted_targets:
                fatal.append(f"event {transaction} has a NULL payload without an audited excision redaction")
        missing_transactions = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_facts f WHERE NOT EXISTS (SELECT 1 FROM fgraph_facts receipt "
                "WHERE receipt.e=f.tx AND receipt.a=1 AND receipt.tx=receipt.e AND receipt.rx IS NULL)"
            ).fetchone()[0]
        )
        if missing_transactions:
            fatal.append(f"facts reference missing asserting transactions: {missing_transactions}")
        missing_retractions = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_facts f WHERE f.rx IS NOT NULL AND NOT EXISTS "
                "(SELECT 1 FROM fgraph_facts receipt WHERE receipt.e=f.rx AND receipt.a=1 "
                "AND receipt.tx=receipt.e AND receipt.rx IS NULL)"
            ).fetchone()[0]
        )
        if missing_retractions:
            fatal.append(f"facts reference missing retracting transactions: {missing_retractions}")
        genesis = self._connection.execute(
            "SELECT v, t, tx, rx FROM fgraph_facts WHERE e=? AND a=? ORDER BY id", (GENESIS_TX, 1)
        ).fetchall()
        created_at = metadata.get("created_at")
        valid_genesis = (
            len(genesis) == 1
            and int(genesis[0]["t"]) == INSTANT
            and int(genesis[0]["tx"]) == GENESIS_TX
            and genesis[0]["rx"] is None
        )
        if not valid_genesis:
            fatal.append("genesis receipt: expected one live format-v2 self-receipt")
        elif not isinstance(created_at, int) or created_at != int(genesis[0]["v"]):
            fatal.append(f"created_at: expected genesis timestamp {int(genesis[0]['v'])}, found {created_at!r}")
        last_genesis_fact_id = 1 + len(SYSTEM_TYPES) + len(SYSTEM_DOCS) + 2
        actual_genesis_facts = {
            int(row["id"]): (
                int(row["e"]),
                int(row["a"]),
                row["v"],
                str(row["storage_class"]),
                int(row["t"]),
                int(row["tx"]),
                row["rx"],
            )
            for row in self._connection.execute(
                "SELECT id,e,a,CAST(v AS BLOB) AS v,typeof(v) AS storage_class,t,tx,rx "
                "FROM fgraph_facts WHERE id BETWEEN 2 AND ? OR (tx=? AND id NOT BETWEEN 1 AND ?) ORDER BY id",
                (last_genesis_fact_id, GENESIS_TX, last_genesis_fact_id),
            )
        }
        expected_genesis_facts = {
            identifier + 1: (identifier, 8, declared_type.encode(), "text", TEXT, GENESIS_TX, None)
            for identifier, declared_type in enumerate(SYSTEM_TYPES, start=1)
        }
        expected_genesis_facts.update(
            {
                identifier + len(SYSTEM_TYPES) + 1: (
                    identifier,
                    10,
                    doc.encode(),
                    "text",
                    TEXT,
                    GENESIS_TX,
                    None,
                )
                for identifier, doc in enumerate(SYSTEM_DOCS, start=1)
            }
        )
        expected_genesis_facts.update(
            {
                2 + len(SYSTEM_TYPES) + len(SYSTEM_DOCS): (16, 5, b"1", "integer", BOOL, GENESIS_TX, None),
                3 + len(SYSTEM_TYPES) + len(SYSTEM_DOCS): (17, 5, b"1", "integer", BOOL, GENESIS_TX, None),
            }
        )
        invalid_genesis_facts = sum(
            actual_genesis_facts.pop(fact_id, None) != expected for fact_id, expected in expected_genesis_facts.items()
        ) + len(actual_genesis_facts)
        if invalid_genesis_facts:
            fatal.append(f"invalid genesis facts: {invalid_genesis_facts}")
        dangling_attributes = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_facts f LEFT JOIN fgraph_ids i ON i.id=f.a WHERE i.id IS NULL"
            ).fetchone()[0]
        )
        if dangling_attributes:
            fatal.append(f"dangling attributes: {dangling_attributes}")
        invalid_value_tags = int(
            self._connection.execute("SELECT count(*) FROM fgraph_facts WHERE t NOT BETWEEN 0 AND 10").fetchone()[0]
        )
        if invalid_value_tags:
            fatal.append(f"invalid value tags: {invalid_value_tags}")
        invalid_physical_values = sum(
            not _valid_physical_value(int(row["t"]), str(row["storage_class"]), row["scalar"], row["raw"])
            for row in self._connection.execute(
                "SELECT t,typeof(v) AS storage_class,"
                "CASE WHEN t IN (4,10) THEN NULL ELSE v END AS scalar,CAST(v AS BLOB) AS raw "
                "FROM fgraph_facts WHERE t BETWEEN 0 AND 10"
            )
        )
        if invalid_physical_values:
            fatal.append(f"invalid physical values: {invalid_physical_values}")
        missing_blobs = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_facts f LEFT JOIN fgraph_blobs b ON b.hash=f.v "
                "WHERE f.t IN (7,8,9) AND b.hash IS NULL"
            ).fetchone()[0]
        )
        if missing_blobs:
            fatal.append(f"missing blobs: {missing_blobs}")
        invalid_blob_ids: set[int] = set()
        for row in self._connection.execute(
            "SELECT b.rowid AS blob_id,b.hash,b.data,f.t FROM fgraph_blobs b "
            "JOIN fgraph_facts f ON f.t IN (7,8,9) AND f.v=b.hash ORDER BY b.rowid,f.t"
        ):
            if not _valid_indirect_blob(int(row["t"]), row["hash"], row["data"]):
                invalid_blob_ids.add(int(row["blob_id"]))
        invalid_blobs = len(invalid_blob_ids)
        if invalid_blobs:
            fatal.append(f"invalid indirect blobs: {invalid_blobs}")
        invalid_intervals = int(
            self._connection.execute("SELECT count(*) FROM fgraph_facts WHERE rx IS NOT NULL AND rx<=tx").fetchone()[0]
        )
        if invalid_intervals:
            fatal.append(f"invalid transaction intervals: {invalid_intervals}")
        schema_problems, shape_violations = self._doctor_logical_invariants()
        if schema_problems:
            fatal.append(f"schema invariants violated: {schema_problems}")
        if shape_violations:
            fatal.append(f"shape invariants violated: {shape_violations}")
        orphaned_blobs = int(
            self._connection.execute(
                "SELECT count(*) FROM fgraph_blobs WHERE NOT EXISTS ("
                "SELECT 1 FROM fgraph_facts WHERE t IN (7,8,9) AND v=fgraph_blobs.hash)"
            ).fetchone()[0]
        )
        actual_fts = [
            (int(row["rowid"]), str(row["text"]))
            for row in self._connection.execute("SELECT rowid,text FROM fgraph_fts ORDER BY rowid")
        ]
        expected_fts_rows = int(
            self._connection.execute("SELECT count(*) FROM fgraph_facts WHERE rx IS NULL AND t IN (4,8)").fetchone()[0]
        )
        unsafe_values = bool(invalid_value_tags or invalid_physical_values or missing_blobs or invalid_blobs)
        expected_fts = []
        if not unsafe_values:
            text_rows = self._connection.execute(
                "SELECT id,v,t FROM fgraph_facts WHERE rx IS NULL AND t IN (4,8) ORDER BY id"
            ).fetchall()
            expected_fts = [(int(row["id"]), str(self._logical(int(row["t"]), row["v"]))) for row in text_rows]
        fts_mismatch = unsafe_values or actual_fts != expected_fts
        repair_problems = []
        if fts_mismatch:
            repair_problems.append("full-text index differs from live text facts")
        if orphaned_blobs:
            repair_problems.append(f"orphaned blobs: {orphaned_blobs}")
        problems = [*fatal, *repair_problems]
        return (
            {
                "ok": not problems,
                "integrity": integrity,
                "problems": problems,
                "repair_needed": bool(repair_problems),
                "repaired": False,
                "fts_rows": len(actual_fts),
                "expected_fts_rows": expected_fts_rows,
                "orphaned_blobs": orphaned_blobs,
                "unverifiable_event_hashes": unverifiable_event_hashes,
                "schema_problems": schema_problems,
                "shape_violations": shape_violations,
                "fts_rows_rebuilt": 0,
                "orphaned_blobs_removed": 0,
            },
            fatal,
        )

    def doctor(self, *, repair: bool = False) -> dict[str, Any]:
        """Check invariants without mutation, optionally repairing derived state."""
        if not isinstance(repair, bool):
            raise FGraphTypeError(f"doctor repair={repair!r} is invalid; use a boolean")
        if not repair:
            if self._closed:
                raise FormatError("fgraph connection is closed; call connect() to open a new handle")
            try:
                # Doctor must inspect malformed identity bytes rather than fail
                # while preparing convenience lookup caches for normal reads.
                self._refresh_cache(force=True, tolerate_malformed_gids=True)
                report, _ = self._doctor_report()
                return report
            except sqlite3.ProgrammingError as exc:
                raise FormatError("fgraph connection is closed; call connect() to open a new handle") from exc
            finally:
                # A subsequent ordinary read must fail closed on the same drift.
                self._cache_version = -1
        self._ensure_writable()
        with self._atomic():
            report, fatal = self._doctor_report()
            if fatal:
                raise FormatError(
                    f"doctor found non-rebuildable format problems {fatal!r}; restore from a valid backup"
                )
            rows = self._connection.execute(
                "SELECT id,v,t FROM fgraph_facts WHERE rx IS NULL AND t IN (4,8) ORDER BY id"
            ).fetchall()
            self._connection.execute("DELETE FROM fgraph_fts")
            self._connection.executemany(
                "INSERT INTO fgraph_fts(rowid,text) VALUES (?,?)",
                ((int(row["id"]), self._logical(int(row["t"]), row["v"])) for row in rows),
            )
            removed_blobs = self._gc_blobs()
            self._connection.execute("ANALYZE")
            repaired, fatal = self._doctor_report()
            if fatal:
                raise FormatError(
                    f"doctor repair exposed non-rebuildable format problems {fatal!r}; restore a valid backup"
                )
            repaired["repaired"] = True
            repaired["fts_rows_rebuilt"] = len(rows)
            repaired["orphaned_blobs_removed"] = removed_blobs
            return repaired

    def search(
        self,
        text: str | None = None,
        vector: Sequence[float] | None = None,
        k: int = 10,
        expand: int = 0,
        filters: Sequence[Sequence[Any]] = (),
        vector_attribute: str | None = None,
        text_attributes: Sequence[str] = (),
        **options: Any,
    ) -> SearchResult:
        """Run current keyword/vector RRF search and optional graph expansion."""
        from fgraph.search import search

        self._ensure_open()
        if options:
            raise FGraphTypeError(
                f"unknown search options {sorted(options)!r}; use vector_attribute, text_attributes, filters, k, expand"
            )
        return search(self, text, vector, k, expand, filters, vector_attribute, text_attributes)

    def close(self) -> None:
        """Close an owning connection; historical views detach only."""
        if self._closed:
            return
        if self._owns_connection:
            self._connection.close()
        self._closed = True

    def __enter__(self) -> Self:
        self._ensure_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
