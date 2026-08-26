import { createHash, randomUUID } from "node:crypto";
import {
  closeSync,
  existsSync,
  fsyncSync,
  linkSync,
  lstatSync,
  openSync,
  realpathSync,
  statSync,
  unlinkSync,
} from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import Database from "better-sqlite3";

import {
  Conflict,
  FormatError,
  NotFound,
  QueryError,
  ReadOnly,
  SchemaError,
  TooLarge,
  TypeError,
  Unsupported,
} from "./errors.js";
import {
  JsonFloat,
  canonicalJson,
  canonicalValueJson,
  compareUnicode,
  parseJson,
  parseJsonValue,
  stringifyJson,
} from "./jsonio.js";
import type {
  AttributeInfo,
  DatomPage,
  RenderedFact,
  Result,
  SchemaManifest,
  SchemaManifestCheck,
  SchemaSnapshot,
  SearchResult,
  TxReport,
  WireInteger,
} from "./models.js";
import { evaluate, isPatternClause, planPattern } from "./query.js";
import { runSearch, type SearchOptions } from "./search.js";
import {
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
  JSON_TAG,
  MAX_VALUE_BYTES,
  REF,
  TEXT,
  TEXT_REF,
  TYPE_NAMES,
  VECTOR,
  type Cell,
  type Encoded,
  type Stored,
  encode,
  equalStored,
  indirectDigest,
  instantValue,
  isRecord,
  publicInteger,
  storedKey,
  typeName,
  valueMatches,
  wireValue,
} from "./values.js";

export const APPLICATION_ID = 0x66677261;
export const FORMAT_VERSION = 2;
export const GENESIS_TX = 64n;
export const FIRST_USER_ID = 65n;
export const DEFAULT_QUERY_BUDGET = 100_000;
export const MAX_EVENT_BYTES = 8 * MAX_VALUE_BYTES + 64 * 1024;
const MAX_CURSOR_BYTES = 4096;
const IMPORTED_AT_ATTRIBUTE = 13n;
const IMPORTED_AT_NAME = "fgraph/imported-at";

export const SYSTEM_NAMES = [
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
  IMPORTED_AT_NAME,
  "fgraph/vector-model",
  "fgraph/shape",
  "fgraph/shape-required",
  "fgraph/shape-allowed",
  "fgraph/shape-closed",
] as const;

const SYSTEM_TYPES = [
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
] as const;
const SYSTEM_DOCS = [
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
] as const;

const GENESIS_EVENT = "00000000-0000-4000-8000-000000000040";

const OMITTED = Symbol("omitted");

const SCHEMA_SQL = `
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
`;

export type EntityRef = string | number | bigint | [string, unknown];
export type Clock = number | bigint | (() => number | bigint);

export interface ConnectOptions {
  clock?: Clock;
  eventId?: (transaction?: bigint) => string;
  readOnly?: boolean;
  queryBudget?: number;
}

export interface TransactOptions {
  source?: string;
  by?: string;
  meta?: unknown;
  tx?: Record<string, unknown>;
  operationId?: string;
  ifBasisTx?: WireInteger;
}

export interface DeclareOptions {
  type?: string;
  ref?: boolean;
  many?: boolean;
  unique?: boolean;
  nohistory?: boolean;
  dims?: number;
  doc?: string;
  vectorModel?: string;
  operationId?: string;
  ifBasisTx?: WireInteger;
}

export interface RawRow {
  id: bigint;
  e: bigint;
  a: bigint;
  v: Stored;
  t: bigint;
  tx: bigint;
  rx: bigint | null;
}

export interface QueryDatom {
  row: RawRow;
  eventTx: bigint;
  added: boolean;
}

interface IdRow {
  id: bigint;
  name: string | null;
  gid: Buffer | null;
  created_tx: bigint;
}

interface Assertion {
  kind: "assert";
  e: bigint;
  a: bigint;
  value: Encoded;
}

interface Retraction {
  kind: "retract";
  e: bigint;
  a: bigint | null;
  value: Encoded | null;
}

type Operation = Assertion | Retraction;

export interface Schema {
  type: string | null;
  many: boolean;
  unique: boolean;
  nohistory: boolean | null;
  dims: number | null;
  doc: string | null;
  vectorModel: string | null;
}

interface Pending {
  operations: Operation[];
  casTargets: Set<string>;
  casOperations: Set<Operation>;
  txFacts: Array<[bigint, Encoded]>;
  reportIds: Map<string, bigint>;
  nextId: bigint;
  names: Map<string, bigint>;
  idNames: Map<bigint, string>;
  allocated: Map<bigint, string | null>;
  schemas: Map<bigint, Schema>;
}

function blankSchema(): Schema {
  return {
    type: null,
    many: false,
    unique: false,
    nohistory: null,
    dims: null,
    doc: null,
    vectorModel: null,
  };
}

function deletesHistory(schema: Schema): boolean {
  return schema.nohistory ?? schema.type === "vector";
}

function indirectDataProblem(tag: number, data: unknown): string | null {
  if (tag === TEXT_REF) {
    if (typeof data !== "string")
      return "an indirect text fact has the wrong SQLite storage class; restore a valid backup";
    const length = Buffer.byteLength(data, "utf8");
    if (length <= BLOB_THRESHOLD || length > MAX_VALUE_BYTES)
      return "an indirect text blob has an invalid byte length; restore a valid backup";
    return null;
  }
  if (tag === BYTES_REF) {
    if (!Buffer.isBuffer(data))
      return "an indirect bytes fact has the wrong SQLite storage class; restore a valid backup";
    if (data.length <= BLOB_THRESHOLD || data.length > MAX_VALUE_BYTES)
      return "an indirect bytes blob has an invalid byte length; restore a valid backup";
    return null;
  }
  // Callers constrain the remaining indirect tag to VECTOR.
  if (
    !Buffer.isBuffer(data) ||
    data.length === 0 ||
    data.length % 4 !== 0 ||
    data.length > MAX_VALUE_BYTES
  )
    return "a vector blob is not bounded float32 little-endian data; restore a valid backup";
  return null;
}

function indirectBytes(tag: number, data: unknown): Buffer {
  return tag === TEXT_REF
    ? Buffer.from(data as string, "utf8")
    : (data as Buffer);
}

interface PhysicalValueRow {
  t: bigint;
  storage_class: string;
  scalar: Stored | null;
  raw: Buffer;
}

function validPhysicalValue(row: PhysicalValueRow): boolean {
  const tag = Number(row.t);
  if (tag === REF)
    return (
      row.storage_class === "integer" &&
      typeof row.scalar === "bigint" &&
      row.scalar > 0n
    );
  if (tag === BOOL)
    return (
      row.storage_class === "integer" &&
      typeof row.scalar === "bigint" &&
      (row.scalar === 0n || row.scalar === 1n)
    );
  if (tag === INT)
    return row.storage_class === "integer" && typeof row.scalar === "bigint";
  if (tag === FLOAT)
    return (
      row.storage_class === "real" &&
      typeof row.scalar === "number" &&
      Number.isFinite(row.scalar)
    );
  if (tag === TEXT) {
    if (row.storage_class !== "text" || row.raw.length > BLOB_THRESHOLD)
      return false;
    try {
      new TextDecoder("utf-8", { fatal: true }).decode(row.raw);
      return true;
    } catch {
      return false;
    }
  }
  if (tag === INSTANT)
    return (
      row.storage_class === "integer" &&
      typeof row.scalar === "bigint" &&
      row.scalar >= INSTANT_MIN &&
      row.scalar <= INSTANT_MAX
    );
  if (tag === BYTES)
    return row.storage_class === "blob" && row.raw.length <= BLOB_THRESHOLD;
  if (tag === VECTOR || tag === TEXT_REF || tag === BYTES_REF) {
    // The indirect validator owns key, blob-domain, length, and digest checks.
    return true;
  }
  if (
    tag !== JSON_TAG ||
    row.storage_class !== "text" ||
    row.raw.length > MAX_VALUE_BYTES
  )
    return false;
  try {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(row.raw);
    return canonicalValueJson(parseJsonValue(text, "stored JSON")) === text;
  } catch {
    // Malformed bytes and JSON are corruption findings, not doctor failures.
    return false;
  }
}

function asBigInt(value: unknown, what = "integer"): bigint {
  const integer =
    typeof value === "bigint"
      ? value
      : typeof value === "number" && Number.isSafeInteger(value)
        ? BigInt(value)
        : null;
  if (integer !== null && integer >= INT64_MIN && integer <= INT64_MAX)
    return integer;
  throw new TypeError(
    `${what} ${String(value)} is not a lossless signed 64-bit integer; use an int64 bigint or safe integer`,
  );
}

function asNumber(value: bigint): number {
  const result = Number(value);
  if (!Number.isSafeInteger(result))
    throw new FormatError(
      `stored identifier ${value} exceeds JavaScript's safe range; restore a valid format-v2 file`,
    );
  return result;
}

function publicId(value: bigint): number | bigint {
  return publicInteger(value);
}

function factKey(entity: bigint, attribute: bigint, value: Encoded): string {
  return `${entity}:${attribute}:${value.tag}:${storedKey(value.stored)}`;
}

function rowKey(row: RawRow): string {
  return `${row.e}:${row.a}:${row.t}:${storedKey(row.v)}`;
}

function isAssertion(value: Assertion | RawRow): value is Assertion {
  return "kind" in value;
}

function recordEntries(
  value: Record<string, unknown>,
): Array<[string, unknown]> {
  return Object.keys(value)
    .sort()
    .map((key) => [key, value[key]]);
}

function wallClock(): bigint {
  return BigInt(Date.now()) * 1000n;
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolvePromise) =>
    setTimeout(resolvePromise, milliseconds),
  );
}

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;

function uuidBytes(value: string): Buffer {
  const canonical = value.toLowerCase();
  if (!UUID_PATTERN.test(canonical))
    throw new TypeError(
      `event id ${JSON.stringify(value)} is not a canonical UUID; use a lowercase RFC 4122 UUID`,
    );
  return Buffer.from(canonical.replaceAll("-", ""), "hex");
}

function uuidText(value: Buffer): string {
  if (value.length !== 16)
    throw new FormatError(
      "stored global identity is not 16 bytes; restore a valid format-v2 file",
    );
  const hex = value.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function digest(value: unknown): Buffer {
  return createHash("sha256").update(canonicalJson(value), "utf8").digest();
}

function canonicalEventData(record: Record<string, unknown>): string {
  const data = canonicalJson(record);
  const size = Buffer.byteLength(data, "utf8");
  if (size > MAX_EVENT_BYTES)
    throw new TooLarge(
      `canonical event is ${size} bytes; keep one transaction at or below ${MAX_EVENT_BYTES} portable bytes`,
    );
  return data;
}

function eventHash(data: string): Buffer {
  return createHash("sha256").update(data, "utf8").digest();
}

function genesisEvent(at: bigint): Record<string, unknown> {
  return {
    fgraph: "event/1",
    event: GENESIS_EVENT,
    at,
    created: SYSTEM_NAMES,
    asserted: [],
    retracted: [],
  };
}

function portableSelectorKey(value: unknown): string | null {
  if (typeof value === "string") return `name:${value}`;
  if (
    isRecord(value) &&
    Object.keys(value).length === 1 &&
    typeof value.eid === "string"
  )
    return `eid:${value.eid.toLowerCase()}`;
  return null;
}

function eventMentionsSelector(
  record: Record<string, unknown>,
  selector: unknown,
): boolean {
  const target = portableSelectorKey(selector);
  if (target === null) return false;
  const matches = (candidate: unknown): boolean =>
    portableSelectorKey(candidate) === target;
  const references = (value: unknown, tag: unknown): boolean =>
    tag === "ref" &&
    isRecord(value) &&
    Object.keys(value).length === 1 &&
    Object.hasOwn(value, "ref") &&
    matches(value.ref);
  if (Array.isArray(record.created) && record.created.some(matches))
    return true;
  for (const key of ["asserted", "retracted"] as const) {
    const tuples = record[key];
    if (
      Array.isArray(tuples) &&
      tuples.some(
        (tuple) =>
          Array.isArray(tuple) &&
          (matches(tuple[0]) ||
            matches(tuple[1]) ||
            references(tuple[2], tuple[3])),
      )
    )
      return true;
  }
  return (
    Array.isArray(record.tx_facts) &&
    record.tx_facts.some(
      (tuple) =>
        Array.isArray(tuple) &&
        (matches(tuple[0]) || references(tuple[1], tuple[2])),
    )
  );
}

/** RFC 4122 UUIDv5 using the event UUID as namespace and an unsigned ordinal. */
function derivedEntityId(namespace: Buffer, ordinal: bigint): Buffer {
  const name = Buffer.alloc(8);
  name.writeBigUInt64BE(ordinal);
  const value = createHash("sha1")
    .update(namespace)
    .update(name)
    .digest()
    .subarray(0, 16);
  value[6] = ((value[6] as number) & 0x0f) | 0x50;
  value[8] = ((value[8] as number) & 0x3f) | 0x80;
  return value;
}

export function connect(path = "fgraph.db", options: ConnectOptions = {}): Db {
  return new Db(path, options);
}

function openSqlite(
  path: string,
  options: Database.Options,
): Database.Database {
  const uriSetting = process.env.SQLITE_USE_URI;
  if (uriSetting === undefined) process.env.SQLITE_USE_URI = "1";
  try {
    // better-sqlite3 loads its native addon lazily on the first constructor.
    // Default URI support on that boundary so a later static-media fallback
    // works even when the first fgraph handle uses an ordinary file path.
    return new Database(path, options);
  } finally {
    if (uriSetting === undefined) delete process.env.SQLITE_USE_URI;
  }
}

function isReadOnlyDirectoryError(error: unknown): boolean {
  return (
    error instanceof Database.SqliteError &&
    error.code === "SQLITE_READONLY_DIRECTORY"
  );
}

function immutableReadOnlyUri(path: string): string | null {
  let realPath: string;
  try {
    realPath = realpathSync(resolve(path));
  } catch {
    return null;
  }
  for (const suffix of ["-wal", "-shm"]) {
    try {
      lstatSync(`${realPath}${suffix}`);
      return null;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") return null;
    }
  }
  const uri = pathToFileURL(realPath);
  uri.searchParams.set("immutable", "1");
  uri.searchParams.set("mode", "ro");
  return uri.href;
}

/** One synchronous SQLite connection, optionally pinned to a historical transaction. */
export class Db implements Disposable {
  readonly path: string;
  readonly queryBudget: number;
  readonly _connection: Database.Database;
  _asOf: bigint | null = null;
  _querySource: "current" | "history" = "current";
  #readOnly: boolean;
  #ownsConnection = true;
  #closed = false;
  #speculationDepth = 0;
  #savepointCounter = 0;
  #cacheVersion = -1n;
  #names = new Map<string, bigint>();
  #idNames = new Map<bigint, string>();
  #statements = new Map<string, Database.Statement<unknown[], unknown>>();
  #clock: () => bigint;
  #eventId: (transaction?: bigint) => string;

  constructor(path: string, options: ConnectOptions = {}, shared?: Db) {
    this.path = path;
    this.queryBudget = options.queryBudget ?? DEFAULT_QUERY_BUDGET;
    if (!Number.isSafeInteger(this.queryBudget) || this.queryBudget <= 0) {
      throw new TypeError(
        `query budget ${String(this.queryBudget)} is invalid; use a positive safe integer`,
      );
    }
    if (shared !== undefined) {
      this._connection = shared._connection;
      this.#readOnly = true;
      this.#ownsConnection = false;
      this.#clock = shared.#clock;
      this.#eventId = shared.#eventId;
      this.#names = shared.#names;
      this.#idNames = shared.#idNames;
      this.#cacheVersion = shared.#cacheVersion;
      this.#statements = shared.#statements;
      return;
    }
    if (
      options.clock === undefined &&
      process.env.FGRAPH_CLOCK !== undefined &&
      !/^-?[0-9]+$/u.test(process.env.FGRAPH_CLOCK)
    ) {
      throw new TypeError(
        `FGRAPH_CLOCK=${JSON.stringify(process.env.FGRAPH_CLOCK)} is not integer microseconds; set an integer such as 1767225600000000`,
      );
    }
    this.#readOnly = options.readOnly ?? false;
    const eventSeed = process.env.FGRAPH_EVENT_SEED;
    this.#eventId =
      options.eventId ??
      (eventSeed === undefined
        ? () => randomUUID()
        : (transaction) => {
            if (transaction === undefined)
              throw new FormatError(
                "deterministic event generation requires the allocated transaction id",
              );
            const bytes = createHash("sha256")
              .update("fgraph-event/1\0", "utf8")
              .update(eventSeed, "utf8")
              .update("\0", "utf8")
              .update(transaction.toString(), "utf8")
              .digest()
              .subarray(0, 16);
            bytes[6] = ((bytes[6] as number) & 0x0f) | 0x40;
            bytes[8] = ((bytes[8] as number) & 0x3f) | 0x80;
            return uuidText(bytes);
          });
    if (this.#readOnly && path === ":memory:") {
      throw new ReadOnly(
        "a read-only :memory: database cannot be initialized; open an existing file with readOnly: true",
      );
    }
    let opened: Database.Database | undefined;
    try {
      if (this.#readOnly && !existsSync(resolve(path))) {
        throw new NotFound(
          `database ${JSON.stringify(path)} does not exist; initialize it before opening read-only`,
        );
      }
      opened = openSqlite(path, {
        readonly: this.#readOnly,
        fileMustExist: this.#readOnly,
        timeout: 5000,
      });
      this._connection = opened;
      try {
        this.#prepareConnection(options.clock);
      } catch (error) {
        const immutableUri = isReadOnlyDirectoryError(error)
          ? immutableReadOnlyUri(path)
          : null;
        if (!this.#readOnly || immutableUri === null) throw error;
        opened.close();
        if (process.env.SQLITE_USE_URI === "0")
          throw new FormatError(
            "read-only SQLite media requires URI support; set SQLITE_USE_URI=1 before starting Node.js",
          );
        try {
          opened = openSqlite(immutableUri, {
            readonly: true,
            fileMustExist: true,
            timeout: 5000,
          });
        } catch (uriError) {
          if (
            uriError instanceof Database.SqliteError &&
            uriError.code === "SQLITE_CANTOPEN"
          )
            throw new FormatError(
              "read-only SQLite media requires URI support; import fgraph before constructing another better-sqlite3 database or set SQLITE_USE_URI=1 before starting Node.js",
            );
          throw uriError;
        }
        this._connection = opened;
        this.#prepareConnection(options.clock);
      }
    } catch (error) {
      // Constructor failure has no Db handle for the caller to close. Preserve the
      // primary validation error if SQLite itself also rejects cleanup.
      try {
        opened?.close();
      } catch (closeError) {
        if (error instanceof Error && error.cause === undefined)
          error.cause = closeError;
      }
      if (
        error instanceof NotFound ||
        error instanceof ReadOnly ||
        error instanceof FormatError ||
        error instanceof TypeError
      )
        throw error;
      throw new FormatError(
        `file ${JSON.stringify(path)} is not a usable SQLite database; restore a valid file or choose a new path`,
      );
    }
    const supplied = options.clock;
    if (typeof supplied === "function")
      this.#clock = () => instantValue(supplied());
    else if (supplied !== undefined) {
      const start = instantValue(supplied);
      this.#clock = () => start;
    } else if (process.env.FGRAPH_CLOCK !== undefined) {
      const text = process.env.FGRAPH_CLOCK;
      const start = instantValue(BigInt(text));
      this.#clock = () => start;
    } else this.#clock = wallClock;
  }

  #prepareConnection(clock: Clock | undefined): void {
    this._connection.defaultSafeIntegers(true);
    this.#configure();
    this.#validateOrInitialize(clock);
    this.#refreshCache(true);
  }

  #configure(): void {
    if (!this.#readOnly && this.path !== ":memory:") {
      const journal = String(
        this._connection.pragma("journal_mode = WAL", { simple: true }),
      ).toLowerCase();
      if (journal !== "wal")
        throw new FormatError(
          `SQLite selected journal_mode=${journal}; fgraph requires WAL for concurrent durable reads`,
        );
    }
    this._connection.pragma("foreign_keys = OFF");
    this._connection.pragma("busy_timeout = 5000");
    this._connection.pragma("trusted_schema = OFF");
    if (this.#readOnly) this._connection.pragma("query_only = ON");
    else {
      this._connection.pragma("synchronous = FULL");
      this._connection.pragma("wal_autocheckpoint = 1000");
    }
  }

  #layoutState(): "initialized" | "pristine" {
    const applicationId = this._connection.pragma("application_id", {
      simple: true,
    }) as bigint;
    const userVersion = this._connection.pragma("user_version", {
      simple: true,
    }) as bigint;
    const hasMeta = this._connection
      .prepare(
        "SELECT 1 found FROM sqlite_master WHERE type='table' AND name='fgraph_meta'",
      )
      .get();
    if (hasMeta !== undefined) {
      if (
        applicationId !== BigInt(APPLICATION_ID) ||
        userVersion !== BigInt(FORMAT_VERSION)
      ) {
        throw new FormatError(
          `file ${JSON.stringify(this.path)} has application_id=${applicationId} and user_version=${userVersion}; open an fgraph format-v${FORMAT_VERSION} file instead`,
        );
      }
      this.#validateObjects();
      return "initialized";
    }
    if (applicationId !== 0n || userVersion !== 0n) {
      throw new FormatError(
        `file ${JSON.stringify(this.path)} has application_id=${applicationId}, user_version=${userVersion} but no complete fgraph layout; restore the claimed database or use an unmarked SQLite file`,
      );
    }
    const fgraphObject = this._connection
      .prepare(
        "SELECT 1 found FROM sqlite_master WHERE name LIKE 'fgraph_%' LIMIT 1",
      )
      .get();
    if (fgraphObject !== undefined)
      throw new FormatError(
        `file ${JSON.stringify(this.path)} contains a partial fgraph layout; restore a complete file or remove the partial objects intentionally`,
      );
    const foreignObject = this._connection
      .prepare<[], { name: string }>(
        "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' AND name NOT LIKE 'fgraph_%' LIMIT 1",
      )
      .get();
    if (foreignObject !== undefined)
      throw new FormatError(
        `file ${JSON.stringify(this.path)} already contains application object ${JSON.stringify(foreignObject.name)}; fgraph v1 owns a dedicated SQLite file`,
      );
    return "pristine";
  }

  #validateOrInitialize(clock: Clock | undefined): void {
    if (this.#layoutState() === "initialized") return;
    if (this.#readOnly)
      throw new FormatError(
        `file ${JSON.stringify(this.path)} is not initialized as fgraph; open it writable once to initialize`,
      );
    let genesisAt: bigint;
    if (typeof clock === "function") genesisAt = instantValue(clock());
    else if (clock !== undefined) genesisAt = instantValue(clock);
    else if (
      process.env.FGRAPH_CLOCK !== undefined &&
      /^-?[0-9]+$/u.test(process.env.FGRAPH_CLOCK)
    )
      genesisAt = instantValue(BigInt(process.env.FGRAPH_CLOCK));
    else genesisAt = wallClock();
    this.#initialize(genesisAt);
  }

  #initialize(genesisAt: bigint): void {
    this._connection.exec("BEGIN IMMEDIATE");
    try {
      // Another opener may have initialized the pristine file while this
      // connection waited for the writer lock. Recheck the complete format
      // boundary under that lock and accept only a valid winning initializer.
      if (this.#layoutState() === "initialized") {
        this._connection.exec("COMMIT");
        return;
      }
      this._connection.exec(SCHEMA_SQL);
      this._connection.pragma(`application_id = ${APPLICATION_ID}`);
      this._connection.pragma(`user_version = ${FORMAT_VERSION}`);
      const insertId = this._connection.prepare(
        "INSERT INTO fgraph_ids(id, name, gid, created_tx) VALUES (?, ?, NULL, ?)",
      );
      SYSTEM_NAMES.forEach((name, index) =>
        insertId.run(BigInt(index + 1), name, GENESIS_TX),
      );
      this._connection
        .prepare(
          "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,NULL,?,?)",
        )
        .run(GENESIS_TX, uuidBytes(GENESIS_EVENT), GENESIS_TX);
      const insertMeta = this._connection.prepare(
        "INSERT INTO fgraph_meta(key, value) VALUES (?, ?)",
      );
      insertMeta.run("next_id", FIRST_USER_ID);
      insertMeta.run("created_at", genesisAt);
      this.#insertRawFact(
        GENESIS_TX,
        1n,
        { tag: INSTANT, stored: genesisAt, logical: genesisAt },
        GENESIS_TX,
      );
      SYSTEM_TYPES.forEach((type, index) =>
        this.#insertRawFact(
          BigInt(index + 1),
          8n,
          { tag: TEXT, stored: type, logical: type },
          GENESIS_TX,
        ),
      );
      SYSTEM_DOCS.forEach((doc, index) =>
        this.#insertRawFact(
          BigInt(index + 1),
          10n,
          { tag: TEXT, stored: doc, logical: doc },
          GENESIS_TX,
        ),
      );
      for (const attribute of [16n, 17n])
        this.#insertRawFact(
          attribute,
          5n,
          { tag: BOOL, stored: 1n, logical: true },
          GENESIS_TX,
        );
      const genesisData = canonicalEventData(genesisEvent(genesisAt));
      this._connection
        .prepare(
          "INSERT INTO fgraph_events(tx,event_hash,event_data,operation_id,request_hash) VALUES (?,?,?,NULL,NULL)",
        )
        .run(GENESIS_TX, eventHash(genesisData), genesisData);
      this._connection.exec("COMMIT");
    } catch (error) {
      if (this._connection.inTransaction) this._connection.exec("ROLLBACK");
      throw error;
    }
  }

  #validateObjects(): void {
    const explicit = [
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
    ];
    const required = new Set(explicit);
    const reference = new Database(":memory:");
    reference.exec(SCHEMA_SQL);
    const expected = new Map(
      reference
        .prepare<unknown[], { name: string; type: string; sql: string | null }>(
          `SELECT name,type,sql FROM sqlite_schema WHERE name IN (${explicit.map(() => "?").join(",")})`,
        )
        .all(...explicit)
        .map((row) => [row.name, row]),
    );
    reference.close();
    const found = this._connection
      .prepare<[], { name: string; type: string; sql: string | null }>(
        "SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name",
      )
      .all();
    const ftsInternals = /^fgraph_fts_(config|content|data|docsize|idx)$/u;
    const normalize = (sql: string | null): string =>
      (sql ?? "").replaceAll(/\s+/gu, " ").trim().toLowerCase();
    for (const row of found) {
      const contract = expected.get(row.name);
      if (contract === undefined) {
        if (ftsInternals.test(row.name) && row.type === "table") continue;
        throw new FormatError(
          `file ${JSON.stringify(this.path)} contains non-format object ${JSON.stringify(row.name)}; fgraph v1 requires a dedicated SQLite file`,
        );
      }
      required.delete(row.name);
      if (
        row.type !== contract.type ||
        normalize(row.sql) !== normalize(contract.sql)
      )
        throw new FormatError(
          `file ${JSON.stringify(this.path)} has a modified ${row.type} ${row.name}; restore the canonical format-v2 layout`,
        );
    }
    if (required.size > 0)
      throw new FormatError(
        `file ${JSON.stringify(this.path)} is missing format-v2 objects ${[...required].sort().join(", ")}; restore from a valid backup or snapshot`,
      );
  }

  #ensureOpen(): void {
    if (this.#closed || !this._connection.open)
      throw new FormatError(
        "fgraph connection is closed; call connect() to open a new handle",
      );
    this.#refreshCache();
  }

  #ensureWritable(): void {
    this.#ensureOpen();
    if (this.#readOnly || this._asOf !== null)
      throw new ReadOnly(
        "this fgraph view is read-only; write through the live writable database connection",
      );
  }

  #statement<BindParameters extends unknown[], Result = unknown>(
    source: string,
  ): Database.Statement<BindParameters, Result> {
    const cached = this.#statements.get(source);
    if (cached !== undefined)
      return cached as unknown as Database.Statement<BindParameters, Result>;
    const statement = this._connection.prepare<BindParameters, Result>(source);
    this.#statements.set(
      source,
      statement as unknown as Database.Statement<unknown[], unknown>,
    );
    return statement;
  }

  #atomic<T>(operation: () => T): T {
    this.#ensureWritable();
    const nested = this._connection.inTransaction;
    const savepoint = nested ? `fgraph_write_${++this.#savepointCounter}` : "";
    try {
      this._connection.exec(
        nested ? `SAVEPOINT ${savepoint}` : "BEGIN IMMEDIATE",
      );
    } catch (error) {
      const message = error instanceof Error ? error.message.toLowerCase() : "";
      if (message.includes("locked") || message.includes("busy"))
        throw new Conflict(
          "database writer lock is busy; retry the transaction after the current writer commits",
        );
      throw new FormatError(
        "SQLite could not start an atomic write; run doctor or restore a valid backup",
      );
    }
    let committed = false;
    try {
      const result = operation();
      if (nested) {
        // Mutations publish exact name deltas. A failed savepoint invalidates
        // the cache below, so rescanning every prior identity here is redundant.
        this._connection.exec(`RELEASE ${savepoint}`);
      } else {
        this._connection.exec("COMMIT");
        committed = true;
      }
      return result;
    } catch (error) {
      if (!committed && this._connection.inTransaction) {
        if (nested)
          this._connection.exec(
            `ROLLBACK TO ${savepoint}; RELEASE ${savepoint}`,
          );
        else this._connection.exec("ROLLBACK");
      }
      this.#cacheVersion = -1n;
      throw error;
    }
  }

  #beginRead(): { basis: bigint; owned: boolean } {
    this.#ensureOpen();
    const owned = !this._connection.inTransaction;
    if (owned) this._connection.exec("BEGIN");
    try {
      // The basis read establishes the deferred SQLite snapshot before any
      // later statement can observe a different committed generation.
      const basis = this._asOf ?? this.#latestBasis();
      this.#refreshCache(true);
      return { basis, owned };
    } catch (error) {
      if (owned && this._connection.inTransaction)
        this._connection.exec("ROLLBACK");
      throw error;
    }
  }

  #finishRead(owned: boolean, commit: boolean): void {
    if (!owned || !this._connection.inTransaction) return;
    this._connection.exec(commit ? "COMMIT" : "ROLLBACK");
  }

  #refreshCache(force = false): void {
    const version = this._connection.pragma("data_version", {
      simple: true,
    }) as bigint;
    if (force || version !== this.#cacheVersion) {
      const rows = this._connection
        .prepare<[], IdRow>(
          "SELECT id,name,gid,created_tx FROM fgraph_ids WHERE name IS NOT NULL",
        )
        .all();
      this.#names = new Map(rows.map((row) => [row.name as string, row.id]));
      this.#idNames = new Map(rows.map((row) => [row.id, row.name as string]));
      this.#cacheVersion = version;
    }
  }

  #validateName(name: string): void {
    const bytes = Buffer.from(name);
    if (
      !name.isWellFormed() ||
      bytes.length === 0 ||
      bytes.length > 512 ||
      /\p{Cc}/u.test(name)
    ) {
      throw new TypeError(
        `invalid entity name ${JSON.stringify(name)}; use 1-512 UTF-8 bytes without control characters`,
      );
    }
    if (name.startsWith("fgraph/") && !this.#names.has(name))
      throw new SchemaError(
        `name ${JSON.stringify(name)} uses the reserved fgraph/ namespace; choose an application namespace`,
      );
  }

  #validateAttribute(attribute: string): void {
    if (!ATTRIBUTE_PATTERN.test(attribute))
      throw new SchemaError(
        `invalid attribute ${JSON.stringify(attribute)}; use exactly one slash, for example 'person/name'`,
      );
    if (attribute.startsWith("fgraph/") && !this.#names.has(attribute))
      throw new SchemaError(
        `attribute ${JSON.stringify(attribute)} uses the reserved fgraph/ namespace; choose an application namespace`,
      );
  }

  #nextAvailableId(): bigint {
    const row = this._connection
      .prepare<[], { value: unknown }>(
        "SELECT value FROM fgraph_meta WHERE key='next_id'",
      )
      .get();
    if (row === undefined || typeof row.value !== "bigint")
      throw new FormatError(
        "fgraph_meta.next_id is missing or non-integer; restore from a valid backup",
      );
    if (row.value < FIRST_USER_ID)
      throw new FormatError(
        `fgraph_meta.next_id is ${row.value}; restore a valid fgraph file with next_id at least ${FIRST_USER_ID}`,
      );
    return row.value;
  }

  #allocate(pending: Pending, name?: string): bigint {
    const result = this.#takeId(pending);
    pending.allocated.set(result, name ?? null);
    if (name !== undefined) {
      pending.names.set(name, result);
      pending.idNames.set(result, name);
    }
    return result;
  }

  #takeId(pending: Pending): bigint {
    if (pending.nextId >= INT64_MAX)
      throw new TooLarge(
        "signed 64-bit identifier space is exhausted; export into a fresh database before writing more facts",
      );
    return pending.nextId++;
  }

  #resolveNameWrite(name: string, pending: Pending, report = true): bigint {
    this.#validateName(name);
    const known = pending.names.get(name) ?? this.#names.get(name);
    if (known !== undefined) return known;
    const result = this.#allocate(pending, name);
    if (report && !name.includes("/")) pending.reportIds.set(name, result);
    return result;
  }

  #resolveAttributeWrite(attribute: unknown, pending: Pending): bigint {
    if (typeof attribute !== "string")
      throw new SchemaError(
        `attribute ${String(attribute)} is not a string; use a name such as 'person/name'`,
      );
    this.#validateAttribute(attribute);
    return this.#resolveNameWrite(attribute, pending, false);
  }

  _resolveRead(ref: unknown, missingOk = false): bigint | null {
    this.#ensureOpen();
    if (typeof ref === "boolean")
      throw new TypeError(
        `invalid entity reference ${String(ref)}; use an integer id, name, or unique lookup`,
      );
    if (typeof ref === "number" || typeof ref === "bigint") {
      const entity = asBigInt(ref, "entity reference");
      const horizon = this._asOf;
      const found = this._connection
        .prepare(
          horizon === null
            ? "SELECT 1 FROM fgraph_ids WHERE id=?"
            : "SELECT 1 FROM fgraph_ids WHERE id=? AND created_tx<=?",
        )
        .get(...(horizon === null ? [entity] : [entity, horizon]));
      if (found !== undefined) return entity;
    } else if (typeof ref === "string") {
      const row = this._connection
        .prepare<unknown[], { id: bigint }>(
          this._asOf === null
            ? "SELECT id FROM fgraph_ids WHERE name=?"
            : "SELECT id FROM fgraph_ids WHERE name=? AND created_tx<=?",
        )
        .get(...(this._asOf === null ? [ref] : [ref, this._asOf]));
      if (row !== undefined) return row.id;
    } else if (Array.isArray(ref) && ref.length === 2)
      return this.#lookupOwner(ref[0], ref[1], missingOk);
    else
      throw new TypeError(
        `invalid entity reference ${String(ref)}; use an integer id, name, or [unique-attribute, value] lookup`,
      );
    if (missingOk) return null;
    throw new NotFound(
      `entity ${String(ref)} was not found; transact it first or use a known name/id`,
    );
  }

  _attributeId(attribute: string): bigint | null {
    try {
      this.#validateAttribute(attribute);
    } catch {
      throw new QueryError(
        `invalid attribute ${JSON.stringify(attribute)}; use exactly one namespace/name slash`,
      );
    }
    const row = this._connection
      .prepare<unknown[], { id: bigint }>(
        this._asOf === null
          ? "SELECT id FROM fgraph_ids WHERE name=?"
          : "SELECT id FROM fgraph_ids WHERE name=? AND created_tx<=?",
      )
      .get(...(this._asOf === null ? [attribute] : [attribute, this._asOf]));
    return row?.id ?? null;
  }

  _isReadOnly(): boolean {
    return this.#readOnly || this._asOf !== null;
  }

  _basisTx(): number | bigint {
    return publicId(this._asOf ?? this.#latestBasis());
  }

  #lookupOwner(
    attribute: unknown,
    value: unknown,
    missingOk = false,
  ): bigint | null {
    if (typeof attribute !== "string")
      throw new SchemaError(
        `lookup attribute ${String(attribute)} is invalid; use an attribute name string`,
      );
    const attributeId = this._attributeId(attribute);
    if (attributeId === null) {
      if (missingOk) return null;
      throw new NotFound(
        `lookup attribute ${JSON.stringify(attribute)} was not found; declare and transact it before lookup`,
      );
    }
    const schema = this._schema(attributeId);
    if (!schema.unique)
      throw new SchemaError(
        `lookup attribute ${JSON.stringify(attribute)} is not unique; declare unique=true before using lookups`,
      );
    const encoded = this._encodeReadValue(value, schema);
    const visibility = this._visibility();
    const row = this._connection
      .prepare<unknown[], { e: bigint }>(
        `SELECT e FROM fgraph_facts WHERE a=? AND v=? AND t=? AND ${visibility.sql}`,
      )
      .get(attributeId, encoded.stored, encoded.tag, ...visibility.params);
    if (row !== undefined) return row.e;
    if (missingOk) return null;
    throw new NotFound(
      `lookup [${JSON.stringify(attribute)}, ${String(value)}] was not found; transact that unique value before referencing it`,
    );
  }

  #resolveRefWrite(ref: unknown, pending: Pending): bigint {
    if (typeof ref === "boolean")
      throw new TypeError(
        `invalid reference ${String(ref)}; use an entity id, name, or unique lookup`,
      );
    if (typeof ref === "number" || typeof ref === "bigint") {
      const entity = this._resolveRead(ref, true);
      if (entity === null)
        throw new NotFound(
          `referenced entity id ${String(ref)} was not found; transact the entity before linking it`,
        );
      return entity;
    }
    if (typeof ref === "string") return this.#resolveNameWrite(ref, pending);
    if (Array.isArray(ref) && ref.length === 2) {
      return this.#lookupOwner(ref[0], ref[1]) as bigint;
    }
    if (
      isRecord(ref) &&
      Object.keys(ref).length === 1 &&
      Object.hasOwn(ref, "tmp")
    )
      return this.#resolveEntityWriteSelector(ref, pending);
    throw new TypeError(
      `invalid reference ${String(ref)}; use an entity id, name, or unique lookup`,
    );
  }

  _nameOrId(entity: bigint): string | number | bigint {
    return this.#idNames.get(entity) ?? publicId(entity);
  }

  #ensureUserTarget(entity: bigint): bigint {
    const isTransaction = this.#statement<[bigint]>(
      "SELECT 1 FROM fgraph_facts WHERE e=? AND a=1 AND tx=e LIMIT 1",
    ).get(entity);
    if (entity <= GENESIS_TX || isTransaction !== undefined)
      throw new Unsupported(
        `system/transaction entity ${String(this._nameOrId(entity))} cannot be changed; attach transaction facts in the creating transact({tx:...}) call`,
      );
    return entity;
  }

  _visibility(
    asOf: bigint | null = this._asOf,
    alias = "",
  ): { sql: string; params: bigint[] } {
    const prefix = alias === "" ? "" : `${alias}.`;
    return asOf === null
      ? { sql: `${prefix}rx IS NULL`, params: [] }
      : {
          sql: `${prefix}tx <= ? AND (${prefix}rx IS NULL OR ${prefix}rx > ?)`,
          params: [asOf, asOf],
        };
  }

  _schema(attribute: bigint, asOf: bigint | null = this._asOf): Schema {
    const schema = blankSchema();
    const visibility = this._visibility(asOf);
    const rows = this._connection
      .prepare<[bigint, ...bigint[]], { a: bigint; v: Stored; t: bigint }>(
        `SELECT a, v, t FROM fgraph_facts WHERE e=? AND (a BETWEEN 5 AND 10 OR a=14) AND ${visibility.sql} ORDER BY tx`,
      )
      .all(attribute, ...visibility.params);
    for (const row of rows)
      this.#updateSchema(
        schema,
        Number(row.a),
        this._logical(Number(row.t), row.v),
      );
    return schema;
  }

  #schemaWithPending(attribute: bigint, operations: Operation[]): Schema {
    let schema = this._schema(attribute, null);
    for (const operation of operations) {
      if (operation.e !== attribute) continue;
      if (operation.kind === "assert")
        this.#updateSchema(
          schema,
          Number(operation.a),
          operation.value.logical,
        );
      else if (operation.a === null) schema = blankSchema();
      else if (
        (operation.a >= 5n && operation.a <= 10n) ||
        operation.a === 14n
      ) {
        const current = this.#schemaValue(schema, Number(operation.a));
        if (
          operation.value === null ||
          this.#logicalEqual(current, operation.value.logical)
        )
          this.#clearSchema(schema, Number(operation.a));
      }
    }
    return schema;
  }

  #schemaValue(schema: Schema, attribute: number): unknown {
    if (attribute === 5) return schema.many;
    if (attribute === 6) return schema.unique;
    if (attribute === 7) return schema.nohistory;
    if (attribute === 8) return schema.type;
    if (attribute === 9) return schema.dims;
    if (attribute === 10) return schema.doc;
    if (attribute === 14) return schema.vectorModel;
    return null;
  }

  #updateSchema(schema: Schema, attribute: number, logical: unknown): void {
    if (attribute === 5) schema.many = Boolean(logical);
    else if (attribute === 6) schema.unique = Boolean(logical);
    else if (attribute === 7) schema.nohistory = Boolean(logical);
    else if (attribute === 8) schema.type = String(logical);
    else if (attribute === 9) schema.dims = Number(logical);
    else if (attribute === 10) schema.doc = String(logical);
    else if (attribute === 14) schema.vectorModel = String(logical);
  }

  #clearSchema(schema: Schema, attribute: number): void {
    if (attribute === 5) schema.many = false;
    else if (attribute === 6) schema.unique = false;
    else if (attribute === 7) schema.nohistory = null;
    else if (attribute === 8) schema.type = null;
    else if (attribute === 9) schema.dims = null;
    else if (attribute === 10) schema.doc = null;
    else if (attribute === 14) schema.vectorModel = null;
  }

  #logicalEqual(left: unknown, right: unknown): boolean {
    return left === right;
  }

  _encodeReadValue(value: unknown, schema?: Schema): Encoded {
    const encoded = encode(value, (ref) => {
      return this._resolveRead(ref) as bigint;
    });
    if (schema !== undefined && !valueMatches(schema.type, encoded)) {
      throw new TypeError(
        `value ${String(value)} has type ${typeName(encoded.tag)}, but the attribute requires ${String(schema.type)}; write a ${String(schema.type)} value or change the declaration`,
      );
    }
    return encoded;
  }

  #encodeWriteValue(
    value: unknown,
    attribute: bigint,
    pending: Pending,
  ): Encoded {
    const schema = this.#pendingSchema(attribute, pending);
    const encoded = encode(value, (ref) => this.#resolveRefWrite(ref, pending));
    if (!valueMatches(schema.type, encoded))
      throw new TypeError(
        `attribute ${String(this._nameOrId(attribute))} requires ${String(schema.type)}, but the value is ${typeName(encoded.tag)}; write the declared type or change its declaration`,
      );
    if (encoded.tag === VECTOR) {
      const dimensions = (encoded.logical as number[]).length;
      const fixed = schema.dims ?? this.#inferredVectorDims(attribute);
      if (fixed !== null && fixed !== dimensions)
        throw new TypeError(
          `attribute ${String(this._nameOrId(attribute))} requires vectors with ${fixed} dimensions, got ${dimensions}; write a matching vector or declare the intended dims before the first value`,
        );
    }
    return encoded;
  }

  #inferredVectorDims(attribute: bigint): number | null {
    const row = this._connection
      .prepare<[bigint, number], { v: Stored }>(
        "SELECT v FROM fgraph_facts WHERE a=? AND t=? ORDER BY id LIMIT 1",
      )
      .get(attribute, VECTOR);
    return row === undefined
      ? null
      : (this._logical(VECTOR, row.v) as number[]).length;
  }

  _vectorDims(attribute: bigint): number | null {
    return this.#inferredVectorDims(attribute);
  }

  _logical(
    tag: number,
    stored: Stored,
    indirectData: unknown | typeof OMITTED = OMITTED,
  ): unknown {
    if (!Number.isInteger(tag) || tag < REF || tag > JSON_TAG)
      throw new FormatError(
        `a fact has unknown physical tag ${tag}; run doctor and restore a valid backup`,
      );
    if (tag === BOOL) {
      if (typeof stored !== "bigint" || (stored !== 0n && stored !== 1n))
        throw new FormatError(
          "a bool fact is not physical integer 0 or 1; run doctor and restore a valid backup",
        );
      return stored !== 0n;
    }
    if (tag === REF) {
      if (typeof stored !== "bigint" || stored <= 0n)
        throw new FormatError(
          "a ref fact is not a positive physical integer; run doctor and restore a valid backup",
        );
      return stored;
    }
    if (tag === INT) {
      if (typeof stored !== "bigint")
        throw new FormatError(
          "an int fact is not a physical integer; run doctor and restore a valid backup",
        );
      return stored;
    }
    if (tag === FLOAT) {
      if (typeof stored !== "number" || !Number.isFinite(stored))
        throw new FormatError(
          "a float fact is not a finite physical real; run doctor and restore a valid backup",
        );
      return stored;
    }
    if (tag === TEXT) {
      if (
        typeof stored !== "string" ||
        !stored.isWellFormed() ||
        Buffer.byteLength(stored, "utf8") > BLOB_THRESHOLD
      )
        throw new FormatError(
          "an inline text fact has an invalid physical value; run doctor and restore a valid backup",
        );
      return stored;
    }
    if (tag === INSTANT) {
      if (
        typeof stored !== "bigint" ||
        stored < INSTANT_MIN ||
        stored > INSTANT_MAX
      )
        throw new FormatError(
          "an instant fact is outside its physical domain; run doctor and restore a valid backup",
        );
      return stored;
    }
    if (tag === BYTES) {
      if (!Buffer.isBuffer(stored) || stored.length > BLOB_THRESHOLD)
        throw new FormatError(
          "an inline bytes fact has an invalid physical value; run doctor and restore a valid backup",
        );
      return stored;
    }
    if (tag === VECTOR || tag === TEXT_REF || tag === BYTES_REF) {
      if (!Buffer.isBuffer(stored) || stored.length !== 32)
        throw new FormatError(
          "an indirect fact key is not a 32-byte hash; restore a valid backup",
        );
      const data =
        indirectData === OMITTED
          ? this.#statement<[Buffer], { data: unknown }>(
              "SELECT data FROM fgraph_blobs WHERE hash=?",
            ).get(stored)?.data
          : indirectData;
      if (data === undefined || data === null)
        throw new FormatError(
          "an indirect fact references a missing blob; run doctor or restore a valid backup",
        );
      const problem = indirectDataProblem(tag, data);
      if (problem !== null) throw new FormatError(problem);
      if (!indirectDigest(tag, indirectBytes(tag, data)).equals(stored))
        throw new FormatError(
          "an indirect blob does not match its content-addressed hash; run doctor and restore a valid backup",
        );
      if (tag === VECTOR) {
        const packed = data as Buffer;
        const values: number[] = [];
        for (let index = 0; index < packed.length; index += 4)
          values.push(packed.readFloatLE(index));
        return values;
      }
      return data;
    }
    if (tag === JSON_TAG) {
      if (typeof stored !== "string")
        throw new FormatError(
          "a JSON fact is not physical text; run doctor and restore a valid backup",
        );
      try {
        if (
          Buffer.byteLength(stored, "utf8") > MAX_VALUE_BYTES ||
          canonicalValueJson(parseJsonValue(stored, "stored JSON")) !== stored
        )
          throw new Error("non-canonical");
      } catch {
        throw new FormatError(
          "a JSON fact is not canonical JSON; run doctor and restore a valid backup",
        );
      }
    }
    return stored;
  }

  _cell(tag: number, stored: Stored): Cell {
    const logical = this._logical(tag, stored);
    if (tag === JSON_TAG) return { tag, value: String(logical) };
    if (tag === VECTOR) return { tag, value: logical as number[] };
    return { tag, value: logical };
  }

  _wire(tag: number, stored: Stored): unknown {
    return wireValue(tag, this._logical(tag, stored), (id) =>
      this._nameOrId(id),
    );
  }

  _renderRow(
    row: RawRow,
    rxOverride: bigint | null | undefined = undefined,
    localNames?: Map<bigint, string>,
    logical: unknown | typeof OMITTED = OMITTED,
  ): RenderedFact {
    const renderId = (entity: bigint): string | number | bigint =>
      localNames?.get(entity) ?? this._nameOrId(entity);
    const rx = rxOverride === undefined ? row.rx : rxOverride;
    return {
      id: publicId(row.id),
      e: renderId(row.e),
      a: String(renderId(row.a)),
      v: wireValue(
        Number(row.t),
        logical === OMITTED ? this._logical(Number(row.t), row.v) : logical,
        renderId,
      ),
      tx: publicId(row.tx),
      rx: rx === null ? null : publicId(rx),
    };
  }

  _renderViewRow(row: RawRow): RenderedFact {
    return this._asOf !== null && row.rx !== null && row.rx > this._asOf
      ? this._renderRow(row, null)
      : this._renderRow(row);
  }

  #insertRawFact(
    entity: bigint,
    attribute: bigint,
    value: Encoded,
    transaction: bigint,
  ): RawRow {
    if (value.blob !== undefined)
      this.#statement<[unknown, unknown]>(
        "INSERT OR IGNORE INTO fgraph_blobs(hash, data) VALUES (?, ?)",
      ).run(value.stored, value.blob);
    const result = this.#statement<[bigint, bigint, unknown, number, bigint]>(
      "INSERT INTO fgraph_facts(e, a, v, t, tx, rx) VALUES (?, ?, ?, ?, ?, NULL)",
    ).run(entity, attribute, value.stored, value.tag, transaction);
    const factId = asBigInt(result.lastInsertRowid);
    if (value.tag === TEXT || value.tag === TEXT_REF) {
      this.#statement<[bigint, unknown]>(
        "INSERT INTO fgraph_fts(rowid, text) VALUES (?, ?)",
      ).run(factId, value.logical);
    }
    return this.#statement<[bigint], RawRow>(
      "SELECT * FROM fgraph_facts WHERE id=?",
    ).get(factId) as RawRow;
  }

  #deleteOrRetract(row: RawRow, transaction: bigint): RenderedFact {
    const schema = this._schema(row.a, null);
    const rendered = this._renderRow(row, transaction);
    this._connection
      .prepare("DELETE FROM fgraph_fts WHERE rowid=?")
      .run(row.id);
    if (deletesHistory(schema))
      this._connection
        .prepare("DELETE FROM fgraph_facts WHERE id=?")
        .run(row.id);
    else
      this._connection
        .prepare("UPDATE fgraph_facts SET rx=? WHERE id=?")
        .run(transaction, row.id);
    return rendered;
  }

  #gcBlobs(candidates?: RawRow[]): number {
    if (candidates === undefined)
      return this._connection
        .prepare(
          "DELETE FROM fgraph_blobs WHERE hash NOT IN (SELECT v FROM fgraph_facts WHERE t IN (7,8,9))",
        )
        .run().changes;
    const remove = this._connection.prepare(
      "DELETE FROM fgraph_blobs WHERE hash=? AND NOT EXISTS " +
        "(SELECT 1 FROM fgraph_facts WHERE t IN (7,8,9) AND v=?)",
    );
    const seen = new Set<string>();
    let removed = 0;
    for (const row of candidates) {
      if (![VECTOR, TEXT_REF, BYTES_REF].includes(Number(row.t))) continue;
      const hash = row.v as Buffer;
      const key = hash.toString("hex");
      if (seen.has(key)) continue;
      seen.add(key);
      removed += remove.run(hash, hash).changes;
    }
    return removed;
  }

  #newPending(): Pending {
    return {
      operations: [],
      casTargets: new Set(),
      casOperations: new Set(),
      txFacts: [],
      reportIds: new Map(),
      nextId: this.#nextAvailableId(),
      names: new Map(),
      idNames: new Map(),
      allocated: new Map(),
      schemas: new Map(),
    };
  }

  #pendingSchema(attribute: bigint, pending: Pending): Schema {
    let schema = pending.schemas.get(attribute);
    if (schema === undefined) {
      schema = this.#schemaWithPending(attribute, pending.operations);
      pending.schemas.set(attribute, schema);
    }
    return schema;
  }

  #appendPending(pending: Pending, operation: Operation): void {
    pending.operations.push(operation);
    if (
      operation.a === null ||
      (operation.a >= 5n && operation.a <= 10n) ||
      operation.a === 14n
    )
      pending.schemas.delete(operation.e);
  }

  #uniqueOwnersIncludingPending(
    attribute: bigint,
    value: Encoded,
    pending: Pending,
  ): Set<bigint> {
    const owners = new Set<bigint>();
    for (const operation of pending.operations) {
      if (
        operation.kind === "assert" &&
        operation.a === attribute &&
        operation.value.tag === value.tag &&
        equalStored(operation.value.stored, value.stored)
      )
        owners.add(operation.e);
    }
    const row = this._connection
      .prepare<[bigint, Stored, number], { e: bigint }>(
        "SELECT e FROM fgraph_facts WHERE a=? AND v=? AND t=? AND rx IS NULL",
      )
      .get(attribute, value.stored, value.tag);
    if (row !== undefined) owners.add(row.e);
    return owners;
  }

  #upsertOwnerForMap(
    data: Record<string, unknown>,
    pending: Pending,
  ): bigint | null {
    const owners = new Set<bigint>();
    for (const [attribute, raw] of recordEntries(data)) {
      if (attribute === "id") continue;
      const attributeId =
        pending.names.get(attribute) ?? this.#names.get(attribute);
      if (attributeId === undefined) continue;
      const schema = this.#pendingSchema(attributeId, pending);
      if (!schema.unique) continue;
      const values = Array.isArray(raw) ? raw : [raw];
      for (const value of values) {
        try {
          const encoded = this.#encodeWriteValue(value, attributeId, pending);
          for (const owner of this.#uniqueOwnersIncludingPending(
            attributeId,
            encoded,
            pending,
          ))
            owners.add(owner);
        } catch (error) {
          if (!(error instanceof NotFound || error instanceof TypeError))
            throw error;
        }
      }
    }
    if (owners.size > 1)
      throw new Conflict(
        `unique attributes in one map resolve to different entities; split or correct the input`,
      );
    return owners.values().next().value ?? null;
  }

  #mapEntity(data: Record<string, unknown>, pending: Pending): bigint {
    const selector = Object.hasOwn(data, "id") ? data.id : OMITTED;
    const owner = this.#upsertOwnerForMap(data, pending);
    if (selector === OMITTED) return owner ?? this.#allocate(pending);
    if (
      isRecord(selector) &&
      Object.keys(selector).length === 1 &&
      Object.hasOwn(selector, "tmp")
    ) {
      const temp = selector.tmp;
      if (typeof temp !== "string" || temp.length === 0)
        throw new TypeError(
          `invalid tempid ${String(temp)}; use a non-empty string such as {"tmp":"t1"}`,
        );
      const known = pending.reportIds.get(temp);
      if (known !== undefined) {
        if (owner !== null && owner !== known)
          throw new Conflict(
            `tempid ${JSON.stringify(temp)} conflicts with a unique owner; use one identity`,
          );
        return known;
      }
      const entity = owner ?? this.#allocate(pending);
      pending.reportIds.set(temp, entity);
      return entity;
    }
    let entity: bigint;
    if (typeof selector === "string")
      entity = this.#resolveNameWrite(selector, pending);
    else if (typeof selector === "number" || typeof selector === "bigint") {
      entity = this._resolveRead(selector) as bigint;
    } else if (Array.isArray(selector) && selector.length === 2) {
      entity = this.#lookupOwner(selector[0], selector[1]) as bigint;
    } else
      throw new TypeError(
        `invalid map id ${String(selector)}; use a name, integer id, unique lookup, or {"tmp":"name"}`,
      );
    if (owner !== null && owner !== entity)
      throw new Conflict(
        `map id pins a different entity from its unique value; use that owner or change the unique value`,
      );
    return this.#ensureUserTarget(entity);
  }

  #appendVectorDims(
    attribute: bigint,
    value: Encoded,
    pending: Pending,
    autoDims: Map<bigint, number>,
  ): void {
    if (value.tag !== VECTOR) return;
    let schema = this.#pendingSchema(attribute, pending);
    if (schema.type === null) {
      this.#appendPending(pending, {
        kind: "assert",
        e: attribute,
        a: 8n,
        value: { tag: TEXT, stored: "vector", logical: "vector" },
      });
      schema = this.#pendingSchema(attribute, pending);
    }
    if (schema.dims !== null) return;
    const dimensions = (value.logical as number[]).length;
    const fixed =
      autoDims.get(attribute) ?? this.#inferredVectorDims(attribute);
    if (fixed !== null) {
      if (fixed !== dimensions)
        throw new TypeError(
          `attribute ${String(this._nameOrId(attribute))} requires vectors with ${fixed} dimensions, got ${dimensions}; write a matching vector`,
        );
      return;
    }
    autoDims.set(attribute, dimensions);
    this.#appendPending(pending, {
      kind: "assert",
      e: attribute,
      a: 9n,
      value: {
        tag: INT,
        stored: BigInt(dimensions),
        logical: BigInt(dimensions),
      },
    });
  }

  #isTypedWrapper(value: Record<string, unknown>): boolean {
    const keys = Object.keys(value);
    return (
      keys.length === 1 &&
      ["ref", "instant", "bytes", "vector", "json"].includes(keys[0] as string)
    );
  }

  #parseMap(
    data: Record<string, unknown>,
    pending: Pending,
    autoDims: Map<bigint, number>,
  ): void {
    const keys = Object.keys(data);
    if (keys.length === 0) return;
    if (keys.length === 1 && keys[0] === "id") {
      const selector = data.id;
      if (typeof selector === "string")
        this.#resolveNameWrite(selector, pending);
      else if (typeof selector === "number" || typeof selector === "bigint")
        this._resolveRead(selector);
      else if (Array.isArray(selector) && selector.length === 2)
        this.#lookupOwner(selector[0], selector[1]);
      else if (
        isRecord(selector) &&
        Object.keys(selector).length === 1 &&
        Object.hasOwn(selector, "tmp")
      ) {
        const temp = selector.tmp;
        if (typeof temp !== "string" || temp.length === 0)
          throw new TypeError(
            "identity-only tempid must be a non-empty string",
          );
        if (!pending.reportIds.has(temp)) {
          const entity = this.#allocate(pending);
          pending.reportIds.set(temp, entity);
        }
      } else
        throw new TypeError(
          `invalid map id ${String(selector)}; use a name, integer id, unique lookup, or tempid`,
        );
      return;
    }
    const entity = this.#mapEntity(data, pending);
    this.#parseMapForEntity(data, entity, pending, autoDims);
  }

  #parseMapForEntity(
    data: Record<string, unknown>,
    entity: bigint,
    pending: Pending,
    autoDims: Map<bigint, number>,
  ): void {
    for (const [attribute, raw] of recordEntries(data)) {
      if (attribute === "id") continue;
      const attributeId = this.#resolveAttributeWrite(attribute, pending);
      const schema = this.#pendingSchema(attributeId, pending);
      if (Array.isArray(raw) && !schema.many)
        throw new Conflict(
          `attribute ${JSON.stringify(attribute)} holds one value per entity; declare it many=true to assert an array, or wrap a literal array with {"json":[...]}`,
        );
      const values = Array.isArray(raw) ? raw : [raw];
      for (const item of values) {
        let value: Encoded;
        if (isRecord(item) && !this.#isTypedWrapper(item)) {
          if (schema.type !== "ref")
            throw new TypeError(
              `nested map on ${JSON.stringify(attribute)} requires type='ref'; declare ref=true or wrap domain JSON with {"json":...}`,
            );
          const child = this.#mapEntity(item, pending);
          this.#parseMapForEntity(item, child, pending, autoDims);
          value = { tag: REF, stored: child, logical: child };
        } else value = this.#encodeWriteValue(item, attributeId, pending);
        this.#appendVectorDims(attributeId, value, pending, autoDims);
        this.#appendPending(pending, {
          kind: "assert",
          e: entity,
          a: attributeId,
          value,
        });
      }
    }
  }

  #resolveEntityWriteSelector(selector: unknown, pending: Pending): bigint {
    let entity: bigint;
    if (typeof selector === "string")
      entity = this.#resolveNameWrite(selector, pending);
    else if (typeof selector === "number" || typeof selector === "bigint") {
      entity = this._resolveRead(selector) as bigint;
    } else if (
      isRecord(selector) &&
      Object.keys(selector).length === 1 &&
      Object.hasOwn(selector, "tmp")
    ) {
      const temp = selector.tmp;
      if (typeof temp !== "string" || temp.length === 0)
        throw new TypeError(
          `invalid tempid ${String(temp)}; use a non-empty string`,
        );
      entity = pending.reportIds.get(temp) ?? this.#allocate(pending);
      pending.reportIds.set(temp, entity);
    } else if (Array.isArray(selector) && selector.length === 2) {
      entity = this.#lookupOwner(selector[0], selector[1]) as bigint;
    } else
      throw new TypeError(
        `invalid entity selector ${String(selector)}; use a name, id, lookup, or tempid`,
      );
    return this.#ensureUserTarget(entity);
  }

  #parseOp(
    operation: unknown[],
    pending: Pending,
    autoDims: Map<bigint, number>,
  ): void {
    if (
      operation.length === 0 ||
      (operation[0] !== "assert" &&
        operation[0] !== "retract" &&
        operation[0] !== "cas")
    )
      throw new TypeError(
        `invalid operation; use ["assert",...], ["retract",...], or ["cas",entity,attribute,expected,desired]`,
      );
    if (operation[0] === "cas") {
      if (operation.length !== 5)
        throw new TypeError(
          `cas operation has ${operation.length - 1} arguments; use entity, attribute, expected, and desired`,
        );
      const entity = this.#ensureUserTarget(
        this._resolveRead(operation[1] as EntityRef) as bigint,
      );
      if (typeof operation[2] !== "string")
        throw new TypeError("CAS attribute must be an existing attribute name");
      const attribute = this._attributeId(operation[2]);
      if (attribute === null)
        throw new NotFound(
          `CAS attribute ${JSON.stringify(operation[2])} was not found`,
        );
      const schema = this.#pendingSchema(attribute, pending);
      if (schema.many)
        throw new SchemaError(
          `CAS requires cardinality-one, but ${String(this._nameOrId(attribute))} is many`,
        );
      const target = `${entity}/${attribute}`;
      if (
        pending.casTargets.has(target) ||
        pending.operations.some(
          (candidate) =>
            candidate.e === entity &&
            (candidate.a === attribute || candidate.a === null),
        )
      )
        throw new Conflict(
          `CAS target ${String(this._nameOrId(entity))}/${String(this._nameOrId(attribute))} is touched twice in one transaction`,
        );
      pending.casTargets.add(target);
      const missing = (value: unknown): boolean => {
        if (
          !isRecord(value) ||
          Object.keys(value).length !== 1 ||
          !Object.hasOwn(value, "missing")
        )
          return false;
        if (value.missing !== true)
          throw new TypeError('CAS missing sentinel must be {"missing":true}');
        return true;
      };
      const expected = missing(operation[3])
        ? null
        : this._encodeReadValue(operation[3], schema);
      const desired = missing(operation[4])
        ? null
        : this.#encodeWriteValue(operation[4], attribute, pending);
      if (desired !== null)
        this.#appendVectorDims(attribute, desired, pending, autoDims);
      const current = this._connection
        .prepare<[bigint, bigint], RawRow>(
          "SELECT * FROM fgraph_facts WHERE e=? AND a=? AND rx IS NULL ORDER BY id",
        )
        .all(entity, attribute);
      if (current.length > 1)
        throw new FormatError(
          "cardinality-one CAS found multiple current values; run doctor and restore a valid backup",
        );
      const actual = current[0];
      const matches =
        expected === null
          ? actual === undefined
          : actual !== undefined &&
            Number(actual.t) === expected.tag &&
            equalStored(actual.v, expected.stored);
      if (!matches)
        throw new Conflict(
          `CAS expected value does not match the current ${String(this._nameOrId(attribute))} fact`,
        );
      if (actual !== undefined) {
        const retraction: Retraction = {
          kind: "retract",
          e: entity,
          a: attribute,
          value: {
            tag: Number(actual.t),
            stored: actual.v,
            logical: this._logical(Number(actual.t), actual.v),
          },
        };
        this.#appendPending(pending, retraction);
        pending.casOperations.add(retraction);
      }
      if (desired !== null) {
        const assertion: Assertion = {
          kind: "assert",
          e: entity,
          a: attribute,
          value: desired,
        };
        this.#appendPending(pending, assertion);
        pending.casOperations.add(assertion);
      }
      return;
    }
    if (operation[0] === "assert") {
      if (operation.length !== 4)
        throw new TypeError(
          `assert operation has ${operation.length - 1} arguments; use ["assert", entity, attribute, value]`,
        );
      const entity = this.#resolveEntityWriteSelector(operation[1], pending);
      const attribute = this.#resolveAttributeWrite(operation[2], pending);
      const value = this.#encodeWriteValue(operation[3], attribute, pending);
      this.#appendVectorDims(attribute, value, pending, autoDims);
      this.#appendPending(pending, {
        kind: "assert",
        e: entity,
        a: attribute,
        value,
      });
      return;
    }
    if (operation.length < 2 || operation.length > 4)
      throw new TypeError(
        `retract operation has ${operation.length - 1} arguments; use entity, optional attribute, optional value`,
      );
    const selector = operation[1];
    if (
      typeof selector === "string" &&
      !this.#names.has(selector) &&
      !pending.names.has(selector)
    )
      return;
    const entity = this.#resolveEntityWriteSelector(selector, pending);
    let attribute: bigint | null = null;
    let value: Encoded | null = null;
    if (operation.length >= 3) {
      if (typeof operation[2] !== "string")
        throw new SchemaError(
          `retract attribute ${String(operation[2])} is invalid; use an attribute name`,
        );
      this.#validateAttribute(operation[2]);
      attribute =
        pending.names.get(operation[2]) ??
        this.#names.get(operation[2]) ??
        null;
      if (attribute === null) return;
    }
    if (operation.length === 4)
      value = this._encodeReadValue(
        operation[3],
        this.#pendingSchema(attribute as bigint, pending),
      );
    this.#appendPending(pending, {
      kind: "retract",
      e: entity,
      a: attribute,
      value,
    });
  }

  #parseData(data: unknown, pending: Pending): void {
    const autoDims = new Map<bigint, number>();
    if (isRecord(data)) {
      this.#parseMap(data, pending, autoDims);
      return;
    }
    if (Array.isArray(data)) {
      if (
        typeof data[0] === "string" &&
        (data[0] === "assert" || data[0] === "retract" || data[0] === "cas")
      ) {
        this.#parseOp(data, pending, autoDims);
        return;
      }
      for (const item of data) {
        if (isRecord(item)) this.#parseMap(item, pending, autoDims);
        else if (Array.isArray(item)) this.#parseOp(item, pending, autoDims);
        else
          throw new TypeError(
            `transaction item ${String(item)} is invalid; mix entity maps and assert/retract/cas operations`,
          );
      }
      return;
    }
    throw new TypeError(
      `transaction ${String(data)} is invalid; use one map, one operation, or an array of them`,
    );
  }

  #validateCASIsolation(pending: Pending): void {
    for (const operation of pending.operations) {
      if (pending.casOperations.has(operation)) continue;
      for (const target of pending.casTargets) {
        const [entityText, attributeText] = target.split("/");
        const entity = BigInt(entityText as string);
        const attribute = BigInt(attributeText as string);
        if (
          operation.e === entity &&
          (operation.a === attribute || operation.a === null)
        )
          throw new Conflict(
            "CAS target cannot be changed by another operation in the same transaction",
          );
      }
    }
  }

  #parseTxFacts(
    txData: Record<string, unknown> | undefined,
    pending: Pending,
    extra: Array<[string, unknown]> = [],
  ): void {
    const autoDims = new Map<bigint, number>();
    const append = (attribute: string, raw: unknown): void => {
      if (
        SYSTEM_NAMES.slice(0, 4).includes(
          attribute as (typeof SYSTEM_NAMES)[number],
        )
      )
        throw new SchemaError(
          `transaction tx map cannot set ${JSON.stringify(attribute)}; use the automatic receipt or top-level by/source/meta option`,
        );
      const attributeId = this.#resolveAttributeWrite(attribute, pending);
      const schema = this.#pendingSchema(attributeId, pending);
      let value: Encoded;
      if (isRecord(raw) && !this.#isTypedWrapper(raw)) {
        if (schema.type !== "ref")
          throw new TypeError(
            `nested transaction map on ${JSON.stringify(attribute)} requires type='ref'; declare ref=true before nesting`,
          );
        const child = this.#mapEntity(raw, pending);
        this.#parseMapForEntity(raw, child, pending, autoDims);
        value = { tag: REF, stored: child, logical: child };
      } else value = this.#encodeWriteValue(raw, attributeId, pending);
      this.#appendVectorDims(attributeId, value, pending, autoDims);
      const existing = pending.txFacts.filter(
        ([candidate]) => candidate === attributeId,
      );
      if (
        existing.some(
          ([, candidate]) =>
            candidate.tag === value.tag &&
            equalStored(candidate.stored, value.stored),
        )
      )
        return;
      if (existing.length > 0 && !schema.many)
        throw new Conflict(
          `transaction attribute ${JSON.stringify(attribute)} holds one value; provide one value or declare it many=true`,
        );
      if (
        schema.unique &&
        this.#uniqueOwnersIncludingPending(attributeId, value, pending).size > 0
      )
        throw new Conflict(
          `unique transaction fact ${JSON.stringify(attribute)} already belongs to another entity`,
        );
      pending.txFacts.push([attributeId, value]);
      this.#appendPending(pending, {
        kind: "assert",
        e: 0n,
        a: attributeId,
        value,
      });
    };
    if (txData !== undefined) {
      for (const [attribute, raw] of recordEntries(txData)) {
        if (attribute === "id")
          throw new SchemaError(
            "transaction metadata cannot set id; the transactor allocates the transaction entity last",
          );
        if (Array.isArray(raw)) {
          const attributeId = this.#resolveAttributeWrite(attribute, pending);
          if (!this.#pendingSchema(attributeId, pending).many)
            throw new Conflict(
              `transaction attribute ${JSON.stringify(attribute)} holds one value; declare it many=true for an array`,
            );
          raw.forEach((value) => append(attribute, value));
        } else append(attribute, raw);
      }
    }
    extra.forEach(([attribute, value]) => append(attribute, value));
  }

  #rowsForPlan(operations: Operation[]): Map<string, RawRow> {
    const rows = new Map<string, RawRow>();
    const schemas = new Map<bigint, Schema>();
    const finalSchema = (attribute: bigint): Schema => {
      let schema = schemas.get(attribute);
      if (schema === undefined) {
        schema = this.#schemaWithPending(attribute, operations);
        schemas.set(attribute, schema);
      }
      return schema;
    };
    const add = (candidates: RawRow[]): void =>
      candidates.forEach((row) => rows.set(rowKey(row), row));
    const entities = [
      ...new Set(operations.map((operation) => operation.e)),
    ].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));
    for (let offset = 0; offset < entities.length; offset += 400) {
      const chunk = entities.slice(offset, offset + 400);
      add(
        this._connection
          .prepare<bigint[], RawRow>(
            `SELECT * FROM fgraph_facts WHERE rx IS NULL AND e IN (${chunk.map(() => "?").join(",")}) ORDER BY id`,
          )
          .all(...chunk),
      );
    }
    for (const operation of operations) {
      if (operation.kind === "assert") {
        if (finalSchema(operation.a).unique)
          add(
            this._connection
              .prepare<[bigint, Stored, number], RawRow>(
                "SELECT * FROM fgraph_facts WHERE a=? AND v=? AND t=? AND rx IS NULL ORDER BY id",
              )
              .all(operation.a, operation.value.stored, operation.value.tag),
          );
        continue;
      }
      if (operation.a === null)
        add(
          this._connection
            .prepare<[bigint, bigint], RawRow>(
              "SELECT f.* FROM fgraph_facts f WHERE f.rx IS NULL AND f.t=0 AND f.v=? AND f.e>? " +
                "AND NOT EXISTS (SELECT 1 FROM fgraph_facts receipt WHERE receipt.e=f.e AND receipt.a=1 AND receipt.tx=receipt.e AND receipt.rx IS NULL) ORDER BY f.id",
            )
            .all(operation.e, GENESIS_TX),
        );
    }
    return rows;
  }

  #planDiff(operations: Operation[]): {
    assertions: Assertion[];
    retractions: RawRow[];
  } {
    const schemas = new Map<bigint, Schema>();
    const finalSchema = (attribute: bigint): Schema => {
      let schema = schemas.get(attribute);
      if (schema === undefined) {
        schema = this.#schemaWithPending(attribute, operations);
        schemas.set(attribute, schema);
      }
      return schema;
    };
    const working = new Map<string, RawRow | Assertion>(
      this.#rowsForPlan(operations),
    );
    const inserted: Assertion[] = [];
    let retracted: RawRow[] = [];
    const cancelled = new Set<Assertion>();
    for (const operation of operations) {
      if (operation.kind === "retract") {
        const matches: Array<[string, RawRow | Assertion]> = [];
        for (const [key, fact] of working) {
          const own = fact.e === operation.e;
          const tag = isAssertion(fact) ? fact.value.tag : Number(fact.t);
          const stored = isAssertion(fact) ? fact.value.stored : fact.v;
          const inbound =
            operation.a === null &&
            tag === REF &&
            stored === operation.e &&
            fact.e > GENESIS_TX;
          const attribute = fact.a;
          if (
            !(own || inbound) ||
            (operation.a !== null && attribute !== operation.a)
          )
            continue;
          if (
            operation.value !== null &&
            (tag !== operation.value.tag ||
              !equalStored(stored, operation.value.stored))
          )
            continue;
          matches.push([key, fact]);
        }
        for (const [key, fact] of matches) {
          working.delete(key);
          if (isAssertion(fact)) cancelled.add(fact);
          else if (!retracted.some((row) => row.id === fact.id))
            retracted.push(fact);
        }
        continue;
      }
      const key = factKey(operation.e, operation.a, operation.value);
      if (working.has(key)) continue;
      const schema = finalSchema(operation.a);
      if (!schema.many) {
        const conflicts = [...working.entries()].filter(
          ([, fact]) => fact.e === operation.e && fact.a === operation.a,
        );
        for (const [existingKey, fact] of conflicts) {
          if (isAssertion(fact))
            throw new Conflict(
              `attribute ${String(this._nameOrId(operation.a))} holds one value per entity, but this transaction asserts two; declare it many=true or submit one value`,
            );
          working.delete(existingKey);
          if (!retracted.some((row) => row.id === fact.id))
            retracted.push(fact);
        }
      }
      const restored = retracted.find((fact) => rowKey(fact) === key);
      if (restored !== undefined) {
        retracted = retracted.filter((fact) => fact.id !== restored.id);
        working.set(key, restored);
        continue;
      }
      if (schema.unique) {
        const owner = [...working.values()].find((fact) => {
          const tag = isAssertion(fact) ? fact.value.tag : Number(fact.t);
          const stored = isAssertion(fact) ? fact.value.stored : fact.v;
          return (
            fact.a === operation.a &&
            tag === operation.value.tag &&
            equalStored(stored, operation.value.stored) &&
            fact.e !== operation.e
          );
        })?.e;
        if (owner !== undefined)
          throw new Conflict(
            `unique value for ${String(this._nameOrId(operation.a))} already belongs to ${String(this._nameOrId(owner))}; use that entity to upsert or choose another value`,
          );
      }
      working.set(key, operation);
      inserted.push(operation);
    }
    return {
      assertions: inserted.filter((item) => !cancelled.has(item)),
      retractions: retracted,
    };
  }

  #compactPendingAllocations(pending: Pending, assertions: Assertion[]): void {
    const first = this.#nextAvailableId();
    const kept = new Set<bigint>(
      [...pending.names.values()].filter(
        (id) => id >= first && id < pending.nextId,
      ),
    );
    const keepValue = (value: Encoded): void => {
      if (
        value.tag === REF &&
        typeof value.stored === "bigint" &&
        value.stored >= first &&
        value.stored < pending.nextId
      )
        kept.add(value.stored);
    };
    for (const assertion of assertions) {
      if (assertion.e >= first && assertion.e < pending.nextId)
        kept.add(assertion.e);
      if (assertion.a >= first && assertion.a < pending.nextId)
        kept.add(assertion.a);
      keepValue(assertion.value);
    }
    for (const [attribute, value] of pending.txFacts) {
      if (attribute >= first && attribute < pending.nextId) kept.add(attribute);
      keepValue(value);
    }
    const remap = new Map(
      [...kept]
        .sort((a, b) => (a < b ? -1 : 1))
        .map((old, index) => [old, first + BigInt(index)]),
    );
    if (
      [...remap].every(([old, mapped]) => old === mapped) &&
      BigInt(kept.size) === pending.nextId - first
    )
      return;
    const mapValue = (value: Encoded): Encoded => {
      if (value.tag !== REF || typeof value.stored !== "bigint") return value;
      const target = remap.get(value.stored);
      return target === undefined
        ? value
        : { tag: REF, stored: target, logical: target };
    };
    for (const assertion of assertions) {
      assertion.e = remap.get(assertion.e) ?? assertion.e;
      assertion.a = remap.get(assertion.a) ?? assertion.a;
      assertion.value = mapValue(assertion.value);
    }
    pending.txFacts = pending.txFacts.map(([attribute, value]) => [
      remap.get(attribute) ?? attribute,
      mapValue(value),
    ]);
    pending.names = new Map(
      [...pending.names].map(([name, id]) => [name, remap.get(id) ?? id]),
    );
    pending.idNames = new Map(
      [...pending.idNames].map(([id, name]) => [remap.get(id) ?? id, name]),
    );
    pending.allocated = new Map(
      [...pending.allocated]
        .filter(([id]) => kept.has(id))
        .map(([id, name]) => [remap.get(id) ?? id, name]),
    );
    pending.reportIds = new Map(
      [...pending.reportIds]
        .filter(([, id]) => id < first || kept.has(id))
        .map(([token, id]) => [token, remap.get(id) ?? id]),
    );
    pending.nextId = first + BigInt(kept.size);
  }

  #validateSchemaChanges(assertions: Assertion[], retractions: RawRow[]): void {
    const retractedIds = new Set(retractions.map((row) => row.id));
    const finalValues = (attribute: bigint): Array<[bigint, Encoded]> => {
      const rows: Array<[bigint, Encoded]> = this._connection
        .prepare<[bigint], RawRow>(
          "SELECT * FROM fgraph_facts WHERE a=? AND rx IS NULL ORDER BY id",
        )
        .all(attribute)
        .filter((row) => !retractedIds.has(row.id))
        .map((row) => [
          row.e,
          {
            tag: Number(row.t),
            stored: row.v,
            logical: this._logical(Number(row.t), row.v),
          },
        ]);
      rows.push(
        ...assertions
          .filter((candidate) => candidate.a === attribute)
          .map(
            (candidate) => [candidate.e, candidate.value] as [bigint, Encoded],
          ),
      );
      return rows;
    };
    const affected = new Set<bigint>();
    assertions
      .filter(
        (candidate) =>
          (candidate.a >= 5n && candidate.a <= 10n) || candidate.a === 14n,
      )
      .forEach((candidate) => affected.add(candidate.e));
    retractions
      .filter((row) => (row.a >= 5n && row.a <= 10n) || row.a === 14n)
      .forEach((row) => affected.add(row.e));
    for (const target of affected) {
      const schema = blankSchema();
      const existing = this._connection
        .prepare<[bigint], RawRow>(
          "SELECT * FROM fgraph_facts WHERE e=? AND (a BETWEEN 5 AND 10 OR a=14) AND rx IS NULL ORDER BY id",
        )
        .all(target)
        .filter((row) => !retractedIds.has(row.id));
      existing.forEach((row) =>
        this.#updateSchema(
          schema,
          Number(row.a),
          this._logical(Number(row.t), row.v),
        ),
      );
      assertions
        .filter(
          (candidate) =>
            candidate.e === target &&
            ((candidate.a >= 5n && candidate.a <= 10n) || candidate.a === 14n),
        )
        .forEach((candidate) =>
          this.#updateSchema(
            schema,
            Number(candidate.a),
            candidate.value.logical,
          ),
        );
      const values = finalValues(target);
      if (schema.type !== null) {
        if (!TYPE_NAMES.has(schema.type))
          throw new SchemaError(
            `attribute ${String(this._nameOrId(target))} declares unknown type ${schema.type}; use a supported logical type`,
          );
        if (values.some(([, value]) => !valueMatches(schema.type, value)))
          throw new SchemaError(
            `attribute ${String(this._nameOrId(target))} already has incompatible live values; retract or migrate them before changing type`,
          );
      }
      if (!schema.many) {
        const counts = new Map<bigint, number>();
        values.forEach(([entity]) =>
          counts.set(entity, (counts.get(entity) ?? 0) + 1),
        );
        if ([...counts.values()].some((count) => count > 1))
          throw new SchemaError(
            `attribute ${String(this._nameOrId(target))} has multiple live values; retract extras before declaring many=false`,
          );
      }
      if (schema.unique) {
        if (
          schema.type === null ||
          schema.type === "json" ||
          schema.type === "vector"
        )
          throw new SchemaError(
            `attribute ${String(this._nameOrId(target))} requires a non-json, non-vector type before unique=true; declare type first`,
          );
        const owners = new Map<string, Set<bigint>>();
        values.forEach(([entity, value]) => {
          const key = `${value.tag}:${storedKey(value.stored)}`;
          const set = owners.get(key) ?? new Set<bigint>();
          set.add(entity);
          owners.set(key, set);
        });
        if ([...owners.values()].some((set) => set.size > 1))
          throw new SchemaError(
            `attribute ${String(this._nameOrId(target))} has duplicate live values; resolve them before unique=true`,
          );
      }
      if (schema.dims !== null) {
        if (schema.type !== "vector")
          throw new SchemaError(
            `attribute ${String(this._nameOrId(target))} declares dims without type=vector`,
          );
        if (!Number.isSafeInteger(schema.dims) || schema.dims <= 0)
          throw new SchemaError(
            `vector dims ${schema.dims} for ${String(this._nameOrId(target))} must be a positive integer`,
          );
        const mismatch = values.find(
          ([, value]) =>
            value.tag === VECTOR &&
            (value.logical as number[]).length !== schema.dims,
        );
        if (mismatch !== undefined)
          throw new SchemaError(
            `attribute ${String(this._nameOrId(target))} already contains vectors with another dimension; migrate them before changing dims`,
          );
      }
      if (schema.vectorModel !== null && schema.type !== "vector")
        throw new SchemaError(
          `attribute ${String(this._nameOrId(target))} declares vectorModel without type=vector`,
        );
    }
  }

  #finalEntityFacts(
    entity: bigint,
    assertions: Assertion[],
    retractedIds: Set<bigint>,
  ): Array<RawRow | Assertion> {
    const current: Array<RawRow | Assertion> = this.#statement<
      [bigint],
      RawRow
    >("SELECT * FROM fgraph_facts WHERE e=? AND rx IS NULL ORDER BY id")
      .all(entity)
      .filter((row) => !retractedIds.has(row.id));
    current.push(...assertions.filter((assertion) => assertion.e === entity));
    return current;
  }

  #shapeViolationsForEntity(
    entity: bigint,
    assertions: Assertion[] = [],
    retractions: RawRow[] = [],
  ): Array<Record<string, unknown>> {
    const retractedIds = new Set(retractions.map((row) => row.id));
    const facts = this.#finalEntityFacts(entity, assertions, retractedIds);
    const attributeOf = (fact: RawRow | Assertion): bigint => fact.a;
    const valueOf = (fact: RawRow | Assertion): unknown =>
      isAssertion(fact)
        ? fact.value.logical
        : this._logical(Number(fact.t), fact.v);
    const shapeFacts = facts.filter((fact) => attributeOf(fact) === 15n);
    const violations: Array<Record<string, unknown>> = [];
    for (const shapeFact of shapeFacts) {
      const shape = valueOf(shapeFact) as bigint;
      const definition = this.#finalEntityFacts(
        shape,
        assertions,
        retractedIds,
      );
      const required = new Set(
        definition
          .filter((fact) => attributeOf(fact) === 16n)
          .map((fact) => valueOf(fact) as bigint),
      );
      const allowed = new Set(
        definition
          .filter((fact) => attributeOf(fact) === 17n)
          .map((fact) => valueOf(fact) as bigint),
      );
      const closed = definition
        .filter((fact) => attributeOf(fact) === 18n)
        .some((fact) => valueOf(fact) === true);
      const entityName = this._nameOrId(entity);
      const shapeName = this._nameOrId(shape);
      if (closed)
        for (const requiredAttribute of required)
          if (!allowed.has(requiredAttribute))
            violations.push({
              code: "shape_definition",
              entity: entityName,
              shape: shapeName,
              attribute: this._nameOrId(requiredAttribute),
              message:
                "closed shape does not allow one of its required attributes",
            });
      const present = new Set(facts.map(attributeOf));
      for (const requiredAttribute of required)
        if (!present.has(requiredAttribute))
          violations.push({
            code: "required",
            entity: entityName,
            shape: shapeName,
            attribute: this._nameOrId(requiredAttribute),
            message: "required attribute is missing",
          });
      if (closed)
        for (const attribute of present) {
          const name = String(this._nameOrId(attribute));
          if (!name.startsWith("fgraph/") && !allowed.has(attribute))
            violations.push({
              code: "allowed",
              entity: entityName,
              shape: shapeName,
              attribute: name,
              message: "attribute is not allowed by the closed shape",
            });
        }
    }
    return violations;
  }

  #validateShapes(assertions: Assertion[], retractions: RawRow[]): void {
    const entities = new Set<bigint>();
    assertions.forEach((assertion) => entities.add(assertion.e));
    retractions.forEach((row) => entities.add(row.e));
    const changedShapes = new Set<bigint>();
    assertions
      .filter((assertion) => assertion.a >= 16n && assertion.a <= 18n)
      .forEach((assertion) => changedShapes.add(assertion.e));
    retractions
      .filter((row) => row.a >= 16n && row.a <= 18n)
      .forEach((row) => changedShapes.add(row.e));
    for (const shape of changedShapes)
      this._connection
        .prepare<[bigint], { e: bigint }>(
          "SELECT e FROM fgraph_facts WHERE a=15 AND v=? AND t=0 AND rx IS NULL",
        )
        .all(shape)
        .forEach((row) => entities.add(row.e));
    const violations = [...entities].flatMap((entity) =>
      this.#shapeViolationsForEntity(entity, assertions, retractions),
    );
    if (violations.length > 0) {
      const first = violations[0] as Record<string, unknown>;
      throw new SchemaError(
        `shape validation failed (${String(first.code)}): ${String(first.entity)} ${String(first.attribute)} ${String(first.message)}`,
      );
    }
  }

  #nextTimestamp(override?: bigint): bigint {
    if (override !== undefined) return instantValue(override);
    let proposed = instantValue(this.#clock());
    const row = this._connection
      .prepare<[], { latest: bigint | null }>(
        "SELECT max(v) latest FROM fgraph_facts WHERE a=1 AND tx=e AND t=5 AND rx IS NULL",
      )
      .get();
    if (
      row?.latest !== null &&
      row?.latest !== undefined &&
      proposed <= row.latest
    )
      proposed = instantValue(row.latest + 1_000_000n);
    return proposed;
  }

  #latestBasis(): bigint {
    const row = this._connection
      .prepare<[], { tx: bigint | null }>(
        "SELECT max(tx) tx FROM fgraph_events",
      )
      .get();
    if (row?.tx === null || row?.tx === undefined)
      throw new FormatError(
        "fgraph_events has no genesis receipt; restore a valid format-v2 file",
      );
    return row.tx;
  }

  #normalizedRequest(data: unknown, options: TransactOptions): unknown {
    const normalized: Record<string, unknown> = {};
    for (const key of ["by", "source", "meta", "tx"] as const) {
      if (!Object.hasOwn(options, key)) continue;
      if (options[key] === undefined)
        throw new TypeError(
          `transaction option ${key} is explicitly undefined; omit it or provide a JSON value`,
        );
      normalized[key] = options[key];
    }
    return { data, options: normalized };
  }

  #validateOperationId(value: string | undefined): void {
    if (value === undefined) return;
    const size = Buffer.byteLength(value, "utf8");
    if (
      !value.isWellFormed() ||
      size < 1 ||
      size > 512 ||
      /\p{Cc}/u.test(value)
    )
      throw new TypeError(
        "operationId must be 1-512 UTF-8 bytes without control characters",
      );
  }

  #duplicateOperation(
    operationId: string,
    requestHash: Buffer,
  ): TxReport | null {
    const row = this._connection
      .prepare<
        [string],
        {
          tx: bigint;
          request_hash: Buffer;
          gid: Buffer;
          at: bigint;
          basis: bigint;
        }
      >(
        "SELECT ev.tx,ev.request_hash,i.gid,f.v at," +
          "(SELECT max(prior.tx) FROM fgraph_events prior WHERE prior.tx<ev.tx) basis FROM fgraph_events ev " +
          "JOIN fgraph_ids i ON i.id=ev.tx " +
          "JOIN fgraph_facts f ON f.e=ev.tx AND f.a=1 AND f.tx=ev.tx " +
          "WHERE ev.operation_id=?",
      )
      .get(operationId);
    if (row === undefined) return null;
    if (!row.request_hash.equals(requestHash))
      throw new Conflict(
        `operationId ${JSON.stringify(operationId)} was already used for another request`,
      );
    return {
      status: "already_applied",
      event: uuidText(row.gid),
      basis_tx: publicId(row.basis),
      tx: publicId(row.tx),
      at: publicInteger(row.at),
      ids: {},
      asserted: [],
      retracted: [],
    };
  }

  #insertIdentityRegistry(
    pending: Pending,
    transaction: bigint,
    event: Buffer,
  ): void {
    const insert = this._connection.prepare(
      "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,?,?,?)",
    );
    [...pending.allocated]
      .sort(([left], [right]) => (left < right ? -1 : 1))
      .forEach(([id, name], ordinal) => {
        insert.run(
          id,
          name,
          name === null ? derivedEntityId(event, BigInt(ordinal)) : null,
          transaction,
        );
        // Rollback invalidates the cache; a successful local write can publish
        // its exact name delta without rescanning every prior identity.
        if (name !== null) {
          this.#names.set(name, id);
          this.#idNames.set(id, name);
        }
      });
    insert.run(transaction, null, event, transaction);
  }

  #identitySelector(entity: bigint): string | { eid: string } {
    const row = this.#statement<
      [bigint],
      { name: string | null; gid: Buffer | null }
    >("SELECT name,gid FROM fgraph_ids WHERE id=?").get(entity);
    if (row === undefined)
      throw new FormatError(
        `identity ${entity} is missing from the registry; restore a valid format-v2 file`,
      );
    return row.name ?? { eid: uuidText(row.gid as Buffer) };
  }

  #eventValue(value: Encoded): unknown {
    if (value.tag === REF)
      return { ref: this.#identitySelector(value.logical as bigint) };
    return wireValue(value.tag, value.logical, (id) => this._nameOrId(id));
  }

  #eventRecord(
    event: string,
    at: bigint,
    transaction: bigint,
    pending: Pending,
    assertions: Assertion[],
    retractions: RawRow[],
    options: TransactOptions,
  ): Record<string, unknown> {
    const record: Record<string, unknown> = {
      fgraph: "event/1",
      event,
      at,
      created: [...pending.allocated]
        .sort(([left], [right]) => (left < right ? -1 : 1))
        .map(([id]) => this.#identitySelector(id)),
      asserted: assertions
        .filter((assertion) => assertion.e !== transaction)
        .map((assertion) => [
          this.#identitySelector(assertion.e),
          String(this.#identitySelector(assertion.a)),
          this.#eventValue(assertion.value),
          typeName(assertion.value.tag),
        ]),
      retracted: retractions.map((row) => [
        this.#identitySelector(row.e),
        String(this.#identitySelector(row.a)),
        Number(row.t) === REF
          ? { ref: this.#identitySelector(row.v as bigint) }
          : wireValue(
              Number(row.t),
              this._logical(Number(row.t), row.v),
              (id) => this._nameOrId(id),
            ),
        typeName(Number(row.t)),
      ]),
    };
    if (options.by !== undefined) record.by = options.by;
    if (options.source !== undefined) record.source = options.source;
    if (Object.hasOwn(options, "meta")) record.meta = options.meta;
    if (pending.txFacts.length > 0)
      record.tx_facts = pending.txFacts.map(([attribute, value]) => [
        String(this.#identitySelector(attribute)),
        this.#eventValue(value),
        typeName(value.tag),
      ]);
    return record;
  }

  transact(
    data: unknown,
    options: TransactOptions & {
      _at?: number | bigint;
      _extraTxFacts?: Array<[string, unknown]>;
      _force?: boolean;
      _eventId?: string;
      _eventHash?: Buffer;
      _eventData?: string;
      _originAt?: bigint;
      _compactReport?: boolean;
      _requestHashOverride?: Buffer;
      _prepareData?: () => unknown;
    } = {},
  ): TxReport {
    return this.#atomic(() => {
      const basis = this.#latestBasis();
      this.#validateOperationId(options.operationId);
      if (
        options._requestHashOverride !== undefined &&
        (!Buffer.isBuffer(options._requestHashOverride) ||
          options._requestHashOverride.length !== 32)
      )
        throw new TypeError(
          "internal request hash override must be a 32-byte SHA-256 digest",
        );
      const requestHash =
        options._requestHashOverride ??
        digest(this.#normalizedRequest(data, options));
      if (options.operationId !== undefined) {
        const duplicate = this.#duplicateOperation(
          options.operationId,
          requestHash,
        );
        if (duplicate !== null) return duplicate;
      }
      if (options.ifBasisTx !== undefined) {
        const expectedBasis = asBigInt(options.ifBasisTx, "ifBasisTx");
        if (expectedBasis !== basis)
          throw new Conflict(
            `basis changed from ${expectedBasis} to ${basis}; reread and retry the transaction`,
          );
      }
      if (options.by !== undefined && typeof options.by !== "string")
        throw new TypeError(
          `transaction by=${String(options.by)} is invalid; use a text author name`,
        );
      if (options.source !== undefined && typeof options.source !== "string")
        throw new TypeError(
          `transaction source=${String(options.source)} is invalid; use a text provenance identifier`,
        );
      this.#refreshCache();
      const transactionData =
        options._prepareData === undefined ? data : options._prepareData();
      const pending = this.#newPending();
      this.#parseData(transactionData, pending);
      this.#parseTxFacts(options.tx, pending, options._extraTxFacts);
      this.#validateCASIsolation(pending);
      const planned = this.#planDiff(pending.operations);
      this.#compactPendingAllocations(pending, planned.assertions);
      const hasMeta =
        options._force === true ||
        options.operationId !== undefined ||
        options.source !== undefined ||
        options.by !== undefined ||
        Object.hasOwn(options, "meta") ||
        pending.txFacts.length > 0;
      const hasIdentity = pending.allocated.size > 0;
      if (
        planned.assertions.length === 0 &&
        planned.retractions.length === 0 &&
        !hasMeta &&
        !hasIdentity
      ) {
        return {
          status: "noop",
          event: null,
          basis_tx: publicId(basis),
          tx: null,
          at: null,
          ids: Object.fromEntries(
            [...pending.reportIds].map(([key, value]) => [
              key,
              publicId(value),
            ]),
          ),
          asserted: [],
          retracted: [],
        };
      }
      const transaction = this.#takeId(pending);
      const eventText = (
        options._eventId ?? this.#eventId(transaction)
      ).toLowerCase();
      const event = uuidBytes(eventText);
      this.#insertIdentityRegistry(pending, transaction, event);
      planned.assertions.forEach((assertion) => {
        if (assertion.e === 0n) assertion.e = transaction;
      });
      this.#validateSchemaChanges(planned.assertions, planned.retractions);
      this.#validateShapes(planned.assertions, planned.retractions);
      const metadata: Array<[bigint, Encoded]> = [];
      if (options.by !== undefined) metadata.push([2n, encode(options.by)]);
      if (options.source !== undefined)
        metadata.push([3n, encode(options.source)]);
      if (Object.hasOwn(options, "meta"))
        metadata.push([4n, encode({ json: options.meta })]);
      const atValue = this.#nextTimestamp(
        options._at === undefined ? undefined : instantValue(options._at),
      );
      metadata.unshift([
        1n,
        { tag: INSTANT, stored: atValue, logical: atValue },
      ]);
      const assertedRows = metadata.map(([attribute, value]) =>
        this.#insertRawFact(transaction, attribute, value, transaction),
      );
      assertedRows.push(
        ...planned.assertions.map((assertion) =>
          this.#insertRawFact(
            assertion.e,
            assertion.a,
            assertion.value,
            transaction,
          ),
        ),
      );
      // Build the portable event while indirect retraction values are still readable.
      const eventRecord = this.#eventRecord(
        eventText,
        options._originAt ?? atValue,
        transaction,
        pending,
        planned.assertions,
        planned.retractions,
        options,
      );
      const eventData = options._eventData ?? canonicalEventData(eventRecord);
      if (Buffer.byteLength(eventData, "utf8") > MAX_EVENT_BYTES)
        throw new TooLarge(
          `canonical event exceeds ${MAX_EVENT_BYTES} portable bytes`,
        );
      const computedEventHash = eventHash(eventData);
      if (
        options._eventHash !== undefined &&
        !options._eventHash.equals(computedEventHash)
      )
        throw new Conflict(
          "portable event payload changed while applying; reject the non-canonical or incompatible event",
        );
      const renderedRetractions = planned.retractions.map((row) =>
        this.#deleteOrRetract(row, transaction),
      );
      this.#gcBlobs(planned.retractions);
      this._connection
        .prepare(
          "INSERT INTO fgraph_events(tx,event_hash,event_data,operation_id,request_hash) VALUES (?,?,?,?,?)",
        )
        .run(
          transaction,
          computedEventHash,
          eventData,
          options.operationId ?? null,
          options.operationId === undefined ? null : requestHash,
        );
      this._connection
        .prepare("UPDATE fgraph_meta SET value=? WHERE key='next_id'")
        .run(pending.nextId);
      return {
        status: "applied",
        event: eventText,
        basis_tx: publicId(basis),
        tx: publicId(transaction),
        at: publicInteger(atValue),
        ids: Object.fromEntries(
          [...pending.reportIds].map(([key, value]) => [key, publicId(value)]),
        ),
        asserted:
          options._compactReport === true
            ? []
            : assertedRows.map((row) =>
                this._renderRow(row, undefined, pending.idNames),
              ),
        retracted: renderedRetractions,
      };
    });
  }

  add(data: unknown, options: TransactOptions = {}): TxReport {
    return this.transact(data, options);
  }

  retract(ref: EntityRef, attr?: string, value?: unknown): TxReport {
    const operation: unknown[] = ["retract", ref];
    if (attr !== undefined) {
      operation.push(attr);
      if (arguments.length >= 3) operation.push(value);
    } else if (arguments.length >= 3)
      throw new TypeError(
        `cannot retract a value without an attribute; provide attr first`,
      );
    return this.transact(operation);
  }

  declare(attr: string, options: DeclareOptions = {}): TxReport {
    this.#validateAttribute(attr);
    if (
      options.ref === true &&
      options.type !== undefined &&
      options.type !== "ref"
    )
      throw new SchemaError(
        `attribute ${JSON.stringify(attr)} cannot declare ref=true and type=${JSON.stringify(options.type)}; choose type='ref'`,
      );
    const declaredType = options.ref === true ? "ref" : options.type;
    if (declaredType !== undefined && !TYPE_NAMES.has(declaredType))
      throw new SchemaError(
        `attribute ${JSON.stringify(attr)} has unknown type ${JSON.stringify(declaredType)}; use a supported logical type`,
      );
    if (
      options.dims !== undefined &&
      declaredType !== undefined &&
      declaredType !== "vector"
    )
      throw new SchemaError(
        `attribute ${JSON.stringify(attr)} declares dims but type=${JSON.stringify(declaredType)}; use type='vector'`,
      );
    const data: Record<string, unknown> = { id: attr };
    if (declaredType !== undefined) data["fgraph/type"] = declaredType;
    if (options.many !== undefined) data["fgraph/many"] = options.many;
    if (options.unique !== undefined) data["fgraph/unique"] = options.unique;
    if (options.nohistory !== undefined)
      data["fgraph/nohistory"] = options.nohistory;
    if (options.dims !== undefined) data["fgraph/dims"] = options.dims;
    if (options.doc !== undefined) data["fgraph/doc"] = options.doc;
    if (options.vectorModel !== undefined)
      data["fgraph/vector-model"] = options.vectorModel;
    if (Object.keys(data).length === 1)
      throw new SchemaError(
        `declaration for ${JSON.stringify(attr)} sets no behavior; provide type/ref/many/unique/nohistory/dims/doc/vectorModel`,
      );
    return this.transact(data, {
      ...(options.operationId === undefined
        ? {}
        : { operationId: options.operationId }),
      ...(options.ifBasisTx === undefined
        ? {}
        : { ifBasisTx: options.ifBasisTx }),
    });
  }

  defineShape(
    name: string,
    options: {
      required?: string[];
      allowed?: string[];
      closed?: boolean;
      operationId?: string;
      ifBasisTx?: WireInteger;
    } = {},
  ): TxReport {
    const required = options.required ?? [];
    const allowed = options.allowed ?? [];
    if (
      !Array.isArray(required) ||
      !Array.isArray(allowed) ||
      required.some((value) => typeof value !== "string") ||
      allowed.some((value) => typeof value !== "string") ||
      (options.closed !== undefined && typeof options.closed !== "boolean")
    )
      throw new SchemaError(
        "shape required/allowed must be attribute-name arrays and closed must be boolean",
      );
    [...required, ...allowed].forEach((attribute) =>
      this.#validateAttribute(attribute),
    );
    const effectiveAllowed = [
      ...new Set(options.closed === true ? [...required, ...allowed] : allowed),
    ].sort();
    const definition: Record<string, unknown> = {
      id: name,
      "fgraph/shape-closed": options.closed ?? false,
    };
    if (required.length > 0)
      definition["fgraph/shape-required"] = [...new Set(required)]
        .sort()
        .map((attribute) => ({ ref: attribute }));
    if (effectiveAllowed.length > 0)
      definition["fgraph/shape-allowed"] = effectiveAllowed.map(
        (attribute) => ({ ref: attribute }),
      );
    return this.transact(
      [
        ["retract", name, "fgraph/shape-required"],
        ["retract", name, "fgraph/shape-allowed"],
        ["retract", name, "fgraph/shape-closed"],
        definition,
      ],
      {
        ...(options.operationId === undefined
          ? {}
          : { operationId: options.operationId }),
        ...(options.ifBasisTx === undefined
          ? {}
          : { ifBasisTx: options.ifBasisTx }),
      },
    );
  }

  validate(ref: EntityRef): Record<string, unknown> {
    this.#ensureOpen();
    const entity = this._resolveRead(ref) as bigint;
    const violations = this.#shapeViolationsForEntity(entity);
    return {
      basis_tx: this._basisTx(),
      valid: violations.length === 0,
      violations,
    };
  }

  _visibleFactRows(
    entity: bigint | null = null,
    attribute: bigint | null = null,
  ): RawRow[] {
    const visibility = this._visibility();
    const conditions = [visibility.sql];
    const values: unknown[] = [...visibility.params];
    if (entity !== null) {
      conditions.push("e=?");
      values.push(entity);
    }
    if (attribute !== null) {
      conditions.push("a=?");
      values.push(attribute);
    }
    return this._connection
      .prepare<unknown[], RawRow>(
        `SELECT * FROM fgraph_facts WHERE ${conditions.join(" AND ")} ORDER BY a, tx, id`,
      )
      .all(...values);
  }

  _queryDatoms(
    entity: bigint | null,
    attribute: bigint | null,
    value: Cell | null = null,
    eventTransaction: bigint | null = null,
    added: boolean | null = null,
  ): QueryDatom[] {
    this.#ensureOpen();
    const basis = this._asOf ?? this.#latestBasis();
    const conditions: string[] = [];
    const parameters: unknown[] = [];
    if (entity !== null) {
      conditions.push("e=?");
      parameters.push(entity);
    }
    if (attribute !== null) {
      conditions.push("a=?");
      parameters.push(attribute);
    }
    if (value !== null && attribute !== null) {
      const wire =
        value.tag === FLOAT
          ? new JsonFloat(value.value as number)
          : wireValue(value.tag, value.value, (id) => id);
      const encoded = encode(wire, (ref) => asBigInt(ref));
      conditions.push("t=?", "v=?");
      parameters.push(encoded.tag, encoded.stored);
    }
    if (this._querySource === "current") {
      if (added === false) return [];
      if (eventTransaction !== null) {
        conditions.push("tx=?");
        parameters.push(eventTransaction);
      }
      const visibility = this._visibility(basis);
      conditions.push(visibility.sql);
      parameters.push(...visibility.params);
    } else {
      conditions.push("tx<=?");
      parameters.push(basis);
      if (added === true) {
        if (eventTransaction !== null) {
          conditions.push("tx=?");
          parameters.push(eventTransaction);
        }
      } else if (added === false) {
        conditions.push("rx IS NOT NULL", "rx<=?");
        parameters.push(basis);
        if (eventTransaction !== null) {
          conditions.push("rx=?");
          parameters.push(eventTransaction);
        }
      } else if (eventTransaction !== null) {
        conditions.push("(tx=? OR rx=?)");
        parameters.push(eventTransaction, eventTransaction);
      }
    }
    const rows = this._connection
      .prepare<unknown[], RawRow>(
        `SELECT * FROM fgraph_facts WHERE ${conditions.join(" AND ")} ORDER BY id`,
      )
      .all(...parameters);
    if (this._querySource === "current")
      return rows.map((row) => ({ row, eventTx: row.tx, added: true }));
    return rows.flatMap((row) => {
      const events: QueryDatom[] = [{ row, eventTx: row.tx, added: true }];
      if (row.rx !== null && row.rx <= basis)
        events.push({ row, eventTx: row.rx, added: false });
      return events.filter(
        (event) =>
          (eventTransaction === null || event.eventTx === eventTransaction) &&
          (added === null || event.added === added),
      );
    });
  }

  _queryDatomsForEntities(
    entities: bigint[],
    attribute: bigint,
  ): Map<bigint, QueryDatom[]> | null {
    if (this._querySource !== "current") return null;
    const basis = this._asOf ?? this.#latestBasis();
    const visibility = this._visibility(basis);
    const result = new Map<bigint, QueryDatom[]>();
    const unique = [...new Set(entities)].sort((left, right) =>
      left < right ? -1 : left > right ? 1 : 0,
    );
    for (let offset = 0; offset < unique.length; offset += 400) {
      const chunk = unique.slice(offset, offset + 400);
      const placeholders = chunk.map(() => "?").join(",");
      const rows = this._connection
        .prepare<unknown[], RawRow>(
          `SELECT * FROM fgraph_facts WHERE a=? AND e IN (${placeholders}) AND ${visibility.sql} ORDER BY id`,
        )
        .all(attribute, ...chunk, ...visibility.params);
      for (const row of rows) {
        const items = result.get(row.e) ?? [];
        items.push({ row, eventTx: row.tx, added: true });
        result.set(row.e, items);
      }
    }
    return result;
  }

  /** @internal Query validation shares this semantic schema check. */
  _validatePullPattern(pattern: unknown): asserts pattern is unknown[] {
    if (!Array.isArray(pattern))
      throw new QueryError("pull pattern is invalid; use an attribute array");
    for (const item of pattern) {
      if (typeof item === "string") {
        if (item === "*") continue;
        const forward = item.replace("/_", "/");
        try {
          this.#validateAttribute(forward);
        } catch (error) {
          if (error instanceof SchemaError)
            throw new QueryError(
              `pull attribute ${JSON.stringify(item)} is invalid; use namespace/name or namespace/_name`,
            );
          throw error;
        }
        continue;
      }
      if (!isRecord(item) || Object.keys(item).length !== 1)
        throw new QueryError(
          "pull item is invalid; use an attribute, '*', or one nested ref object",
        );
      const [attribute, subpattern] = Object.entries(item)[0] as [
        string,
        unknown,
      ];
      if (attribute.includes("/_"))
        throw new QueryError(
          `nested pull attribute ${JSON.stringify(attribute)} is reverse; use the reverse attribute string directly`,
        );
      try {
        this.#validateAttribute(attribute);
      } catch (error) {
        if (error instanceof SchemaError)
          throw new QueryError(
            `nested pull attribute ${JSON.stringify(attribute)} is invalid; use namespace/name`,
          );
        throw error;
      }
      const attributeId = this.#names.get(attribute);
      if (attributeId === undefined)
        throw new QueryError(
          `nested pull attribute ${JSON.stringify(attribute)} is unknown; declare or populate a ref attribute`,
        );
      const schema = this._schema(attributeId);
      const rows = this._visibleFactRows(null, attributeId);
      if (
        schema.type !== "ref" &&
        (rows.length === 0 || rows.some((row) => Number(row.t) !== REF))
      ) {
        throw new QueryError(
          `nested pull attribute ${JSON.stringify(attribute)} is not a ref; use a ref attribute`,
        );
      }
      this._validatePullPattern(subpattern);
    }
  }

  _pullEntity(
    entity: bigint,
    pattern: unknown[],
    depth: number,
    seen: ReadonlySet<bigint>,
  ): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    const rows = this._visibleFactRows(entity);
    const requestedAll = pattern.includes("*");
    const direct = new Set(
      pattern.filter(
        (item): item is string =>
          typeof item === "string" && item !== "*" && !item.includes("/_"),
      ),
    );
    const nested = new Map<string, unknown[]>();
    for (const item of pattern) {
      if (isRecord(item)) {
        const [attribute, subpattern] = Object.entries(item)[0] as [
          string,
          unknown[],
        ];
        nested.set(attribute, subpattern);
      }
    }
    for (const row of rows) {
      const attribute = this.#idNames.get(row.a) as string;
      if (!requestedAll && !direct.has(attribute) && !nested.has(attribute))
        continue;
      const tag = Number(row.t);
      let value: unknown;
      if (tag === REF && nested.has(attribute) && depth > 0) {
        const target = row.v as bigint;
        value = seen.has(target)
          ? { ref: this._nameOrId(target) }
          : this._pullEntity(
              target,
              nested.get(attribute) as unknown[],
              depth - 1,
              new Set([...seen, target]),
            );
      } else if (tag === REF && requestedAll && depth > 1) {
        const target = row.v as bigint;
        value = seen.has(target)
          ? { ref: this._nameOrId(target) }
          : this._pullEntity(
              target,
              ["*"],
              depth - 1,
              new Set([...seen, target]),
            );
      } else value = this._wire(tag, row.v);
      if (this._schema(row.a).many) {
        const values = (result[attribute] as unknown[] | undefined) ?? [];
        values.push(value);
        result[attribute] = values;
      } else result[attribute] = value;
    }
    for (const item of pattern) {
      if (typeof item !== "string" || !item.includes("/_")) continue;
      const [namespace, reverseName] = item.split("/_", 2);
      const forward = `${namespace}/${reverseName}`;
      const attribute = this.#names.get(forward);
      if (attribute === undefined) {
        result[item] = [];
        continue;
      }
      const visibility = this._visibility();
      const inbound = this._connection
        .prepare<unknown[], { e: bigint }>(
          `SELECT e FROM fgraph_facts WHERE a=? AND t=0 AND v=? AND ${visibility.sql} ORDER BY id`,
        )
        .all(attribute, entity, ...visibility.params);
      result[item] = inbound.map((row) =>
        depth > 1
          ? this._pullEntity(
              row.e,
              ["*"],
              Math.max(depth - 1, 0),
              new Set([...seen, row.e]),
            )
          : { ref: this._nameOrId(row.e) },
      );
    }
    return result;
  }

  entity(ref: EntityRef, depth = 1): Record<string, unknown> {
    this.#ensureOpen();
    if (!Number.isSafeInteger(depth) || depth < 0)
      throw new QueryError(
        `entity depth ${depth} is invalid; use zero or a positive integer recursion depth`,
      );
    const entity = this._resolveRead(ref) as bigint;
    return this._pullEntity(entity, ["*"], depth, new Set([entity]));
  }

  pull(ref: EntityRef, pattern: unknown[]): Record<string, unknown> {
    this.#ensureOpen();
    this._validatePullPattern(pattern);
    const entity = this._resolveRead(ref) as bigint;
    return this._pullEntity(entity, pattern, 1, new Set([entity]));
  }

  q(
    query: Record<string, unknown>,
    args: Record<string, unknown> = {},
    options: { budget?: number; signal?: AbortSignal } = {},
  ): Result {
    this.#ensureOpen();
    if (
      options.budget !== undefined &&
      (!Number.isSafeInteger(options.budget) || options.budget <= 0)
    ) {
      throw new QueryError(
        `query budget ${String(options.budget)} is invalid; use a positive safe integer`,
      );
    }
    const source = query.source ?? "current";
    if (source !== "current" && source !== "history")
      throw new QueryError("query source must be 'current' or 'history'");
    const previous = this._querySource;
    this._querySource = source;
    try {
      return evaluate(this, query, args, {
        budget: Math.min(options.budget ?? this.queryBudget, this.queryBudget),
        signal: options.signal,
      });
    } finally {
      this._querySource = previous;
    }
  }

  datoms(
    index: "eavt" | "avet" | "vaet" = "eavt",
    options: {
      components?: unknown[];
      source?: "current" | "history";
      limit?: number;
      cursor?: string;
    } = {},
  ): DatomPage {
    this.#ensureOpen();
    if (!(["eavt", "avet", "vaet"] as const).includes(index))
      throw new QueryError("datom index must be eavt, avet, or vaet");
    const source = options.source ?? "current";
    if (source !== "current" && source !== "history")
      throw new QueryError("datom source must be current or history");
    const components = options.components ?? [];
    if (!Array.isArray(components) || components.length > 5)
      throw new QueryError("datom components must be an index-prefix array");
    const limit = options.limit ?? 100;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000)
      throw new QueryError(
        "datom limit must be an integer from 1 through 1000",
      );
    const componentsHash = digest(components).toString("hex");
    const visibleBasis = this._asOf ?? this.#latestBasis();
    let basis = visibleBasis;
    let cursorKey: unknown[] | null = null;
    if (options.cursor !== undefined) {
      if (
        Buffer.byteLength(options.cursor, "utf8") > MAX_CURSOR_BYTES ||
        !/^[A-Za-z0-9_-]+$/u.test(options.cursor)
      )
        throw new QueryError("datom cursor is malformed or truncated");
      let decoded: unknown;
      try {
        const bytes = Buffer.from(options.cursor, "base64url");
        if (bytes.toString("base64url") !== options.cursor)
          throw new Error("non-canonical base64url");
        decoded = parseJson(bytes.toString("utf8"), "datom cursor");
      } catch {
        throw new QueryError("datom cursor is malformed or truncated");
      }
      if (
        !isRecord(decoded) ||
        decoded.v !== FORMAT_VERSION ||
        decoded.index !== index ||
        decoded.source !== source ||
        decoded.components !== componentsHash ||
        !Array.isArray(decoded.last) ||
        decoded.last.length !== 7
      )
        throw new QueryError(
          "datom cursor does not match this format, source, index, or component prefix",
        );
      basis = asBigInt(decoded.basis, "datom cursor basis");
      cursorKey = decoded.last;
      if (basis < GENESIS_TX || basis > visibleBasis)
        throw new QueryError(
          "datom cursor basis is not visible from this database view",
        );
      if (this._asOf !== null && basis !== this._asOf)
        throw new QueryError(
          "datom cursor basis differs from this historical view",
        );
    }
    const resolveEntity = (value: unknown, context: string): bigint => {
      try {
        return this._resolveRead(value as EntityRef) as bigint;
      } catch {
        throw new QueryError(`${context} must identify an existing entity`);
      }
    };
    const resolveAttribute = (value: unknown): bigint => {
      if (typeof value !== "string")
        throw new QueryError("datom attribute component must be a name");
      const attribute = this._attributeId(value);
      if (attribute === null)
        throw new QueryError(
          `datom attribute ${JSON.stringify(value)} was not found`,
        );
      return attribute;
    };
    const conditions: string[] = [];
    const parameters: unknown[] = [];
    let boundAttribute: bigint | null = null;
    const bindValue = (value: unknown): void => {
      if (boundAttribute === null)
        throw new QueryError(
          "datom value prefix requires an earlier attribute component",
        );
      const encoded = this._encodeReadValue(
        value,
        this._schema(boundAttribute, basis),
      );
      conditions.push("t=?", "v=?");
      parameters.push(encoded.tag, encoded.stored);
    };
    const bindAdded = (value: unknown): void => {
      if (typeof value !== "boolean")
        throw new QueryError("datom added component must be a boolean");
      conditions.push("added=?");
      parameters.push(value ? 1 : 0);
    };
    if (index === "eavt") {
      if (components.length > 0) {
        conditions.push("e=?");
        parameters.push(resolveEntity(components[0], "eavt entity"));
      }
      if (components.length > 1) {
        boundAttribute = resolveAttribute(components[1]);
        conditions.push("a=?");
        parameters.push(boundAttribute);
      }
      if (components.length > 2) bindValue(components[2]);
      if (components.length > 3) {
        conditions.push("event_tx=?");
        parameters.push(resolveEntity(components[3], "datom transaction"));
      }
      if (components.length > 4) bindAdded(components[4]);
    } else if (index === "avet") {
      if (components.length > 0) {
        boundAttribute = resolveAttribute(components[0]);
        conditions.push("a=?");
        parameters.push(boundAttribute);
      }
      if (components.length > 1) bindValue(components[1]);
      if (components.length > 2) {
        conditions.push("e=?");
        parameters.push(resolveEntity(components[2], "avet entity"));
      }
      if (components.length > 3) {
        conditions.push("event_tx=?");
        parameters.push(resolveEntity(components[3], "datom transaction"));
      }
      if (components.length > 4) bindAdded(components[4]);
    } else {
      conditions.push("t=0");
      if (components.length > 0) {
        const value = components[0];
        if (
          !isRecord(value) ||
          Object.keys(value).length !== 1 ||
          !Object.hasOwn(value, "ref")
        )
          throw new QueryError("vaet value component must use {ref:entity}");
        conditions.push("v=?");
        parameters.push(resolveEntity(value.ref, "vaet value"));
      }
      if (components.length > 1) {
        boundAttribute = resolveAttribute(components[1]);
        conditions.push("a=?");
        parameters.push(boundAttribute);
      }
      if (components.length > 2) {
        conditions.push("e=?");
        parameters.push(resolveEntity(components[2], "vaet entity"));
      }
      if (components.length > 3) {
        conditions.push("event_tx=?");
        parameters.push(resolveEntity(components[3], "datom transaction"));
      }
      if (components.length > 4) bindAdded(components[4]);
    }
    const order =
      index === "eavt"
        ? ["e", "a", "v", "t", "event_tx", "added", "id"]
        : index === "avet"
          ? ["a", "v", "e", "t", "event_tx", "added", "id"]
          : ["v", "a", "e", "t", "event_tx", "added", "id"];
    const decodeCursorCell = (value: unknown): unknown => {
      if (
        isRecord(value) &&
        Object.keys(value).length === 1 &&
        typeof value.bytes === "string"
      ) {
        const result = Buffer.from(value.bytes, "base64");
        if (result.toString("base64") !== value.bytes)
          throw new QueryError("datom cursor contains malformed bytes");
        return result;
      }
      if (
        typeof value !== "string" &&
        typeof value !== "number" &&
        typeof value !== "bigint"
      )
        throw new QueryError("datom cursor contains an invalid seek value");
      return value;
    };
    if (cursorKey !== null) {
      conditions.push(
        `(${order.join(",")}) > (${order.map(() => "?").join(",")})`,
      );
      parameters.push(...cursorKey.map(decodeCursorCell));
    }
    const current =
      "SELECT id,e,a,v,t,tx,rx,tx event_tx,1 added FROM fgraph_facts " +
      "WHERE tx<=? AND (rx IS NULL OR rx>?)";
    const history =
      "SELECT id,e,a,v,t,tx,rx,tx event_tx,1 added FROM fgraph_facts WHERE tx<=? " +
      "UNION ALL SELECT id,e,a,v,t,tx,rx,rx event_tx,0 added FROM fgraph_facts WHERE rx IS NOT NULL AND rx<=?";
    const cte = source === "current" ? current : history;
    const basisParameters = [basis, basis];
    const sql =
      `WITH datoms AS (${cte}) SELECT * FROM datoms` +
      (conditions.length === 0 ? "" : ` WHERE ${conditions.join(" AND ")}`) +
      ` ORDER BY ${order.join(",")} LIMIT ?`;
    interface DatomRow extends RawRow {
      event_tx: bigint;
      added: bigint;
    }
    const rows = this._connection
      .prepare<unknown[], DatomRow>(sql)
      .all(...basisParameters, ...parameters, limit + 1);
    const selected = rows.slice(0, limit);
    const cursorCell = (value: unknown): unknown =>
      Buffer.isBuffer(value) ? { bytes: value.toString("base64") } : value;
    const keyFor = (row: DatomRow): unknown[] => {
      const values: Record<string, unknown> = {
        id: row.id,
        e: row.e,
        a: row.a,
        v: row.v,
        t: row.t,
        event_tx: row.event_tx,
        added: row.added,
      };
      return order.map((name) => cursorCell(values[name]));
    };
    const last = selected.at(-1);
    return {
      basis_tx: publicId(basis),
      items: selected.map((row) => ({
        e: this._nameOrId(row.e),
        a: String(this._nameOrId(row.a)),
        v: this._wire(Number(row.t), row.v),
        tx: publicId(row.event_tx),
        added: row.added !== 0n,
        fact_id: publicId(row.id),
      })),
      next_cursor:
        rows.length > limit && last !== undefined
          ? Buffer.from(
              canonicalJson({
                v: FORMAT_VERSION,
                basis,
                source,
                index,
                components: componentsHash,
                last: keyFor(last),
              }),
              "utf8",
            ).toString("base64url")
          : null,
    };
  }

  explain(
    query: Record<string, unknown>,
    args: Record<string, unknown> = {},
  ): Record<string, unknown> {
    this.#ensureOpen();
    if (!Array.isArray(query.where))
      throw new QueryError(
        "query where must be an array before it can be explained",
      );
    const source = query.source ?? "current";
    if (source !== "current" && source !== "history")
      throw new QueryError("query source must be current or history");
    const bound = new Set<string>(
      Array.isArray(query.in)
        ? (query.in as unknown[]).filter(
            (value): value is string =>
              typeof value === "string" && Object.hasOwn(args, value),
          )
        : [],
    );
    const sequence = query.where as unknown[];
    const clauses: Array<Record<string, unknown>> = [];
    for (let position = 0; position < sequence.length;) {
      if (isPatternClause(sequence[position])) {
        const block: Array<{ clause: unknown[]; ordinal: number }> = [];
        while (
          position < sequence.length &&
          isPatternClause(sequence[position])
        ) {
          const clause = sequence[position] as unknown[];
          if (![3, 4, 5].includes(clause.length))
            throw new QueryError(
              "invalid pattern; use [e,a,v], [e,a,v,tx], or [e,a,v,tx,added]",
            );
          block.push({ clause, ordinal: position++ });
        }
        while (block.length > 0) {
          block.sort((left, right) => {
            const leftPlan = planPattern(left.clause, bound);
            const rightPlan = planPattern(right.clause, bound);
            return (
              leftPlan.rank - rightPlan.rank || left.ordinal - right.ordinal
            );
          });
          const chosen = block.shift() as (typeof block)[number];
          const plan = planPattern(chosen.clause, bound);
          const boundBefore = [...bound].sort(compareUnicode);
          clauses.push({
            ordinal: chosen.ordinal,
            kind: "pattern",
            access: plan.access,
            bound: boundBefore,
          });
          for (const value of chosen.clause)
            if (typeof value === "string" && value.startsWith("?"))
              bound.add(value);
        }
        continue;
      }
      const clause = sequence[position];
      clauses.push({
        ordinal: position++,
        kind: "barrier",
        operator: Array.isArray(clause)
          ? String(clause[0])
          : isRecord(clause)
            ? (Object.keys(clause)[0] ?? "invalid")
            : "invalid",
      });
    }
    return {
      basis_tx: this._basisTx(),
      source,
      work_limit: this.queryBudget,
      clauses,
      warnings: clauses.some(
        (clause) => isRecord(clause) && clause.access === "scan",
      )
        ? ["unbound datom scan"]
        : [],
    };
  }

  _resolveTime(value: unknown): bigint {
    let timestamp: bigint;
    if (typeof value === "string") timestamp = instantValue(value);
    else if (
      isRecord(value) &&
      Object.keys(value).length === 1 &&
      Object.hasOwn(value, "instant")
    )
      timestamp = instantValue(value.instant);
    else if (typeof value === "number" || typeof value === "bigint") {
      const integer = asBigInt(value, "time");
      if (
        this._connection
          .prepare(
            "SELECT 1 FROM fgraph_facts WHERE e=? AND a=1 AND tx=e LIMIT 1",
          )
          .get(integer) !== undefined
      )
        return integer;
      timestamp = instantValue(integer);
    } else
      throw new TypeError(
        `time ${String(value)} is invalid; use a transaction id or {"instant": RFC3339-or-microseconds}`,
      );
    const row = this._connection
      .prepare<[bigint], { e: bigint }>(
        "SELECT e FROM fgraph_facts WHERE a=1 AND tx=e AND t=5 AND v<=? ORDER BY v DESC, e DESC LIMIT 1",
      )
      .get(timestamp);
    if (row === undefined)
      throw new NotFound(
        `time ${String(value)} precedes the database genesis; choose a later instant or transaction id`,
      );
    return row.e;
  }

  at(value: unknown): Db {
    this.#ensureOpen();
    let point = this._resolveTime(value);
    if (this._asOf !== null && point > this._asOf) point = this._asOf;
    const view = new Db(
      this.path,
      { readOnly: true, queryBudget: this.queryBudget },
      this,
    );
    view._asOf = point;
    return view;
  }

  #txMetadata(transaction: bigint): Record<string, unknown> {
    const rows = this._connection
      .prepare<[bigint, bigint], RawRow>(
        "SELECT * FROM fgraph_facts WHERE e=? AND tx=? ORDER BY id",
      )
      .all(transaction, transaction);
    if (rows.length === 0)
      throw new NotFound(
        `transaction ${transaction} was not found; use a transaction id returned by transact()`,
      );
    const metadata: Record<string, unknown> = {};
    for (const row of rows) {
      if (row.a === 1n) metadata.at = publicInteger(row.v as bigint);
      else if (row.a === 2n) metadata.by = this._logical(Number(row.t), row.v);
      else if (row.a === 3n)
        metadata.source = this._logical(Number(row.t), row.v);
      else if (row.a === 4n)
        metadata.meta = parseJsonValue(
          String(this._logical(Number(row.t), row.v)),
          "stored transaction metadata",
        );
      else if (row.a === IMPORTED_AT_ATTRIBUTE)
        metadata.imported_at = publicInteger(row.v as bigint);
    }
    return metadata;
  }

  _transactionMetadata(transaction: bigint): Record<string, unknown> {
    return this.#txMetadata(transaction);
  }

  receipt(transaction: number | bigint): Record<string, unknown> {
    this.#ensureOpen();
    const tx = asBigInt(transaction, "transaction");
    const readBasis = this._asOf ?? this.#latestBasis();
    if (tx > readBasis)
      throw new NotFound(
        `transaction ${tx} is after this view's basis ${readBasis}`,
      );
    const row = this._connection
      .prepare<
        [bigint],
        {
          event_hash: Buffer;
          operation_id: string | null;
          request_hash: Buffer | null;
          gid: Buffer;
          basis: bigint;
        }
      >(
        "SELECT ev.event_hash,ev.operation_id,ev.request_hash,i.gid," +
          "coalesce((SELECT max(prior.tx) FROM fgraph_events prior WHERE prior.tx<ev.tx),ev.tx) basis " +
          "FROM fgraph_events ev JOIN fgraph_ids i ON i.id=ev.tx WHERE ev.tx=?",
      )
      .get(tx);
    if (row === undefined)
      throw new NotFound(
        `transaction ${tx} was not found; use a transaction id returned by transact()`,
      );
    const facts = this._connection
      .prepare<[bigint], RawRow>(
        "SELECT * FROM fgraph_facts WHERE e=? AND tx=e AND a NOT IN (1,2,3,4,13) ORDER BY id",
      )
      .all(tx)
      .map((fact) => this._renderViewRow(fact));
    return {
      read_basis_tx: publicId(readBasis),
      basis_tx: publicId(row.basis),
      tx: publicId(tx),
      event: uuidText(row.gid),
      event_hash: `sha256:${row.event_hash.toString("hex")}`,
      operation_id: row.operation_id,
      request_hash:
        row.request_hash === null
          ? null
          : `sha256:${row.request_hash.toString("hex")}`,
      ...this.#txMetadata(tx),
      facts,
    };
  }

  history(ref: EntityRef, attr?: string): Array<Record<string, unknown>> {
    this.#ensureOpen();
    const entity = this._resolveRead(ref) as bigint;
    let attribute: bigint | undefined;
    if (attr !== undefined) {
      attribute = this.#names.get(attr);
      if (attribute === undefined)
        throw new NotFound(
          `attribute ${JSON.stringify(attr)} was not found; transact or declare it before reading history`,
        );
    }
    const conditions = ["e=?"];
    const parameters: unknown[] = [entity];
    if (attribute !== undefined) {
      conditions.push("a=?");
      parameters.push(attribute);
    }
    if (this._asOf !== null) {
      conditions.push("tx<=?");
      parameters.push(this._asOf);
    }
    const rows = this._connection
      .prepare<unknown[], RawRow>(
        `SELECT * FROM fgraph_facts WHERE ${conditions.join(" AND ")} ORDER BY tx, id`,
      )
      .all(...parameters);
    return rows.map((row) => {
      const futureRetraction =
        this._asOf !== null && row.rx !== null && row.rx > this._asOf;
      const rendered: Record<string, unknown> = this._renderViewRow(row);
      const start = this.#txMetadata(row.tx);
      for (const key of ["at", "by", "source"] as const)
        if (Object.hasOwn(start, key)) rendered[key] = start[key];
      if (row.rx !== null && !futureRetraction) {
        const end = this.#txMetadata(row.rx);
        for (const key of ["at", "by", "source"] as const)
          if (Object.hasOwn(end, key)) rendered[`rx_${key}`] = end[key];
      }
      return rendered;
    });
  }

  #timeWindow(first: unknown, second: unknown): [bigint, bigint] {
    const start = this._resolveTime(first);
    const end = this._resolveTime(second);
    if (start > end)
      throw new QueryError(
        `time window ${start}..${end} is reversed; provide the earlier boundary first`,
      );
    return [start, end];
  }

  diff(
    t1: unknown,
    t2: unknown,
  ): { asserted: RenderedFact[]; retracted: RenderedFact[] } {
    this.#ensureOpen();
    const [start, requestedEnd] = this.#timeWindow(t1, t2);
    const end =
      this._asOf !== null && requestedEnd > this._asOf
        ? this._asOf
        : requestedEnd;
    if (start >= end) return { asserted: [], retracted: [] };
    const asserted = this._connection
      .prepare<[bigint, bigint], RawRow>(
        "SELECT * FROM fgraph_facts WHERE tx>? AND tx<=? ORDER BY tx, id",
      )
      .all(start, end);
    const retracted = this._connection
      .prepare<[bigint, bigint], RawRow>(
        "SELECT * FROM fgraph_facts WHERE rx>? AND rx<=? ORDER BY rx, id",
      )
      .all(start, end);
    return {
      asserted: asserted.map((row) => this._renderViewRow(row)),
      retracted: retracted.map((row) => this._renderViewRow(row)),
    };
  }

  _latestTx(): bigint {
    return (
      this._connection
        .prepare<[], { value: bigint | null }>(
          "SELECT max(e) value FROM fgraph_facts WHERE a=1 AND tx=e",
        )
        .get()?.value ?? GENESIS_TX
    );
  }

  changes(
    since: unknown,
    until?: unknown,
  ): { asserted: RenderedFact[]; retracted: RenderedFact[] } {
    return this.diff(since, until ?? this._latestTx());
  }

  async *follow(
    since: unknown = GENESIS_TX,
    options: { interval?: number; signal?: AbortSignal } = {},
  ): AsyncGenerator<Record<string, unknown>> {
    this.#ensureOpen();
    if (this._asOf !== null)
      throw new Unsupported(
        "follow on a historical view cannot observe future commits; follow the live database",
      );
    const interval = options.interval ?? 500;
    if (!Number.isFinite(interval) || interval <= 0)
      throw new TypeError(
        `follow interval ${interval} must be positive milliseconds`,
      );
    let cursor = this._resolveTime(since);
    while (!options.signal?.aborted) {
      const latest = this._latestTx();
      if (latest > cursor) {
        const rows = this._connection
          .prepare<[bigint, bigint], { tx: bigint }>(
            "SELECT tx FROM fgraph_events WHERE tx>? AND tx<=? ORDER BY tx",
          )
          .all(cursor, latest);
        for (const row of rows) {
          if (options.signal?.aborted) return;
          yield this.#eventRecordForTx(row.tx);
          cursor = row.tx;
        }
      }
      await sleep(interval);
    }
  }

  why(ref: EntityRef, attr?: string): Array<Record<string, unknown>> {
    this.#ensureOpen();
    const entity = this._resolveRead(ref) as bigint;
    let attribute: bigint | null = null;
    if (attr !== undefined) {
      attribute = this.#names.get(attr) ?? null;
      if (attribute === null)
        throw new NotFound(
          `attribute ${JSON.stringify(attr)} was not found; use a known attribute`,
        );
    }
    return this._visibleFactRows(entity, attribute).map((row) => ({
      ...this._renderViewRow(row),
      provenance: this.at(row.tx).entity(row.tx),
    }));
  }

  speculate<T>(operation: (database: Db) => T): T {
    this.#ensureWritable();
    if (this.#speculationDepth > 0)
      throw new Unsupported(
        "nested speculation is unavailable in the v1 API; use one speculate() scope",
      );
    const savepoint = `fgraph_speculate_${++this.#savepointCounter}`;
    this._connection.exec(`SAVEPOINT ${savepoint}`);
    this.#speculationDepth++;
    try {
      return operation(this);
    } finally {
      this._connection.exec(`ROLLBACK TO ${savepoint}; RELEASE ${savepoint}`);
      this.#speculationDepth--;
      this.#refreshCache(true);
    }
  }

  undo(
    transaction: number | bigint,
    options: {
      by?: string;
      operationId?: string;
      ifBasisTx?: number | bigint;
    } = {},
  ): TxReport {
    this.#ensureWritable();
    const target = asBigInt(transaction, "transaction");
    if (target <= GENESIS_TX)
      throw new Unsupported(
        `system transaction ${target} cannot be undone; choose a user transaction above 64`,
      );
    const request: Record<string, unknown> = {
      operation: "undo",
      tx: publicId(target),
    };
    if (options.by !== undefined) request.by = options.by;
    return this.transact([], {
      tx: { "fgraph/undoes": { ref: publicId(target) } },
      _requestHashOverride: digest(request),
      _prepareData: () => {
        // Undo must inspect the target under the same writer transaction as
        // its inverse, otherwise a newer equal assertion can be retracted.
        this.#txMetadata(target);
        const asserted = this._connection
          .prepare<[bigint, bigint], RawRow>(
            "SELECT * FROM fgraph_facts WHERE tx=? AND e<>? AND rx IS NULL ORDER BY id",
          )
          .all(target, target);
        const retracted = this._connection
          .prepare<[bigint], RawRow>(
            "SELECT * FROM fgraph_facts WHERE rx=? ORDER BY id",
          )
          .all(target);
        const operations: unknown[][] = asserted.map((row) => [
          "retract",
          this._nameOrId(row.e),
          String(this.#idNames.get(row.a) ?? row.a),
          this._wire(Number(row.t), row.v),
        ]);
        operations.push(
          ...retracted.map((row) => [
            "assert",
            this._nameOrId(row.e),
            String(this.#idNames.get(row.a) ?? row.a),
            this._wire(Number(row.t), row.v),
          ]),
        );
        return operations;
      },
      ...(options.by === undefined ? {} : { by: options.by }),
      ...(options.operationId === undefined
        ? {}
        : { operationId: options.operationId }),
      ...(options.ifBasisTx === undefined
        ? {}
        : { ifBasisTx: options.ifBasisTx }),
    });
  }

  excise(
    ref: EntityRef,
    options: { operationId?: string; ifBasisTx?: number | bigint } = {},
  ): TxReport {
    this.#ensureWritable();
    return this.#atomic(() => {
      const basis = this.#latestBasis();
      this.#validateOperationId(options.operationId);
      const requestHash = digest({ operation: "excise", ref });
      if (options.operationId !== undefined) {
        const duplicate = this.#duplicateOperation(
          options.operationId,
          requestHash,
        );
        if (duplicate !== null) return duplicate;
      }
      if (
        options.ifBasisTx !== undefined &&
        asBigInt(options.ifBasisTx, "ifBasisTx") !== basis
      )
        throw new Conflict(
          `basis changed from ${String(options.ifBasisTx)} to ${basis}; reread and retry the excision`,
        );
      const entity = this._resolveRead(ref) as bigint;
      const transactionEntity = this._connection
        .prepare("SELECT 1 FROM fgraph_facts WHERE e=? AND a=1 AND tx=e")
        .get(entity);
      if (entity <= GENESIS_TX || transactionEntity !== undefined)
        throw new Unsupported(
          `entity ${String(this._nameOrId(entity))} is a system/transaction entity and cannot be excised; excise only application entities`,
        );
      const priorExcision = this._connection
        .prepare<[bigint]>(
          "SELECT 1 FROM fgraph_facts WHERE a=11 AND t=0 AND v=? AND rx IS NULL LIMIT 1",
        )
        .get(entity);
      if (priorExcision !== undefined)
        throw new Conflict(
          `entity ${String(this._nameOrId(entity))} was already excised under another operation; retry the original operation id or keep the existing audit proof`,
        );
      const pending = this.#newPending();
      const transaction = this.#takeId(pending);
      const eventText = this.#eventId(transaction).toLowerCase();
      const event = uuidBytes(eventText);
      this.#insertIdentityRegistry(pending, transaction, event);
      const atValue = this.#nextTimestamp();
      const asserted = [
        this.#insertRawFact(
          transaction,
          1n,
          { tag: INSTANT, stored: atValue, logical: atValue },
          transaction,
        ),
      ];
      const erased = this._connection
        .prepare<[bigint, bigint, bigint], RawRow>(
          "SELECT * FROM fgraph_facts WHERE e=? OR a=? OR (t=0 AND v=?) ORDER BY id",
        )
        .all(entity, entity, entity);
      const redactedTransactions = new Set<bigint>();
      for (const row of erased) {
        redactedTransactions.add(row.tx);
        if (row.rx !== null) redactedTransactions.add(row.rx);
      }
      const selector = this.#identitySelector(entity);
      const retainedEvents = this._connection
        .prepare<[], { tx: bigint }>(
          "SELECT tx FROM fgraph_events WHERE event_data IS NOT NULL ORDER BY tx",
        )
        .all();
      for (const retained of retainedEvents)
        if (
          eventMentionsSelector(this.#eventRecordForTx(retained.tx), selector)
        )
          redactedTransactions.add(retained.tx);
      const redacts = [...redactedTransactions]
        .map((tx) => this.#eventIdForTx(tx))
        .sort(compareUnicode);
      const rendered = erased.map((row) => this._renderRow(row, transaction));
      const deleteFts = this._connection.prepare(
        "DELETE FROM fgraph_fts WHERE rowid=?",
      );
      erased.forEach((row) => deleteFts.run(row.id));
      this._connection
        .prepare("DELETE FROM fgraph_facts WHERE e=? OR a=? OR (t=0 AND v=?)")
        .run(entity, entity, entity);
      asserted.push(
        this.#insertRawFact(
          transaction,
          11n,
          { tag: REF, stored: entity, logical: entity },
          transaction,
        ),
      );
      this.#gcBlobs(erased);
      const clearEventData = this._connection.prepare(
        "UPDATE fgraph_events SET event_data=NULL WHERE tx=?",
      );
      redactedTransactions.forEach((tx) => clearEventData.run(tx));
      const excisionRecord: Record<string, unknown> = {
        fgraph: "event/1",
        event: eventText,
        at: atValue,
        created: [],
        asserted: [],
        retracted: [],
        redacted: true,
        redacts,
      };
      const excisionData = canonicalEventData(excisionRecord);
      this._connection
        .prepare(
          "INSERT INTO fgraph_events(tx,event_hash,event_data,operation_id,request_hash) VALUES (?,?,?,?,?)",
        )
        .run(
          transaction,
          eventHash(excisionData),
          excisionData,
          options.operationId ?? null,
          options.operationId === undefined ? null : requestHash,
        );
      this._connection
        .prepare("UPDATE fgraph_meta SET value=? WHERE key='next_id'")
        .run(pending.nextId);
      return {
        status: "applied",
        event: eventText,
        basis_tx: publicId(basis),
        tx: publicId(transaction),
        at: publicInteger(atValue),
        ids: {},
        asserted: asserted.map((row) => this._renderRow(row)),
        retracted: rendered,
      };
    });
  }

  async backup(path: string): Promise<void> {
    this.#ensureOpen();
    const target = resolve(path);
    if (this.path !== ":memory:" && target === resolve(this.path))
      throw new Conflict(
        `backup destination ${JSON.stringify(path)} is the open database; choose a different path`,
      );
    if (existsSync(target))
      throw new Conflict(
        `backup destination ${JSON.stringify(path)} already exists; choose a new file`,
      );
    const directory = dirname(target);
    const temporary = join(
      directory,
      `.${basename(target)}.${randomUUID()}.fgraph-backup`,
    );
    let published = false;
    try {
      await this._connection.backup(temporary);
      const verification = connect(temporary, { readOnly: true });
      try {
        const report = verification.doctor();
        if (report.ok !== true)
          throw new FormatError(
            `backup verification failed: ${String(report.problems)}`,
          );
      } finally {
        verification.close();
      }
      // Windows FlushFileBuffers requires a handle with write access.
      const descriptor = openSync(temporary, "r+");
      try {
        fsyncSync(descriptor);
      } finally {
        closeSync(descriptor);
      }
      // Hard-link publication is atomic and cannot overwrite a concurrent file.
      linkSync(temporary, target);
      published = true;
      unlinkSync(temporary);
      // Windows has no portable directory fsync. The verified file itself is
      // flushed before atomic hard-link publication, so only POSIX needs this.
      if (process.platform !== "win32") {
        const directoryDescriptor = openSync(directory, "r");
        try {
          fsyncSync(directoryDescriptor);
        } finally {
          closeSync(directoryDescriptor);
        }
      }
    } catch (error) {
      if (existsSync(temporary)) unlinkSync(temporary);
      if (error instanceof Conflict || error instanceof FormatError)
        throw error;
      if (published)
        throw new FormatError(
          `backup ${JSON.stringify(path)} was created but directory durability could not be confirmed`,
        );
      if (existsSync(target))
        throw new Conflict(
          `backup destination ${JSON.stringify(path)} appeared during publication; choose a new file`,
        );
      throw new FormatError(
        `backup destination ${JSON.stringify(path)} cannot be created; choose a writable new path`,
      );
    }
  }

  stats(): Record<string, number> {
    this.#ensureOpen();
    const count = (sql: string, ...parameters: unknown[]): number =>
      asNumber(
        (
          this._connection
            .prepare<unknown[], { value: bigint }>(sql)
            .get(...parameters) as { value: bigint }
        ).value,
      );
    const basis = this._asOf ?? this.#latestBasis();
    const visibility = this._visibility(basis);
    const attributes = this._connection
      .prepare<[bigint], { name: string }>(
        "SELECT name FROM fgraph_ids WHERE name IS NOT NULL AND created_tx<=?",
      )
      .all(basis)
      .filter((row) => ATTRIBUTE_PATTERN.test(row.name)).length;
    return {
      application_id: Number(
        this._connection.pragma("application_id", { simple: true }) as bigint,
      ),
      format_version: Number(
        this._connection.pragma("user_version", { simple: true }) as bigint,
      ),
      entities: count(
        "SELECT count(*) value FROM fgraph_ids i WHERE created_tx<=? AND NOT EXISTS (SELECT 1 FROM fgraph_events event WHERE event.tx=i.id)",
        basis,
      ),
      attributes,
      facts: count(
        "SELECT count(*) value FROM fgraph_facts WHERE tx<=?",
        basis,
      ),
      live_facts: count(
        `SELECT count(*) value FROM fgraph_facts WHERE ${visibility.sql}`,
        ...visibility.params,
      ),
      transactions: count(
        "SELECT count(*) value FROM fgraph_events WHERE tx<=?",
        basis,
      ),
      blobs: count(
        "SELECT count(DISTINCT v) value FROM fgraph_facts WHERE t IN (7,8,9) AND tx<=?",
        basis,
      ),
      size: this.path === ":memory:" ? 0 : statSync(this.path).size,
    };
  }

  attributes(
    prefix?: string,
    options: { includeSystem?: boolean } = {},
  ): AttributeInfo[] {
    this.#ensureOpen();
    const includeSystem = options.includeSystem ?? false;
    if (typeof includeSystem !== "boolean")
      throw new TypeError(
        `includeSystem=${String(includeSystem)} is invalid; use a boolean`,
      );
    const visibility = this._visibility();
    const result: AttributeInfo[] = [];
    const identityVisibility =
      this._asOf === null
        ? "name IS NOT NULL"
        : "name IS NOT NULL AND created_tx<=?";
    for (const row of this._connection
      .prepare<unknown[], IdRow>(
        `SELECT id,name,gid,created_tx FROM fgraph_ids WHERE ${identityVisibility} ORDER BY name`,
      )
      .all(...(this._asOf === null ? [] : [this._asOf]))) {
      const name = row.name as string;
      if (
        !ATTRIBUTE_PATTERN.test(name) ||
        (!includeSystem && name.startsWith("fgraph/")) ||
        (prefix !== undefined && !name.startsWith(prefix))
      )
        continue;
      const observed = this._connection
        .prepare<unknown[], { t: bigint; facts: bigint }>(
          `SELECT t,count(*) facts FROM fgraph_facts WHERE a=? AND ${visibility.sql} GROUP BY t`,
        )
        .all(row.id, ...visibility.params);
      const schema = this._schema(row.id);
      const types = new Set(observed.map((item) => typeName(Number(item.t))));
      if (schema.type !== null) types.add(schema.type);
      const description: AttributeInfo = {
        name,
        types: [...types].sort(),
        facts: observed.reduce(
          (total, item) => total + asNumber(item.facts),
          0,
        ),
        many: schema.many,
        unique: schema.unique,
        nohistory: deletesHistory(schema),
      };
      if (schema.dims !== null) description.dims = schema.dims;
      if (schema.doc !== null) description.doc = schema.doc;
      result.push(description);
    }
    return result;
  }

  schema(
    prefix?: string,
    options: { includeSystem?: boolean } = {},
  ): SchemaSnapshot {
    this.#ensureOpen();
    const basis = this._asOf ?? this.#latestBasis();
    const visibility = this._visibility(basis);
    const includeSystem = options.includeSystem ?? false;
    if (typeof includeSystem !== "boolean")
      throw new TypeError("includeSystem must be a boolean");
    const rows = this._connection
      .prepare<[bigint], IdRow>(
        "SELECT id,name,gid,created_tx FROM fgraph_ids " +
          "WHERE name IS NOT NULL AND created_tx<=? ORDER BY name",
      )
      .all(basis)
      .filter((row) => {
        const name = row.name as string;
        return (
          ATTRIBUTE_PATTERN.test(name) &&
          (includeSystem || !name.startsWith("fgraph/")) &&
          (prefix === undefined || name.startsWith(prefix))
        );
      });
    const field = new Map<number, string>([
      [5, "many"],
      [6, "unique"],
      [7, "nohistory"],
      [8, "type"],
      [9, "dims"],
      [10, "doc"],
      [14, "vector_model"],
    ]);
    const attributes = rows.map((row) => {
      const declared: Record<string, unknown> = {};
      const declarations = this._connection
        .prepare<unknown[], RawRow>(
          `SELECT * FROM fgraph_facts WHERE e=? AND (a BETWEEN 5 AND 10 OR a=14) AND ${visibility.sql} ORDER BY id`,
        )
        .all(row.id, ...visibility.params);
      for (const declaration of declarations) {
        const key = field.get(Number(declaration.a));
        if (key !== undefined) {
          const logical = this._logical(Number(declaration.t), declaration.v);
          // Dimensions are a bounded public number in every runtime; do not
          // leak better-sqlite3's internal bigint representation.
          declared[key] = key === "dims" ? Number(logical) : logical;
        }
      }
      const effective = this._schema(row.id, basis);
      const observed = this._connection
        .prepare<unknown[], { t: bigint; facts: bigint }>(
          `SELECT t,count(*) facts FROM fgraph_facts WHERE a=? AND ${visibility.sql} GROUP BY t ORDER BY t`,
        )
        .all(row.id, ...visibility.params);
      const observedEntities = this._connection
        .prepare<unknown[], { entities: bigint }>(
          `SELECT count(DISTINCT e) entities FROM fgraph_facts WHERE a=? AND ${visibility.sql}`,
        )
        .get(row.id, ...visibility.params) as { entities: bigint };
      return {
        name: row.name as string,
        declared,
        effective: {
          type: effective.type,
          many: effective.many,
          unique: effective.unique,
          nohistory: deletesHistory(effective),
          dims: effective.dims,
          doc: effective.doc,
          vector_model: effective.vectorModel,
        },
        observed: {
          types: [
            ...new Set(observed.map((item) => typeName(Number(item.t)))),
          ].sort(),
          live_facts: observed.reduce(
            (total, item) => total + asNumber(item.facts),
            0,
          ),
          entities: asNumber(observedEntities.entities),
        },
      };
    });
    const shapeEntities = this._connection
      .prepare<unknown[], { e: bigint }>(
        `SELECT DISTINCT e FROM fgraph_facts WHERE a BETWEEN 16 AND 18 AND ${visibility.sql} ORDER BY e`,
      )
      .all(...visibility.params);
    const shapes = shapeEntities.map(({ e }) => {
      const definitions = this._connection
        .prepare<unknown[], RawRow>(
          `SELECT * FROM fgraph_facts WHERE e=? AND a BETWEEN 16 AND 18 AND ${visibility.sql} ORDER BY a,id`,
        )
        .all(e, ...visibility.params);
      const refs = (attribute: bigint): string[] =>
        definitions
          .filter((row) => row.a === attribute)
          .map((row) => String(this._nameOrId(row.v as bigint)))
          .sort();
      return {
        name: this._nameOrId(e),
        required: refs(16n),
        allowed: refs(17n),
        closed: definitions.some(
          (row) =>
            row.a === 18n && this._logical(Number(row.t), row.v) === true,
        ),
      };
    });
    const schemaDigest = digest({
      attributes: attributes.map(({ name, declared, effective }) => ({
        name,
        declared,
        effective,
      })),
      shapes,
    }).toString("hex");
    return {
      basis_tx: publicId(basis),
      digest: `sha256:${schemaDigest}`,
      attributes,
      shapes,
    };
  }

  #normalizeSchemaManifest(manifest: unknown): SchemaManifest {
    if (!isRecord(manifest) || manifest.fgraph !== "schema/1")
      throw new SchemaError(
        "schema manifest must be an object with fgraph='schema/1'",
      );
    const allowedTopLevel = new Set([
      "fgraph",
      "digest",
      "attributes",
      "shapes",
    ]);
    if (Object.keys(manifest).some((key) => !allowedTopLevel.has(key)))
      throw new SchemaError("schema manifest has unknown fields");
    const rawAttributes = manifest.attributes ?? [];
    const rawShapes = manifest.shapes ?? [];
    if (!Array.isArray(rawAttributes) || !Array.isArray(rawShapes))
      throw new SchemaError(
        "schema manifest attributes and shapes must be arrays",
      );
    const fields = new Set([
      "type",
      "many",
      "unique",
      "nohistory",
      "dims",
      "doc",
      "vector_model",
    ]);
    const seenAttributes = new Set<string>();
    const attributes = rawAttributes.map((item) => {
      if (
        !isRecord(item) ||
        Object.keys(item).sort().join(",") !== "declared,name" ||
        typeof item.name !== "string" ||
        !isRecord(item.declared)
      )
        throw new SchemaError(
          "schema manifest attributes need exactly name and declared",
        );
      this.#validateAttribute(item.name);
      if (seenAttributes.has(item.name))
        throw new SchemaError(
          `schema manifest repeats attribute ${JSON.stringify(item.name)}`,
        );
      seenAttributes.add(item.name);
      if (Object.keys(item.declared).some((key) => !fields.has(key)))
        throw new SchemaError(
          `schema manifest attribute ${JSON.stringify(item.name)} has an unknown declaration field`,
        );
      const declared = { ...item.declared };
      if (
        Object.hasOwn(declared, "type") &&
        !TYPE_NAMES.has(declared.type as string)
      )
        throw new SchemaError("schema manifest has an unsupported type");
      for (const field of ["many", "unique", "nohistory"])
        if (
          Object.hasOwn(declared, field) &&
          typeof declared[field] !== "boolean"
        )
          throw new SchemaError(
            `schema manifest field ${JSON.stringify(field)} must be boolean`,
          );
      if (
        Object.hasOwn(declared, "dims") &&
        (!Number.isSafeInteger(declared.dims) || Number(declared.dims) <= 0)
      )
        throw new SchemaError(
          "schema manifest dims must be a positive integer",
        );
      for (const field of ["doc", "vector_model"])
        if (
          Object.hasOwn(declared, field) &&
          typeof declared[field] !== "string"
        )
          throw new SchemaError(
            `schema manifest field ${JSON.stringify(field)} must be text`,
          );
      if (
        typeof declared.vector_model === "string" &&
        declared.vector_model.trim() === ""
      )
        throw new SchemaError("schema manifest vector_model must be non-blank");
      return { name: item.name, declared };
    });
    const seenShapes = new Set<string>();
    const shapes = rawShapes.map((item) => {
      if (
        !isRecord(item) ||
        Object.keys(item).sort().join(",") !== "allowed,closed,name,required" ||
        typeof item.name !== "string" ||
        !Array.isArray(item.required) ||
        !Array.isArray(item.allowed) ||
        typeof item.closed !== "boolean" ||
        [...item.required, ...item.allowed].some(
          (value) => typeof value !== "string",
        )
      )
        throw new SchemaError(
          "schema manifest shapes need name, required, allowed, and closed",
        );
      this.#validateName(item.name);
      if (seenShapes.has(item.name))
        throw new SchemaError(
          `schema manifest repeats shape ${JSON.stringify(item.name)}`,
        );
      seenShapes.add(item.name);
      const required = [...new Set(item.required as string[])].sort();
      const allowed = [
        ...new Set(
          item.closed
            ? [...required, ...(item.allowed as string[])]
            : (item.allowed as string[]),
        ),
      ].sort();
      [...required, ...allowed].forEach((attribute) =>
        this.#validateAttribute(attribute),
      );
      return { name: item.name, required, allowed, closed: item.closed };
    });
    const payload = {
      fgraph: "schema/1" as const,
      attributes: attributes
        .filter((attribute) => Object.keys(attribute.declared).length > 0)
        .sort((left, right) => compareUnicode(left.name, right.name)),
      shapes: shapes.sort((left, right) =>
        compareUnicode(left.name, right.name),
      ),
    };
    return {
      ...payload,
      digest: `sha256:${digest(payload).toString("hex")}`,
    };
  }

  schemaManifest(): SchemaManifest {
    const snapshot = this.schema();
    return this.#normalizeSchemaManifest({
      fgraph: "schema/1",
      attributes: snapshot.attributes
        .filter((attribute) => Object.keys(attribute.declared).length > 0)
        .map((attribute) => ({
          name: attribute.name,
          declared: attribute.declared,
        })),
      shapes: snapshot.shapes,
    });
  }

  checkSchemaManifest(manifest: unknown): SchemaManifestCheck {
    const desired = this.#normalizeSchemaManifest(manifest);
    const current = this.schemaManifest();
    const entries = (value: SchemaManifest): Map<string, unknown> => {
      const result = new Map<string, unknown>();
      value.attributes.forEach((item) =>
        result.set(`attribute:${item.name}`, item.declared),
      );
      value.shapes.forEach((item) => result.set(`shape:${item.name}`, item));
      return result;
    };
    const before = entries(current);
    const after = entries(desired);
    const keys = [...new Set([...before.keys(), ...after.keys()])].sort();
    const changes = keys
      .filter((key) => {
        const beforeValue = before.get(key);
        const afterValue = after.get(key);
        return beforeValue === undefined || afterValue === undefined
          ? beforeValue !== afterValue
          : canonicalJson(beforeValue) !== canonicalJson(afterValue);
      })
      .map((key) => {
        const [kind, name] = key.split(":", 2) as [
          "attribute" | "shape",
          string,
        ];
        return {
          kind,
          name,
          before: before.get(key) ?? null,
          after: after.get(key) ?? null,
        };
      });
    return {
      basis_tx: this._basisTx(),
      valid: changes.length === 0,
      current_digest: current.digest,
      desired_digest: desired.digest,
      changes,
    };
  }

  applySchemaManifest(
    manifest: unknown,
    options: Pick<TransactOptions, "operationId" | "ifBasisTx"> = {},
  ): TxReport {
    const desired = this.#normalizeSchemaManifest(manifest);
    const schemaFields = new Map([
      ["many", "fgraph/many"],
      ["unique", "fgraph/unique"],
      ["nohistory", "fgraph/nohistory"],
      ["type", "fgraph/type"],
      ["dims", "fgraph/dims"],
      ["doc", "fgraph/doc"],
      ["vector_model", "fgraph/vector-model"],
    ]);
    return this.transact([], {
      ...options,
      _requestHashOverride: digest({
        operation: "schema-apply",
        manifest: desired,
      }),
      _prepareData: () => {
        // Full replacement discovery must share the writer transaction with
        // planning and commit so a concurrent declaration cannot survive it.
        const current = this.schemaManifest();
        const attributeNames = new Set(
          [...current.attributes, ...desired.attributes].map(
            (item) => item.name,
          ),
        );
        const operations: unknown[] = [];
        for (const name of [...attributeNames].sort())
          for (const systemAttribute of schemaFields.values())
            operations.push(["retract", name, systemAttribute]);
        for (const attribute of desired.attributes) {
          const definition: Record<string, unknown> = { id: attribute.name };
          for (const [field, value] of Object.entries(attribute.declared))
            definition[schemaFields.get(field) as string] = value;
          operations.push(definition);
        }
        const shapeNames = new Set(
          [...current.shapes, ...desired.shapes].map((item) => item.name),
        );
        for (const name of [...shapeNames].sort())
          for (const systemAttribute of [
            "fgraph/shape-required",
            "fgraph/shape-allowed",
            "fgraph/shape-closed",
          ])
            operations.push(["retract", name, systemAttribute]);
        for (const shape of desired.shapes) {
          const definition: Record<string, unknown> = {
            id: shape.name,
            "fgraph/shape-closed": shape.closed,
          };
          if (shape.required.length > 0)
            definition["fgraph/shape-required"] = shape.required.map((ref) => ({
              ref,
            }));
          if (shape.allowed.length > 0)
            definition["fgraph/shape-allowed"] = shape.allowed.map((ref) => ({
              ref,
            }));
          operations.push(definition);
        }
        return operations;
      },
    });
  }

  #expectedFtsRows(): Array<{ id: bigint; text: string }> {
    return this._connection
      .prepare<[], RawRow>(
        "SELECT * FROM fgraph_facts WHERE rx IS NULL AND t IN (4,8) ORDER BY id",
      )
      .all()
      .map((row) => ({
        id: row.id,
        text: String(this._logical(Number(row.t), row.v)),
      }));
  }

  #doctorReport(): { report: Record<string, unknown>; fatal: string[] } {
    const integrityRows = this._connection
      .prepare<[], { integrity_check: string }>("PRAGMA integrity_check")
      .all()
      .map((row) => row.integrity_check);
    const integrity = integrityRows[0] as string;
    const fatal = integrityRows
      .filter((message) => message !== "ok")
      .map((message) => `integrity_check: ${message}`);
    const metadata = new Map(
      this._connection
        .prepare<[], { key: string; value: unknown }>(
          "SELECT key,value FROM fgraph_meta",
        )
        .all()
        .map((row) => [row.key, row.value]),
    );
    const maximum =
      this._connection
        .prepare<[], { value: bigint | null }>(
          "SELECT max(identifier) value FROM (SELECT id identifier FROM fgraph_ids UNION ALL SELECT e FROM fgraph_facts " +
            "UNION ALL SELECT a FROM fgraph_facts UNION ALL SELECT tx FROM fgraph_facts UNION ALL " +
            "SELECT rx FROM fgraph_facts WHERE rx IS NOT NULL UNION ALL SELECT CAST(v AS INTEGER) FROM fgraph_facts WHERE t=0)",
        )
        .get()?.value ?? GENESIS_TX;
    const nextId = metadata.get("next_id");
    if (
      maximum >= INT64_MAX ||
      (typeof nextId === "bigint" && nextId >= INT64_MAX)
    ) {
      fatal.push(
        `next_id: identifier space exhausted at signed 64-bit maximum ${INT64_MAX}`,
      );
    } else {
      const expectedNextId = maximum + 1n;
      if (nextId !== expectedNextId)
        fatal.push(
          `next_id: expected ${expectedNextId}, found ${String(nextId)}`,
        );
    }
    const genesis = this._connection
      .prepare<[bigint, bigint], RawRow>(
        "SELECT * FROM fgraph_facts WHERE e=? AND a=? ORDER BY id",
      )
      .all(GENESIS_TX, 1n);
    if (
      genesis.length !== 1 ||
      genesis[0]?.t !== BigInt(INSTANT) ||
      genesis[0].tx !== GENESIS_TX ||
      genesis[0].rx !== null
    ) {
      fatal.push("genesis receipt: expected one live format-v2 self-receipt");
    } else if (metadata.get("created_at") !== genesis[0].v)
      fatal.push(
        `created_at: expected genesis timestamp ${String(genesis[0].v)}`,
      );
    interface GenesisFactRow {
      id: bigint;
      e: bigint;
      a: bigint;
      v: Buffer;
      storage_class: string;
      t: bigint;
      tx: bigint;
      rx: bigint | null;
    }
    const lastGenesisFactId = BigInt(
      1 + SYSTEM_TYPES.length + SYSTEM_DOCS.length + 2,
    );
    const actualGenesisFacts = new Map(
      this._connection
        .prepare<[bigint, bigint, bigint], GenesisFactRow>(
          "SELECT id,e,a,CAST(v AS BLOB) v,typeof(v) storage_class,t,tx,rx FROM fgraph_facts " +
            "WHERE id BETWEEN 2 AND ? OR (tx=? AND id NOT BETWEEN 1 AND ?) ORDER BY id",
        )
        .all(lastGenesisFactId, GENESIS_TX, lastGenesisFactId)
        .map((row) => [row.id, row]),
    );
    let invalidGenesisFacts = 0;
    const matchesGenesisFact = (
      factId: bigint,
      entity: bigint,
      attribute: bigint,
      value: string,
    ): void => {
      const fact = actualGenesisFacts.get(factId);
      if (
        fact === undefined ||
        fact.e !== entity ||
        fact.a !== attribute ||
        !fact.v.equals(Buffer.from(value)) ||
        fact.storage_class !== "text" ||
        fact.t !== BigInt(TEXT) ||
        fact.tx !== GENESIS_TX ||
        fact.rx !== null
      )
        invalidGenesisFacts++;
      actualGenesisFacts.delete(factId);
    };
    SYSTEM_TYPES.forEach((value, index) =>
      matchesGenesisFact(BigInt(index + 2), BigInt(index + 1), 8n, value),
    );
    SYSTEM_DOCS.forEach((value, index) =>
      matchesGenesisFact(
        BigInt(index + SYSTEM_TYPES.length + 2),
        BigInt(index + 1),
        10n,
        value,
      ),
    );
    for (const [offset, entity] of [16n, 17n].entries()) {
      const factId = BigInt(
        2 + SYSTEM_TYPES.length + SYSTEM_DOCS.length + offset,
      );
      const fact = actualGenesisFacts.get(factId);
      if (
        fact === undefined ||
        fact.e !== entity ||
        fact.a !== 5n ||
        !fact.v.equals(Buffer.from("1")) ||
        fact.storage_class !== "integer" ||
        fact.t !== BigInt(BOOL) ||
        fact.tx !== GENESIS_TX ||
        fact.rx !== null
      )
        invalidGenesisFacts++;
      actualGenesisFacts.delete(factId);
    }
    invalidGenesisFacts += actualGenesisFacts.size;
    if (invalidGenesisFacts > 0)
      fatal.push(`invalid genesis facts: ${invalidGenesisFacts}`);
    const scalar = (sql: string): number =>
      asNumber(
        (
          this._connection.prepare<[], { value: bigint }>(sql).get() as {
            value: bigint;
          }
        ).value,
      );
    const invalidIdentities = asNumber(
      (
        this._connection
          .prepare<[bigint, bigint], { value: bigint }>(
            "SELECT count(*) value FROM fgraph_ids WHERE id<=0 OR (id>? AND id<?)",
          )
          .get(BigInt(SYSTEM_NAMES.length), GENESIS_TX) as { value: bigint }
      ).value,
    );
    if (invalidIdentities > 0)
      fatal.push(`invalid identity ids: ${invalidIdentities}`);
    const actualSystemNames = new Map(
      this._connection
        .prepare<
          [bigint],
          { id: bigint; name: Buffer; gid: Buffer | null; created_tx: bigint }
        >(
          "SELECT id,CAST(name AS BLOB) name,gid,created_tx FROM fgraph_ids WHERE id BETWEEN 1 AND ? ORDER BY id",
        )
        .all(BigInt(SYSTEM_NAMES.length))
        .map((row) => [row.id, row]),
    );
    const invalidSystemIdentities = SYSTEM_NAMES.filter((expected, index) => {
      const actual = actualSystemNames.get(BigInt(index + 1));
      return (
        actual === undefined ||
        !actual.name.equals(Buffer.from(expected)) ||
        actual.gid !== null ||
        actual.created_tx !== GENESIS_TX
      );
    }).length;
    if (invalidSystemIdentities > 0)
      fatal.push(`invalid system identities: ${invalidSystemIdentities}`);
    const genesisIdentity = this._connection
      .prepare<
        [bigint],
        { name: string | null; gid: Buffer | null; created_tx: bigint }
      >("SELECT name,gid,created_tx FROM fgraph_ids WHERE id=?")
      .get(GENESIS_TX);
    if (
      genesisIdentity === undefined ||
      genesisIdentity.name !== null ||
      genesisIdentity.gid === null ||
      !genesisIdentity.gid.equals(uuidBytes(GENESIS_EVENT)) ||
      genesisIdentity.created_tx !== GENESIS_TX
    )
      fatal.push(
        "genesis identity: expected the canonical event UUID registry row",
      );
    const malformedIdentities = scalar(
      "SELECT count(*) value FROM fgraph_ids i WHERE " +
        "((i.name IS NULL) = (i.gid IS NULL)) OR " +
        "(i.gid IS NOT NULL AND (typeof(i.gid)<>'blob' OR length(i.gid)<>16)) OR " +
        "i.created_tx<i.id OR NOT EXISTS (SELECT 1 FROM fgraph_events ev WHERE ev.tx=i.created_tx) OR " +
        "(i.id=i.created_tx AND i.name IS NOT NULL)",
    );
    if (malformedIdentities > 0)
      fatal.push(`malformed identity registry rows: ${malformedIdentities}`);
    let invalidDerivedIdentities = 0;
    const creationTransactions = this._connection
      .prepare<[], { tx: bigint; gid: Buffer }>(
        "SELECT ev.tx,i.gid FROM fgraph_events ev JOIN fgraph_ids i ON i.id=ev.tx ORDER BY ev.tx",
      )
      .all();
    for (const creation of creationTransactions) {
      const created = this._connection
        .prepare<[bigint, bigint], { name: string | null; gid: Buffer | null }>(
          "SELECT name,gid FROM fgraph_ids WHERE created_tx=? AND id<>? ORDER BY id",
        )
        .all(creation.tx, creation.tx);
      created.forEach((identity, ordinal) => {
        if (
          identity.name === null &&
          (identity.gid === null ||
            !identity.gid.equals(
              derivedEntityId(creation.gid, BigInt(ordinal)),
            ))
        )
          invalidDerivedIdentities++;
      });
    }
    if (invalidDerivedIdentities > 0)
      fatal.push(
        `anonymous identities with invalid derived UUIDs: ${invalidDerivedIdentities}`,
      );
    const invalidFactIdentifiers = asNumber(
      (
        this._connection
          .prepare<[bigint, bigint], { value: bigint }>(
            "SELECT count(*) value FROM fgraph_facts WHERE id<=0 OR e<=0 OR a<=0 OR tx<? " +
              "OR (rx IS NOT NULL AND rx<?) OR (t=0 AND CAST(v AS INTEGER)<=0)",
          )
          .get(GENESIS_TX, GENESIS_TX) as { value: bigint }
      ).value,
    );
    if (invalidFactIdentifiers > 0)
      fatal.push(`invalid fact identifiers: ${invalidFactIdentifiers}`);
    const missingIdentityReferences = scalar(
      "SELECT count(*) value FROM fgraph_facts f " +
        "LEFT JOIN fgraph_ids ie ON ie.id=f.e " +
        "LEFT JOIN fgraph_ids ia ON ia.id=f.a " +
        "LEFT JOIN fgraph_ids itx ON itx.id=f.tx " +
        "LEFT JOIN fgraph_ids irx ON irx.id=f.rx " +
        "LEFT JOIN fgraph_ids iv ON iv.id=CAST(f.v AS INTEGER) AND f.t=0 " +
        "WHERE ie.id IS NULL OR ia.id IS NULL OR itx.id IS NULL OR " +
        "(f.rx IS NOT NULL AND irx.id IS NULL) OR (f.t=0 AND iv.id IS NULL)",
    );
    if (missingIdentityReferences > 0)
      fatal.push(
        `facts reference missing identities: ${missingIdentityReferences}`,
      );
    const futureIdentityReferences = scalar(
      "SELECT count(*) value FROM fgraph_facts f " +
        "JOIN fgraph_ids ie ON ie.id=f.e " +
        "JOIN fgraph_ids ia ON ia.id=f.a " +
        "LEFT JOIN fgraph_ids iv ON iv.id=CAST(f.v AS INTEGER) AND f.t=0 " +
        "WHERE ie.created_tx>f.tx OR ia.created_tx>f.tx OR " +
        "(f.t=0 AND iv.created_tx>f.tx)",
    );
    if (futureIdentityReferences > 0)
      fatal.push(
        `facts reference identities before creation: ${futureIdentityReferences}`,
      );
    const namedTransactions = scalar(
      "SELECT count(*) value FROM fgraph_ids i WHERE i.name IS NOT NULL AND EXISTS (SELECT 1 FROM fgraph_facts f " +
        "WHERE f.e=i.id AND f.a=1 AND f.tx=f.e AND f.rx IS NULL)",
    );
    if (namedTransactions > 0)
      fatal.push(
        `named identities overlap transaction receipts: ${namedTransactions}`,
      );
    const missingTransactions = scalar(
      "SELECT count(*) value FROM fgraph_facts f WHERE NOT EXISTS (SELECT 1 FROM fgraph_facts receipt " +
        "WHERE receipt.e=f.tx AND receipt.a=1 AND receipt.tx=receipt.e AND receipt.rx IS NULL)",
    );
    if (missingTransactions > 0)
      fatal.push(
        `facts reference missing asserting transactions: ${missingTransactions}`,
      );
    const missingRetractions = scalar(
      "SELECT count(*) value FROM fgraph_facts f WHERE f.rx IS NOT NULL AND NOT EXISTS " +
        "(SELECT 1 FROM fgraph_facts receipt WHERE receipt.e=f.rx AND receipt.a=1 " +
        "AND receipt.tx=receipt.e AND receipt.rx IS NULL)",
    );
    if (missingRetractions > 0)
      fatal.push(
        `facts reference missing retracting transactions: ${missingRetractions}`,
      );
    const eventWithoutIdentity = scalar(
      "SELECT count(*) value FROM fgraph_events ev LEFT JOIN fgraph_ids i ON i.id=ev.tx " +
        "WHERE i.id IS NULL OR i.name IS NOT NULL OR i.gid IS NULL OR i.created_tx<>ev.tx",
    );
    if (eventWithoutIdentity > 0)
      fatal.push(
        `transaction events without matching identities: ${eventWithoutIdentity}`,
      );
    const eventWithoutReceipt = scalar(
      "SELECT count(*) value FROM fgraph_events ev WHERE NOT EXISTS " +
        "(SELECT 1 FROM fgraph_facts f WHERE f.e=ev.tx AND f.a=1 AND f.tx=ev.tx AND f.rx IS NULL)",
    );
    if (eventWithoutReceipt > 0)
      fatal.push(
        `transaction events without live time receipts: ${eventWithoutReceipt}`,
      );
    const receiptWithoutEvent = scalar(
      "SELECT count(*) value FROM fgraph_facts f WHERE f.e=f.tx AND f.a=1 AND f.rx IS NULL " +
        "AND NOT EXISTS (SELECT 1 FROM fgraph_events ev WHERE ev.tx=f.tx)",
    );
    if (receiptWithoutEvent > 0)
      fatal.push(`transaction receipts without events: ${receiptWithoutEvent}`);
    const malformedEvents = scalar(
      "SELECT count(*) value FROM fgraph_events WHERE " +
        "typeof(event_hash)<>'blob' OR length(event_hash)<>32 OR " +
        "(event_data IS NOT NULL AND typeof(event_data)<>'text') OR " +
        "((operation_id IS NULL)<>(request_hash IS NULL)) OR " +
        "(request_hash IS NOT NULL AND (typeof(request_hash)<>'blob' OR length(request_hash)<>32)) OR " +
        "(operation_id IS NOT NULL AND (length(CAST(operation_id AS BLOB))<1 OR length(CAST(operation_id AS BLOB))>512))",
    );
    if (malformedEvents > 0)
      fatal.push(`malformed event receipts: ${malformedEvents}`);
    const excisionTransactions = new Set(
      this._connection
        .prepare<[], { tx: bigint }>(
          "SELECT DISTINCT e tx FROM fgraph_facts WHERE a=11 AND tx=e AND rx IS NULL",
        )
        .all()
        .map((row) => row.tx),
    );
    const eventRows = this._connection
      .prepare<
        [],
        {
          tx: bigint;
          gid: Buffer | null;
          event_hash: Buffer;
          event_data: string | null;
        }
      >(
        "SELECT ev.tx,i.gid,ev.event_hash,ev.event_data FROM fgraph_events ev " +
          "JOIN fgraph_ids i ON i.id=ev.tx ORDER BY ev.tx",
      )
      .all();
    const eventsById = new Map<string, (typeof eventRows)[number]>();
    for (const row of eventRows)
      if (Buffer.isBuffer(row.gid) && row.gid.length === 16)
        eventsById.set(uuidText(row.gid), row);
    const nullEvents = new Set<string>();
    const redactedTargets = new Set<string>();
    let unverifiableEventHashes = 0;
    for (const row of eventRows) {
      if (!Buffer.isBuffer(row.gid) || row.gid.length !== 16) {
        fatal.push(`event ${row.tx} has no canonical event identity`);
        continue;
      }
      const expectedEvent = uuidText(row.gid);
      if (row.event_data === null) {
        nullEvents.add(expectedEvent);
        unverifiableEventHashes++;
        continue;
      }
      try {
        if (Buffer.byteLength(row.event_data, "utf8") > MAX_EVENT_BYTES)
          throw new FormatError(
            "event payload exceeds the portable size limit",
          );
        const record = parseJson(row.event_data, `stored event ${row.tx}`);
        if (
          !isRecord(record) ||
          record.fgraph !== "event/1" ||
          record.event !== expectedEvent ||
          canonicalJson(record) !== row.event_data ||
          !eventHash(row.event_data).equals(row.event_hash)
        )
          throw new FormatError(
            "event payload is non-canonical or hash-mismatched",
          );
        if (record.redacted === true) {
          const keys = Object.keys(record).sort().join(",");
          if (
            keys !==
              "asserted,at,created,event,fgraph,redacted,redacts,retracted" ||
            !Array.isArray(record.created) ||
            record.created.length !== 0 ||
            !Array.isArray(record.asserted) ||
            record.asserted.length !== 0 ||
            !Array.isArray(record.retracted) ||
            record.retracted.length !== 0 ||
            !Array.isArray(record.redacts) ||
            !excisionTransactions.has(row.tx)
          )
            throw new FormatError(
              "redacted event is not an audited excision payload",
            );
          const targets = record.redacts.map((value) => {
            if (typeof value !== "string")
              throw new FormatError("redacted event target is not a UUID");
            return uuidText(uuidBytes(value));
          });
          if (
            new Set(targets).size !== targets.length ||
            targets.some(
              (value, index) =>
                value !== [...targets].sort(compareUnicode)[index],
            )
          )
            throw new FormatError(
              "redacted event targets are not unique canonical sort order",
            );
          for (const target of targets) {
            const targetRow = eventsById.get(target);
            if (
              targetRow === undefined ||
              targetRow.tx >= row.tx ||
              targetRow.event_data !== null
            )
              throw new FormatError(
                `redacted event target ${target} is absent, later, or still has payload data`,
              );
            redactedTargets.add(target);
          }
        } else if (
          Object.hasOwn(record, "redacted") ||
          Object.hasOwn(record, "redacts")
        )
          throw new FormatError(
            "ordinary event contains redaction-only fields",
          );
      } catch (error) {
        fatal.push(
          `event ${row.tx} is invalid: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }
    for (const event of nullEvents)
      if (!redactedTargets.has(event))
        fatal.push(
          `event ${event} has no payload and is not named by an audited excision`,
        );
    const dangling = scalar(
      "SELECT count(*) value FROM fgraph_facts f LEFT JOIN fgraph_ids i ON i.id=f.a WHERE i.id IS NULL",
    );
    if (dangling > 0) fatal.push(`dangling attributes: ${dangling}`);
    const physicalRows = this._connection
      .prepare<[], PhysicalValueRow>(
        "SELECT t,typeof(v) storage_class,CASE WHEN t IN (4,10) THEN NULL ELSE v END scalar," +
          "CAST(v AS BLOB) raw FROM fgraph_facts ORDER BY id",
      )
      .all();
    const invalidValueTags = physicalRows.filter(
      (row) => row.t < BigInt(REF) || row.t > BigInt(JSON_TAG),
    ).length;
    const invalidPhysicalValues = physicalRows.filter(
      (row) =>
        row.t >= BigInt(REF) &&
        row.t <= BigInt(JSON_TAG) &&
        !validPhysicalValue(row),
    ).length;
    if (invalidValueTags > 0)
      fatal.push(`invalid value tags: ${invalidValueTags}`);
    if (invalidPhysicalValues > 0)
      fatal.push(`invalid physical values: ${invalidPhysicalValues}`);
    const missingBlobs = scalar(
      "SELECT count(*) value FROM fgraph_facts f LEFT JOIN fgraph_blobs b ON b.hash=f.v WHERE f.t IN (7,8,9) AND b.hash IS NULL",
    );
    if (missingBlobs > 0) fatal.push(`missing blobs: ${missingBlobs}`);
    const indirectRows = this._connection
      .prepare<[], { t: bigint; v: Stored; data: unknown }>(
        "SELECT f.t,f.v,b.data FROM fgraph_facts f " +
          "JOIN fgraph_blobs b ON b.hash=f.v WHERE f.t IN (7,8,9)",
      )
      .all();
    const invalidIndirect = indirectRows.reduce((count, row) => {
      const tag = Number(row.t);
      if (!Buffer.isBuffer(row.v) || row.v.length !== 32) return count + 1;
      if (indirectDataProblem(tag, row.data) !== null) return count + 1;
      return indirectDigest(tag, indirectBytes(tag, row.data)).equals(row.v)
        ? count
        : count + 1;
    }, 0);
    if (invalidIndirect > 0)
      fatal.push(`invalid indirect blobs: ${invalidIndirect}`);
    const invalidIntervals = scalar(
      "SELECT count(*) value FROM fgraph_facts WHERE rx IS NOT NULL AND rx<=tx",
    );
    if (invalidIntervals > 0)
      fatal.push(`invalid transaction intervals: ${invalidIntervals}`);
    let schemaProblems = 0;
    const attributeIds = this._connection
      .prepare<[], { a: bigint }>(
        "SELECT DISTINCT a FROM fgraph_facts ORDER BY a",
      )
      .all();
    for (const { a } of attributeIds) {
      try {
        const schema = this._schema(a);
        const values = this._connection
          .prepare<[bigint], RawRow>(
            "SELECT * FROM fgraph_facts WHERE a=? AND rx IS NULL ORDER BY id",
          )
          .all(a);
        if (
          schema.type !== null &&
          (!TYPE_NAMES.has(schema.type) ||
            values.some(
              (row) =>
                !valueMatches(schema.type, {
                  tag: Number(row.t),
                  stored: row.v,
                  logical: this._logical(Number(row.t), row.v),
                }),
            ))
        )
          schemaProblems++;
        if (!schema.many) {
          const counts = new Map<bigint, number>();
          values.forEach((row) =>
            counts.set(row.e, (counts.get(row.e) ?? 0) + 1),
          );
          if ([...counts.values()].some((count) => count > 1)) schemaProblems++;
        }
        if (schema.unique) {
          if (
            schema.type === null ||
            schema.type === "json" ||
            schema.type === "vector"
          )
            schemaProblems++;
          const owners = new Map<string, Set<bigint>>();
          values.forEach((row) => {
            const key = `${row.t}:${storedKey(row.v)}`;
            const set = owners.get(key) ?? new Set<bigint>();
            set.add(row.e);
            owners.set(key, set);
          });
          if (
            [...owners.values()].some(
              (ownersForValue) => ownersForValue.size > 1,
            )
          )
            schemaProblems++;
        }
        if (
          schema.dims !== null &&
          (schema.type !== "vector" ||
            !Number.isSafeInteger(schema.dims) ||
            schema.dims <= 0 ||
            values.some(
              (row) =>
                Number(row.t) === VECTOR &&
                (this._logical(Number(row.t), row.v) as number[]).length !==
                  schema.dims,
            ))
        )
          schemaProblems++;
        if (schema.vectorModel !== null && schema.type !== "vector")
          schemaProblems++;
      } catch {
        schemaProblems++;
      }
    }
    if (schemaProblems > 0)
      fatal.push(`schema invariants violated: ${schemaProblems}`);
    let shapeViolations = 0;
    const shapedEntities = this._connection
      .prepare<[], { e: bigint }>(
        "SELECT DISTINCT e FROM fgraph_facts WHERE a=15 AND rx IS NULL ORDER BY e",
      )
      .all();
    for (const { e } of shapedEntities) {
      try {
        shapeViolations += this.#shapeViolationsForEntity(e).length;
      } catch {
        shapeViolations++;
      }
    }
    const shapeDefinitions = this._connection
      .prepare<[], { e: bigint }>(
        "SELECT DISTINCT e FROM fgraph_facts WHERE a BETWEEN 16 AND 18 AND rx IS NULL ORDER BY e",
      )
      .all();
    for (const { e } of shapeDefinitions) {
      const rows = this._connection
        .prepare<[bigint], RawRow>(
          "SELECT * FROM fgraph_facts WHERE e=? AND a BETWEEN 16 AND 18 AND rx IS NULL ORDER BY id",
        )
        .all(e);
      const closed = rows.some(
        (row) => row.a === 18n && this._logical(Number(row.t), row.v) === true,
      );
      if (!closed) continue;
      const required = rows
        .filter((row) => row.a === 16n)
        .map((row) => row.v as bigint);
      const allowed = new Set(
        rows.filter((row) => row.a === 17n).map((row) => row.v as bigint),
      );
      shapeViolations += required.filter(
        (attribute) => !allowed.has(attribute),
      ).length;
    }
    if (shapeViolations > 0)
      fatal.push(`shape invariants violated: ${shapeViolations}`);
    const orphaned = scalar(
      "SELECT count(*) value FROM fgraph_blobs WHERE NOT EXISTS (SELECT 1 FROM fgraph_facts WHERE t IN (7,8,9) AND v=fgraph_blobs.hash)",
    );
    const actualFts = this._connection
      .prepare<[], { rowid: bigint; text: string }>(
        "SELECT rowid,text FROM fgraph_fts ORDER BY rowid",
      )
      .all();
    const expectedFtsRows = scalar(
      "SELECT count(*) value FROM fgraph_facts WHERE rx IS NULL AND t IN (4,8)",
    );
    const unsafeValues =
      invalidValueTags > 0 ||
      invalidPhysicalValues > 0 ||
      missingBlobs > 0 ||
      invalidIndirect > 0;
    const expectedFts = unsafeValues ? [] : this.#expectedFtsRows();
    const ftsMismatch =
      unsafeValues ||
      stringifyJson(actualFts) !==
        stringifyJson(
          expectedFts.map((item) => ({ rowid: item.id, text: item.text })),
        );
    const repairProblems: string[] = [];
    if (ftsMismatch)
      repairProblems.push("full-text index differs from live text facts");
    if (orphaned > 0) repairProblems.push(`orphaned blobs: ${orphaned}`);
    const problems = [...fatal, ...repairProblems];
    return {
      report: {
        ok: problems.length === 0,
        integrity,
        problems,
        repair_needed: repairProblems.length > 0,
        repaired: false,
        fts_rows: actualFts.length,
        expected_fts_rows: expectedFtsRows,
        orphaned_blobs: orphaned,
        unverifiable_event_hashes: unverifiableEventHashes,
        schema_problems: schemaProblems,
        shape_violations: shapeViolations,
        fts_rows_rebuilt: 0,
        orphaned_blobs_removed: 0,
      },
      fatal,
    };
  }

  doctor(options: { repair?: boolean } = {}): Record<string, unknown> {
    const repair = options.repair ?? false;
    if (typeof repair !== "boolean")
      throw new TypeError(
        `doctor repair=${String(repair)} is invalid; use a boolean`,
      );
    if (!repair) {
      this.#ensureOpen();
      return this.#doctorReport().report;
    }
    return this.#atomic(() => {
      const checked = this.#doctorReport();
      if (checked.fatal.length > 0)
        throw new FormatError(
          `doctor found non-rebuildable format problems; restore from a valid backup: ${checked.fatal.join("; ")}`,
        );
      const rows = this.#expectedFtsRows();
      this._connection.exec("DELETE FROM fgraph_fts");
      const insert = this._connection.prepare(
        "INSERT INTO fgraph_fts(rowid,text) VALUES (?,?)",
      );
      rows.forEach((row) => insert.run(row.id, row.text));
      const removed = this.#gcBlobs();
      this._connection.exec("ANALYZE");
      const repaired = this.#doctorReport();
      return {
        ...repaired.report,
        repaired: true,
        fts_rows_rebuilt: rows.length,
        orphaned_blobs_removed: removed,
      };
    });
  }

  #portableFactTuple(row: RawRow): unknown[] {
    const logical = this._logical(Number(row.t), row.v);
    const value =
      Number(row.t) === REF
        ? { ref: this.#identitySelector(logical as bigint) }
        : wireValue(Number(row.t), logical, (id) => this._nameOrId(id));
    return [
      this.#identitySelector(row.e),
      this.#identitySelector(row.a),
      value,
      typeName(Number(row.t)),
    ];
  }

  #eventRecordForTx(transaction: bigint): Record<string, unknown> {
    const row = this._connection
      .prepare<
        [bigint],
        { gid: Buffer; event_hash: Buffer; event_data: string | null }
      >(
        "SELECT i.gid,ev.event_hash,ev.event_data FROM fgraph_events ev " +
          "JOIN fgraph_ids i ON i.id=ev.tx WHERE ev.tx=? AND i.name IS NULL",
      )
      .get(transaction);
    if (row === undefined)
      throw new FormatError(
        `transaction ${transaction} lacks its stable event receipt; restore a valid format-v2 file`,
      );
    const event = uuidText(row.gid);
    if (row.event_data === null) {
      const metadata = this.#txMetadata(transaction);
      return {
        fgraph: "event/1",
        event,
        at: metadata.imported_at ?? metadata.at,
        redacted: true,
        event_hash: row.event_hash.toString("hex"),
      };
    }
    const record = parseJson(row.event_data, `stored event ${transaction}`);
    if (
      !isRecord(record) ||
      record.fgraph !== "event/1" ||
      record.event !== event ||
      canonicalJson(record) !== row.event_data ||
      !eventHash(row.event_data).equals(row.event_hash)
    )
      throw new FormatError(
        `transaction ${transaction} has non-canonical or hash-mismatched event data; restore a valid backup`,
      );
    return record;
  }

  eventRecords(
    since: number | bigint = GENESIS_TX,
    through?: number | bigint,
  ): Record<string, unknown>[] {
    this.#ensureOpen();
    const after = asBigInt(since, "event cursor");
    if (after < GENESIS_TX)
      throw new TypeError(
        `event cursor ${after} is invalid; use a transaction id at least ${GENESIS_TX}`,
      );
    let end =
      through === undefined
        ? (this._asOf ?? this.#latestBasis())
        : asBigInt(through, "event through");
    if (end < GENESIS_TX)
      throw new TypeError(
        `event through ${end} is invalid; use a transaction id at least ${GENESIS_TX}`,
      );
    if (this._asOf !== null && end > this._asOf) end = this._asOf;
    return this._connection
      .prepare<[bigint, bigint], { tx: bigint }>(
        "SELECT tx FROM fgraph_events WHERE tx>? AND tx<=? ORDER BY tx",
      )
      .all(after, end)
      .map((row) => this.#eventRecordForTx(row.tx));
  }

  tail(
    since: number | bigint = GENESIS_TX,
    writer?: { write(value: string): unknown },
  ): string | void {
    const output = this.eventRecords(since)
      .map((record) => canonicalJson(record))
      .join("\n");
    const text = output === "" ? "" : `${output}\n`;
    if (writer !== undefined) {
      writer.write(text);
      return;
    }
    return text;
  }

  #applySelector(
    selector: unknown,
    tokens: Map<string, string>,
  ): EntityRef | { tmp: string } {
    if (typeof selector === "string") return selector;
    if (
      !isRecord(selector) ||
      Object.keys(selector).length !== 1 ||
      typeof selector.eid !== "string"
    )
      throw new TypeError(
        "event entity selector must be a name or {eid: canonical-uuid}",
      );
    const gid = uuidBytes(selector.eid);
    const existing = this._connection
      .prepare<[Buffer], { id: bigint }>(
        "SELECT id FROM fgraph_ids WHERE gid=?",
      )
      .get(gid);
    if (existing !== undefined) return publicId(existing.id);
    const canonical = uuidText(gid);
    const token = tokens.get(canonical) ?? `event:${canonical}`;
    tokens.set(canonical, token);
    return { tmp: token };
  }

  #applyValue(
    value: unknown,
    tag: unknown,
    tokens: Map<string, string>,
  ): unknown {
    if (tag === "ref") {
      if (
        !isRecord(value) ||
        Object.keys(value).length !== 1 ||
        !Object.hasOwn(value, "ref")
      )
        throw new TypeError('event ref value must use {"ref":selector}');
      return { ref: this.#applySelector(value.ref, tokens) };
    }
    return this.#taggedWireValue(value, tag);
  }

  apply(source: string | Iterable<string>): TxReport[] {
    return this.#applyEvents(source, true).reports;
  }

  applySummary(source: string | Iterable<string>): {
    events: number;
    applied: number;
    already_applied: number;
    noop: number;
    basis_tx: WireInteger;
  } {
    return this.#applyEvents(source, false).summary;
  }

  #applyEvents(
    source: string | Iterable<string>,
    collectReports: boolean,
  ): {
    reports: TxReport[];
    summary: {
      events: number;
      applied: number;
      already_applied: number;
      noop: number;
      basis_tx: WireInteger;
    };
  } {
    this.#ensureWritable();
    const lines: Iterable<string> =
      typeof source === "string"
        ? (function* eventLines(): Generator<string> {
            let start = 0;
            for (let newline = source.indexOf("\n"); newline >= 0;) {
              const end =
                newline > start && source[newline - 1] === "\r"
                  ? newline - 1
                  : newline;
              yield source.slice(start, end);
              start = newline + 1;
              newline = source.indexOf("\n", start);
            }
            yield source.slice(start);
          })()
        : source;
    return this.#atomic(() => {
      const reports: TxReport[] = [];
      const summary = {
        events: 0,
        applied: 0,
        already_applied: 0,
        noop: 0,
        basis_tx: publicId(this.#latestBasis()),
      };
      const recordReport = (report: TxReport): void => {
        summary.events++;
        summary[report.status]++;
        if (collectReports) reports.push(report);
      };
      let lineNumber = 0;
      for (const raw of lines) {
        lineNumber++;
        if (raw.trim() === "") continue;
        const size = Buffer.byteLength(raw, "utf8");
        if (size > MAX_EVENT_BYTES)
          throw new TooLarge(
            `event line ${lineNumber} is ${size} bytes; keep each event at or below ${MAX_EVENT_BYTES} portable bytes`,
          );
        const record = parseJson(raw, `event line ${lineNumber}`);
        if (!isRecord(record) || record.fgraph !== "event/1")
          throw new TypeError(
            `event line ${lineNumber} must be an fgraph event/1 object`,
          );
        const allowed = new Set([
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
        ]);
        const unknown = Object.keys(record).filter((key) => !allowed.has(key));
        if (unknown.length > 0)
          throw new TypeError(
            `event line ${lineNumber} has unknown fields ${unknown.sort().join(", ")}`,
          );
        if (typeof record.event !== "string")
          throw new TypeError(`event line ${lineNumber} has no UUID event id`);
        const event = uuidBytes(record.event);
        const eventHash = digest(record);
        const existingIdentity = this._connection
          .prepare<[Buffer], { id: bigint }>(
            "SELECT id FROM fgraph_ids WHERE gid=?",
          )
          .get(event);
        if (existingIdentity !== undefined) {
          const existing = this._connection
            .prepare<[bigint], { event_hash: Buffer }>(
              "SELECT event_hash FROM fgraph_events WHERE tx=?",
            )
            .get(existingIdentity.id);
          if (existing === undefined || !existing.event_hash.equals(eventHash))
            throw new Conflict(
              `event ${record.event} collides with another identity or payload`,
            );
          const receipt = this.receipt(existingIdentity.id);
          recordReport({
            status: "already_applied",
            event: record.event,
            basis_tx: receipt.basis_tx as WireInteger,
            tx: publicId(existingIdentity.id),
            at: receipt.at as WireInteger,
            ids: {},
            asserted: [],
            retracted: [],
          });
          continue;
        }
        if (
          (typeof record.at !== "number" && typeof record.at !== "bigint") ||
          !Array.isArray(record.created) ||
          !Array.isArray(record.asserted) ||
          !Array.isArray(record.retracted)
        )
          throw new TypeError(
            `event line ${lineNumber} needs integer at and created/asserted/retracted arrays`,
          );
        const originAt = instantValue(record.at);
        const tokens = new Map<string, string>();
        const data: unknown[] = record.created.map((selector) => ({
          id: this.#applySelector(selector, tokens),
        }));
        const append = (
          kind: "assert" | "retract",
          tuples: unknown[],
        ): void => {
          for (const tuple of tuples) {
            if (
              !Array.isArray(tuple) ||
              tuple.length !== 4 ||
              typeof tuple[1] !== "string"
            )
              throw new TypeError(
                `event line ${lineNumber} ${kind} tuple must be [selector,attribute,value,tag]`,
              );
            data.push([
              kind,
              this.#applySelector(tuple[0], tokens),
              tuple[1],
              this.#applyValue(tuple[2], tuple[3], tokens),
            ]);
          }
        };
        append("retract", record.retracted);
        append("assert", record.asserted);
        const txFacts: Array<[string, unknown]> = [];
        if (record.tx_facts !== undefined) {
          if (!Array.isArray(record.tx_facts))
            throw new TypeError(
              `event line ${lineNumber} tx_facts must be an array`,
            );
          for (const tuple of record.tx_facts) {
            if (
              !Array.isArray(tuple) ||
              tuple.length !== 3 ||
              typeof tuple[0] !== "string"
            )
              throw new TypeError(
                `event line ${lineNumber} tx fact must be [attribute,value,tag]`,
              );
            txFacts.push([
              tuple[0],
              this.#applyValue(tuple[1], tuple[2], tokens),
            ]);
          }
        }
        const options: TransactOptions & {
          _compactReport: boolean;
          _force: boolean;
          _eventId: string;
          _eventHash: Buffer;
          _eventData: string;
          _originAt: bigint;
          _extraTxFacts: Array<[string, unknown]>;
        } = {
          _compactReport: !collectReports,
          _force: true,
          _eventId: record.event,
          _eventHash: eventHash,
          _eventData: canonicalJson(record),
          _originAt: originAt,
          // Preserve the portable tuple order and repeated cardinality-many values.
          _extraTxFacts: [
            ...txFacts,
            [IMPORTED_AT_NAME, { instant: originAt }],
          ],
        };
        if (typeof record.by === "string") options.by = record.by;
        else if (record.by !== undefined)
          throw new TypeError(`event line ${lineNumber} by must be text`);
        if (typeof record.source === "string") options.source = record.source;
        else if (record.source !== undefined)
          throw new TypeError(`event line ${lineNumber} source must be text`);
        if (Object.hasOwn(record, "meta")) options.meta = record.meta;
        const report = this.transact(data, options);
        for (const [eid, token] of tokens) {
          const id = report.ids[token];
          if (id === undefined) continue;
          this._connection
            .prepare("UPDATE fgraph_ids SET gid=? WHERE id=? AND name IS NULL")
            .run(uuidBytes(eid), asBigInt(id));
        }
        recordReport(report);
      }
      summary.basis_tx = publicId(this.#latestBasis());
      return { reports, summary };
    });
  }

  #eventIdForTx(transaction: bigint): string {
    const row = this._connection
      .prepare<[bigint], { gid: Buffer }>(
        "SELECT gid FROM fgraph_ids WHERE id=? AND name IS NULL",
      )
      .get(transaction);
    if (row === undefined)
      throw new FormatError(
        `transaction ${transaction} is missing its event identity`,
      );
    return uuidText(row.gid);
  }

  *snapshotLines(): Generator<string> {
    const read = this.#beginRead();
    try {
      const createdAt = this._connection
        .prepare<[], { value: bigint }>(
          "SELECT value FROM fgraph_meta WHERE key='created_at'",
        )
        .get()?.value;
      if (createdAt === undefined)
        throw new FormatError("snapshot cannot read fgraph_meta.created_at");
      const streamDigest = createHash("sha256");
      const emit = (record: Record<string, unknown>): string => {
        const line = `${canonicalJson(record)}\n`;
        streamDigest.update(line, "utf8");
        return line;
      };
      yield emit({
        fgraph: "snapshot/1",
        format: FORMAT_VERSION,
        created_at: createdAt,
        basis: this.#eventIdForTx(read.basis),
      });
      let receiptCount = 0;
      const receipts = this._connection
        .prepare<
          [bigint, bigint],
          {
            tx: bigint;
            event_hash: Buffer;
            event_data: string | null;
            operation_id: string | null;
            request_hash: Buffer | null;
          }
        >(
          "SELECT tx,event_hash,event_data,operation_id,request_hash FROM fgraph_events WHERE tx>? AND tx<=? ORDER BY tx",
        )
        .iterate(GENESIS_TX, read.basis);
      for (const receipt of receipts) {
        receiptCount++;
        const metadata = this.#txMetadata(receipt.tx);
        const created = this._connection
          .prepare<[bigint, bigint], { id: bigint }>(
            "SELECT id FROM fgraph_ids WHERE created_tx=? AND id<>? ORDER BY id",
          )
          .all(receipt.tx, receipt.tx)
          .map((row) => this.#identitySelector(row.id));
        yield emit({
          receipt: {
            event: this.#eventIdForTx(receipt.tx),
            at: metadata.at,
            origin_at: metadata.imported_at ?? metadata.at,
            event_hash: receipt.event_hash.toString("hex"),
            event_data:
              receipt.event_data === null
                ? null
                : this.#eventRecordForTx(receipt.tx),
            operation_id: receipt.operation_id,
            request_hash: receipt.request_hash?.toString("hex") ?? null,
            created,
          },
        });
      }
      let factCount = 0;
      const facts = this._connection
        .prepare<[bigint, bigint], RawRow>(
          "SELECT * FROM fgraph_facts WHERE tx>? AND tx<=? ORDER BY id",
        )
        .iterate(GENESIS_TX, read.basis);
      for (const row of facts) {
        factCount++;
        const tuple = this.#portableFactTuple(row);
        yield emit({
          fact: [
            tuple[0],
            tuple[1],
            tuple[2],
            tuple[3],
            this.#eventIdForTx(row.tx),
            row.rx === null || row.rx > read.basis
              ? null
              : this.#eventIdForTx(row.rx),
          ],
        });
      }
      yield `${canonicalJson({
        fgraph: "end",
        sha256: streamDigest.digest("hex"),
        receipts: receiptCount,
        facts: factCount,
      })}\n`;
      this.#finishRead(read.owned, true);
    } finally {
      this.#finishRead(read.owned, false);
    }
  }

  snapshot(writer?: { write(value: string): unknown }): string | void {
    const lines = this.snapshotLines();
    if (writer !== undefined) {
      for (const line of lines) writer.write(line);
      return;
    }
    return [...lines].join("");
  }

  #snapshotSelectorKey(selector: unknown): string {
    if (typeof selector === "string") return `name:${selector}`;
    if (
      isRecord(selector) &&
      Object.keys(selector).length === 1 &&
      typeof selector.eid === "string"
    )
      return `eid:${uuidText(uuidBytes(selector.eid))}`;
    throw new TypeError(
      "snapshot identity selector must be a name or {eid: canonical-uuid}",
    );
  }

  restore(source: string | Iterable<string>): void {
    this.#ensureWritable();
    const rawLines =
      typeof source === "string" ? source.split(/\r?\n/u) : [...source];
    const lines = rawLines.filter((line) => line.trim() !== "");
    if (lines.length < 2)
      throw new TypeError(
        "snapshot is truncated; header and footer are required",
      );
    const parsed = lines.map((line, index) =>
      parseJson(line, `snapshot line ${index + 1}`),
    );
    const header = parsed[0];
    const footer = parsed.at(-1);
    if (
      !isRecord(header) ||
      header.fgraph !== "snapshot/1" ||
      header.format !== FORMAT_VERSION ||
      !isRecord(footer) ||
      footer.fgraph !== "end" ||
      typeof footer.sha256 !== "string"
    )
      throw new TypeError(
        "snapshot header/footer is invalid or targets another format version",
      );
    const canonicalBody = parsed
      .slice(0, -1)
      .map((record) => canonicalJson(record));
    const expectedDigest = createHash("sha256")
      .update(`${canonicalBody.join("\n")}\n`, "utf8")
      .digest("hex");
    if (footer.sha256 !== expectedDigest)
      throw new Conflict(
        "snapshot digest does not match its body; reject the truncated or modified stream",
      );
    this.#atomic(() => {
      if (this.#latestBasis() !== GENESIS_TX)
        throw new Conflict(
          "restore requires a pristine database; use apply for an ordered event stream",
        );
      const body = parsed.slice(1, -1) as unknown[];
      const receipts = body
        .filter(
          (record) => isRecord(record) && Object.hasOwn(record, "receipt"),
        )
        .map((record) => record as Record<string, unknown>);
      const factRecords = body
        .filter((record) => isRecord(record) && Object.hasOwn(record, "fact"))
        .map((record) => record as Record<string, unknown>);
      if (
        receipts.length !== Number(footer.receipts) ||
        factRecords.length !== Number(footer.facts) ||
        receipts.length + factRecords.length !== parsed.length - 2
      )
        throw new TypeError(
          "snapshot footer counts or record kinds do not match the body",
        );
      const expectedBasis =
        receipts.length === 0
          ? GENESIS_EVENT
          : (receipts.at(-1)?.receipt as Record<string, unknown> | undefined)
              ?.event;
      if (header.basis !== expectedBasis)
        throw new Conflict(
          "snapshot header basis does not match its final transaction receipt",
        );
      const createdAt = instantValue(header.created_at);
      this._connection
        .prepare("UPDATE fgraph_meta SET value=? WHERE key='created_at'")
        .run(createdAt);
      this._connection
        .prepare("UPDATE fgraph_facts SET v=? WHERE e=? AND a=1 AND tx=?")
        .run(createdAt, GENESIS_TX, GENESIS_TX);
      const genesisData = canonicalEventData(genesisEvent(createdAt));
      this._connection
        .prepare(
          "UPDATE fgraph_events SET event_hash=?,event_data=? WHERE tx=?",
        )
        .run(eventHash(genesisData), genesisData, GENESIS_TX);
      let next = FIRST_USER_ID;
      const identities = new Map<string, bigint>();
      for (const row of this._connection
        .prepare<[], { id: bigint; name: string | null; gid: Buffer | null }>(
          "SELECT id,name,gid FROM fgraph_ids",
        )
        .all()) {
        const selector = row.name ?? { eid: uuidText(row.gid as Buffer) };
        identities.set(this.#snapshotSelectorKey(selector), row.id);
      }
      const events = new Map<string, bigint>();
      const insertIdentity = this._connection.prepare(
        "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,?,?,?)",
      );
      for (const wrapper of receipts) {
        const receipt = wrapper.receipt;
        if (
          !isRecord(receipt) ||
          typeof receipt.event !== "string" ||
          !Array.isArray(receipt.created)
        )
          throw new TypeError("snapshot receipt is malformed");
        const eventText = uuidText(uuidBytes(receipt.event));
        if (events.has(eventText))
          throw new Conflict(`snapshot repeats event ${eventText}`);
        const reserved: Array<[unknown, bigint]> = [];
        for (const selector of receipt.created) {
          const key = this.#snapshotSelectorKey(selector);
          if (identities.has(key))
            throw new Conflict(`snapshot repeats identity ${key}`);
          reserved.push([selector, next++]);
        }
        const transaction = next++;
        for (const [selector, id] of reserved) {
          if (typeof selector === "string")
            insertIdentity.run(id, selector, null, transaction);
          else {
            const eid = (selector as { eid?: unknown }).eid;
            if (typeof eid !== "string")
              throw new TypeError("snapshot EID selector is malformed");
            insertIdentity.run(id, null, uuidBytes(eid), transaction);
          }
          identities.set(this.#snapshotSelectorKey(selector), id);
        }
        const eventBytes = uuidBytes(eventText);
        insertIdentity.run(transaction, null, eventBytes, transaction);
        identities.set(
          this.#snapshotSelectorKey({ eid: eventText }),
          transaction,
        );
        events.set(eventText, transaction);
        if (
          typeof receipt.event_hash !== "string" ||
          !/^[0-9a-f]{64}$/u.test(receipt.event_hash)
        )
          throw new TypeError(
            "snapshot receipt event_hash must be 32-byte lowercase hex",
          );
        const storedEventHash = Buffer.from(receipt.event_hash, "hex");
        let eventData: string | null;
        if (receipt.event_data === null) eventData = null;
        else if (
          isRecord(receipt.event_data) &&
          receipt.event_data.fgraph === "event/1" &&
          receipt.event_data.event === eventText
        )
          eventData = canonicalEventData(receipt.event_data);
        else
          throw new TypeError(
            "snapshot receipt event_data must be its canonical event/1 object or null",
          );
        if (eventData !== null && !eventHash(eventData).equals(storedEventHash))
          throw new Conflict(
            "snapshot receipt event_data does not match event_hash",
          );
        const operationId =
          receipt.operation_id === null ||
          typeof receipt.operation_id === "string"
            ? receipt.operation_id
            : undefined;
        const requestHash =
          receipt.request_hash === null
            ? null
            : typeof receipt.request_hash === "string" &&
                /^[0-9a-f]{64}$/u.test(receipt.request_hash)
              ? Buffer.from(receipt.request_hash, "hex")
              : undefined;
        if (
          operationId === undefined ||
          requestHash === undefined ||
          (operationId === null) !== (requestHash === null)
        )
          throw new TypeError("snapshot operation receipt is malformed");
        this._connection
          .prepare(
            "INSERT INTO fgraph_events(tx,event_hash,event_data,operation_id,request_hash) VALUES (?,?,?,?,?)",
          )
          .run(
            transaction,
            storedEventHash,
            eventData,
            operationId,
            requestHash,
          );
      }
      const resolveSelector = (selector: unknown): bigint => {
        const id = identities.get(this.#snapshotSelectorKey(selector));
        if (id === undefined)
          throw new NotFound("snapshot fact references an unknown identity");
        return id;
      };
      for (const wrapper of factRecords) {
        const tuple = wrapper.fact;
        if (
          !Array.isArray(tuple) ||
          tuple.length !== 6 ||
          typeof tuple[3] !== "string"
        )
          throw new TypeError(
            "snapshot fact must be [e,a,v,tag,assert-event,retract-event]",
          );
        const entity = resolveSelector(tuple[0]);
        const attribute = resolveSelector(tuple[1]);
        if (typeof tuple[4] !== "string")
          throw new TypeError("snapshot assertion event must be a UUID");
        const transaction = events.get(uuidText(uuidBytes(tuple[4])));
        if (transaction === undefined)
          throw new NotFound("snapshot fact assertion event is unknown");
        let value = tuple[2];
        if (tuple[3] === "ref") {
          if (
            !isRecord(value) ||
            Object.keys(value).length !== 1 ||
            !Object.hasOwn(value, "ref")
          )
            throw new TypeError("snapshot ref fact must use {ref:selector}");
          value = { ref: publicId(resolveSelector(value.ref)) };
        } else if (tuple[3] === "float") {
          const number =
            value instanceof JsonFloat ? value.value : Number(value);
          value = new JsonFloat(number);
        }
        const encoded = encode(value, (ref) => asBigInt(ref));
        if (typeName(encoded.tag) !== tuple[3])
          throw new TypeError(
            "snapshot fact value does not match its logical tag",
          );
        const inserted = this.#insertRawFact(
          entity,
          attribute,
          encoded,
          transaction,
        );
        if (tuple[5] !== null) {
          if (typeof tuple[5] !== "string")
            throw new TypeError(
              "snapshot retraction event must be null or UUID",
            );
          const retraction = events.get(uuidText(uuidBytes(tuple[5])));
          if (retraction === undefined || retraction <= transaction)
            throw new Conflict(
              "snapshot retraction event is unknown or not later",
            );
          this._connection
            .prepare("UPDATE fgraph_facts SET rx=? WHERE id=?")
            .run(retraction, inserted.id);
          this._connection
            .prepare("DELETE FROM fgraph_fts WHERE rowid=?")
            .run(inserted.id);
        }
      }
      this._connection
        .prepare("UPDATE fgraph_meta SET value=? WHERE key='next_id'")
        .run(next);
      this.#cacheVersion = -1n;
      this.#refreshCache(true);
      const checked = this.#doctorReport();
      if (checked.fatal.length > 0)
        throw new FormatError(
          `restored snapshot violates format invariants: ${checked.fatal.join("; ")}`,
        );
    });
  }

  #taggedWireValue(value: unknown, tag: unknown): unknown {
    if (typeof tag !== "string" || !TYPE_NAMES.has(tag))
      throw new TypeError(
        `event value tag ${String(tag)} is invalid; use a supported logical type`,
      );
    let translated = value;
    if (tag === "float") {
      const numeric =
        translated instanceof JsonFloat ? translated.value : translated;
      if (
        (typeof numeric !== "number" && typeof numeric !== "bigint") ||
        !Number.isFinite(Number(numeric))
      )
        throw new TypeError(
          `event float value ${String(value)} must be a finite JSON number`,
        );
      // Preserve the explicit tuple tag even when the mathematical value is integral.
      translated = new JsonFloat(Number(numeric));
    }
    const encoded = encode(translated);
    if (typeName(encoded.tag) !== tag)
      throw new TypeError(
        `event value ${String(value)} does not match logical tag ${tag}; use its canonical wire wrapper`,
      );
    return translated;
  }

  search(options: SearchOptions): SearchResult {
    const read = this.#beginRead();
    try {
      const result = runSearch(this, options, publicId(read.basis));
      this.#finishRead(read.owned, true);
      return result;
    } catch (error) {
      this.#finishRead(read.owned, false);
      throw error;
    }
  }

  close(): void {
    if (this.#closed) return;
    if (this.#ownsConnection) this._connection.close();
    this.#closed = true;
  }

  [Symbol.dispose](): void {
    this.close();
  }
}
