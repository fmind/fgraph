#!/usr/bin/env node
import {
  closeSync,
  createReadStream,
  lstatSync,
  openSync,
  readFileSync,
  readSync,
  realpathSync,
} from "node:fs";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

import { FGraphError, FormatError, TooLarge, TypeError } from "./errors.js";
import {
  JsonFloat,
  canonicalJson,
  parseJson,
  stringifyJson,
} from "./jsonio.js";
import type {
  Db,
  DeclareOptions,
  EntityRef,
  TransactOptions,
} from "./store.js";
import {
  GENESIS_TX,
  MAX_EVENT_BYTES,
  MAX_SNAPSHOT_LINE_BYTES,
  connect,
} from "./store.js";
import { INT64_MAX, INT64_MIN } from "./values.js";

const VERSION = "1.2.0";
const DEFAULT_DATABASE_PATH = "facts.fgraph";
const LEGACY_DEFAULT_DATABASE_PATH = "fgraph.db";
const VALUE_OPTIONS: ReadonlySet<string> = new Set([
  "--args",
  "--at",
  "--batch-size",
  "--components",
  "--cursor",
  "--depth",
  "--dims",
  "--doc",
  "--embed-cmd",
  "--expand",
  "--filter",
  "--if-basis-tx",
  "--k",
  "--limit",
  "--operation-id",
  "--operation-id-prefix",
  "--required",
  "--allowed",
  "--since",
  "--source",
  "--text",
  "--text-attribute",
  "--type",
  "--vector",
  "--vector-attribute",
  "--vector-model",
]);
const COMMAND_OPTIONS = {
  init: [],
  info: [],
  add: [
    "--batch-size",
    "--operation-id-prefix",
    "--operation-id",
    "--if-basis-tx",
  ],
  retract: ["--operation-id", "--if-basis-tx"],
  get: ["--depth", "--at"],
  tx: [],
  q: ["--args", "--at"],
  explain: ["--args"],
  datoms: ["--source", "--components", "--cursor", "--limit"],
  search: [
    "--text",
    "--vector",
    "--embed-cmd",
    "--k",
    "--expand",
    "--vector-attribute",
    "--text-attribute",
    "--filter",
  ],
  history: [],
  why: [],
  diff: [],
  declare: [
    "--operation-id",
    "--if-basis-tx",
    "--type",
    "--doc",
    "--dims",
    "--vector-model",
    "--ref",
    "--many",
    "--one",
    "--unique",
    "--not-unique",
    "--nohistory",
    "--history",
  ],
  shape: [
    "--operation-id",
    "--if-basis-tx",
    "--required",
    "--allowed",
    "--closed",
    "--open",
  ],
  validate: [],
  schema: ["--system"],
  "schema-export": [],
  "schema-check": [],
  "schema-apply": ["--operation-id", "--if-basis-tx"],
  apply: [],
  snapshot: [],
  restore: [],
  undo: ["--operation-id", "--if-basis-tx"],
  excise: ["--operation-id", "--if-basis-tx"],
  tail: ["--since", "--follow"],
  backup: [],
  doctor: ["--repair"],
  mcp: ["--write", "--embed-cmd"],
} as const satisfies Record<string, readonly string[]>;

class UsageError extends Error {
  readonly exitCode: number;

  constructor(message: string, exitCode = 2) {
    super(message);
    this.exitCode = exitCode;
  }
}

function usage(message?: string, exitCode = 2): never {
  if (message !== undefined) process.stderr.write(`fgraph: ${message}\n\n`);
  process.stderr.write(
    "Usage: fgraph <command> [--db PATH] [--json] [--query-budget N] ...\n" +
      "Commands: init info add retract get tx q explain datoms search history why diff declare shape validate schema schema-export schema-check schema-apply tail apply snapshot restore undo excise backup doctor mcp version\n",
  );
  throw new UsageError(message ?? "usage requested", exitCode);
}

function isOptionToken(value: string): boolean {
  return value === "--" || value.startsWith("--") || /^-[A-Za-z]/u.test(value);
}

function option(args: string[], name: string): string | undefined {
  let result: string | undefined;
  for (let index = 0; index < args.length; index++) {
    const value = args[index];
    if (value === "--") break;
    if (value === name) {
      const next = args[index + 1];
      if (next === undefined || isOptionToken(next))
        usage(`${name} needs a value`);
      result = next;
      args.splice(index, 2);
      index--;
    } else if (value?.startsWith(`${name}=`)) {
      result = value.slice(name.length + 1);
      args.splice(index, 1);
      index--;
    }
  }
  return result;
}

function options(args: string[], name: string): string[] {
  const result: string[] = [];
  for (let index = 0; index < args.length; index++) {
    const value = args[index];
    if (value === "--") break;
    if (value === name) {
      // validateCommandOptions has already established this value boundary.
      const next = args[index + 1] as string;
      result.push(next);
      args.splice(index, 2);
      index--;
    } else if (value?.startsWith(`${name}=`)) {
      result.push(value.slice(name.length + 1));
      args.splice(index, 1);
      index--;
    }
  }
  return result;
}

function flag(args: string[], name: string): boolean {
  let found = false;
  for (let index = 0; index < args.length;) {
    if (args[index] === "--") break;
    if (args[index] === name) {
      found = true;
      args.splice(index, 1);
    } else {
      index++;
    }
  }
  return found;
}

function removeOptionDelimiter(args: string[]): void {
  const index = args.indexOf("--");
  if (index >= 0) args.splice(index, 1);
}

function validateCommandOptions(command: string, args: string[]): void {
  if (!Object.hasOwn(COMMAND_OPTIONS, command))
    usage(`unknown command ${JSON.stringify(command)}`);
  const allowed = new Set<string>(
    COMMAND_OPTIONS[command as keyof typeof COMMAND_OPTIONS],
  );
  for (let index = 0; index < args.length; index++) {
    const value = args[index] as string;
    if (value === "--") break;
    if (!isOptionToken(value)) continue;
    const equals = value.indexOf("=");
    const name = equals < 0 ? value : value.slice(0, equals);
    if (!allowed.has(name))
      usage(`unknown option ${JSON.stringify(name)} for ${command}`);
    if (!VALUE_OPTIONS.has(name)) {
      if (equals >= 0) usage(`${name} does not take a value`);
      continue;
    }
    if (equals < 0) {
      const next = args[index + 1];
      if (next === undefined || isOptionToken(next))
        usage(`${name} needs a value`);
      index++;
    }
  }
}

function pathEntryExists(path: string, description: string): boolean {
  try {
    lstatSync(path);
    return true;
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT")
      return false;
    const detail = error instanceof Error ? error.message : String(error);
    throw new FormatError(
      `cannot inspect ${description} database path ${JSON.stringify(path)}; check directory permissions: ${detail}`,
    );
  }
}

function resolveImplicitDatabasePath(): string {
  if (!pathEntryExists(LEGACY_DEFAULT_DATABASE_PATH, "legacy"))
    return DEFAULT_DATABASE_PATH;
  if (!pathEntryExists(DEFAULT_DATABASE_PATH, "default"))
    return LEGACY_DEFAULT_DATABASE_PATH;
  try {
    connect(DEFAULT_DATABASE_PATH, { readOnly: true }).close();
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new FormatError(
      `legacy default database ${LEGACY_DEFAULT_DATABASE_PATH} exists and ${DEFAULT_DATABASE_PATH} is not an initialized fgraph database; ` +
        `use --db ${LEGACY_DEFAULT_DATABASE_PATH} to keep using the legacy file or explicitly pass --db ${DEFAULT_DATABASE_PATH} to select the new default: ${detail}`,
    );
  }
  return DEFAULT_DATABASE_PATH;
}

function openSelectedDatabase(
  path: string,
  implicitDatabasePath: boolean,
  readOnly: boolean,
  queryBudget: number,
): Db {
  if (path === "")
    throw new FormatError(
      `database path is empty; pass --db PATH or unset FGRAPH_DB to use ${DEFAULT_DATABASE_PATH}`,
    );
  const selectedPath = implicitDatabasePath
    ? resolveImplicitDatabasePath()
    : path;
  return open(selectedPath, readOnly, queryBudget);
}

function integer(value: string, context: string): number | bigint {
  if (!/^-?[0-9]+$/u.test(value))
    usage(`${context} ${JSON.stringify(value)} is not an integer`);
  const parsed = BigInt(value);
  if (parsed < INT64_MIN || parsed > INT64_MAX)
    throw new TypeError(
      `${context} ${JSON.stringify(value)} is outside signed 64-bit integer range`,
    );
  return parsed >= BigInt(Number.MIN_SAFE_INTEGER) &&
    parsed <= BigInt(Number.MAX_SAFE_INTEGER)
    ? Number(parsed)
    : parsed;
}

function positiveInteger(
  value: string,
  context: string,
  allowZero = false,
): number {
  if (!/^[0-9]+$/u.test(value))
    usage(`${context} ${JSON.stringify(value)} is invalid`);
  const result = Number(value);
  if (!Number.isSafeInteger(result) || result < (allowZero ? 0 : 1))
    usage(`${context} ${JSON.stringify(value)} is invalid`);
  return result;
}

function reference(value: string): EntityRef {
  return /^-?[0-9]+$/u.test(value) ? integer(value, "entity") : value;
}

function readArgument(value: string): string {
  if (value === "-") return readFileSync(0, "utf8");
  if (value.startsWith("@")) {
    try {
      return readFileSync(value.slice(1), "utf8");
    } catch {
      throw new FormatError(
        `input file ${JSON.stringify(value.slice(1))} cannot be read as UTF-8; check the path, permissions, and encoding`,
      );
    }
  }
  return value;
}

function emit(value: unknown, machine: boolean): void {
  process.stdout.write(
    `${machine ? canonicalJson(value) : stringifyJson(value, true)}\n`,
  );
}

function payloads(argument: string, context: string): unknown[] {
  const raw = readArgument(argument);
  if (raw.trim() === "")
    throw new TypeError(
      `${context} is empty; provide JSON inline, via @file, or on stdin with '-'`,
    );
  try {
    return [parseJson(raw.trim(), context)];
  } catch (error) {
    const lines = raw.split(/\r?\n/u).filter((line) => line.trim() !== "");
    if (lines.length <= 1) throw error;
    return lines.map((line, index) =>
      parseJson(line, `${context} line ${index + 1}`),
    );
  }
}

async function* batchPayloads(
  argument: string,
  context: string,
): AsyncGenerator<unknown> {
  if (argument !== "-" && !argument.startsWith("@")) {
    if (argument.trim() === "")
      throw new TypeError(`${context} is empty; provide non-empty NDJSON`);
    yield parseJson(argument, context);
    return;
  }

  const input =
    argument === "-"
      ? process.stdin
      : createReadStream(argument.slice(1), { encoding: "utf8" });
  const lines = createInterface({ input, crlfDelay: Number.POSITIVE_INFINITY });
  let lineNumber = 0;
  let seen = false;
  try {
    for await (const line of lines) {
      lineNumber++;
      if (line.trim() === "") continue;
      seen = true;
      yield parseJson(line, `${context} line ${String(lineNumber)}`);
    }
  } catch (error) {
    if (error instanceof FGraphError) throw error;
    throw new FormatError(
      `${context} cannot be read as UTF-8; check the path, permissions, and encoding`,
    );
  } finally {
    lines.close();
    if (argument !== "-") input.destroy();
  }
  if (!seen)
    throw new TypeError(`${context} is empty; provide non-empty NDJSON`);
}

async function takeBatch(
  payloadIterator: AsyncIterator<unknown>,
  size: number,
): Promise<unknown[]> {
  const batch: unknown[] = [];
  while (batch.length < size) {
    const next = await payloadIterator.next();
    if (next.done === true) break;
    batch.push(next.value);
  }
  return batch;
}

async function addBatches(
  db: Db,
  argument: string,
  batchSize: number,
  transactionOptions: TransactOptions,
  operationIdPrefix: string | undefined,
): Promise<Record<string, unknown>> {
  const payloadIterator = batchPayloads(argument, "add input")[
    Symbol.asyncIterator
  ]();
  try {
    let batch = await takeBatch(payloadIterator, batchSize);
    if (
      transactionOptions.operationId !== undefined ||
      transactionOptions.ifBasisTx !== undefined
    ) {
      const extra = await payloadIterator.next();
      if (extra.done !== true) {
        const name =
          transactionOptions.operationId === undefined
            ? "--if-basis-tx"
            : "--operation-id";
        throw new TypeError(
          `${name} cannot span multiple batches; use idempotent batch operation ids`,
        );
      }
    }

    const summary = {
      batches: 0,
      items: 0,
      applied: 0,
      already_applied: 0,
      noop: 0,
    };
    let last: ReturnType<Db["transact"]> | undefined;
    while (batch.length > 0) {
      last = db.transact(batch, {
        ...transactionOptions,
        ...(operationIdPrefix === undefined
          ? {}
          : {
              operationId: `${operationIdPrefix}:${String(summary.batches).padStart(8, "0")}`,
            }),
      });
      summary.batches++;
      summary.items += batch.length;
      summary[last.status]++;
      batch = await takeBatch(payloadIterator, batchSize);
    }
    // batchPayloads rejects an empty stream, so the first takeBatch either
    // yields work or throws before this point.
    const completed = last as ReturnType<Db["transact"]>;
    return {
      ...summary,
      basis_tx: completed.tx ?? completed.basis_tx,
      tx: completed.tx,
    };
  } finally {
    await payloadIterator.return(undefined);
  }
}

function vector(value: unknown, context: string): number[] {
  if (!Array.isArray(value) || value.length === 0)
    throw new TypeError(`${context} must be a non-empty JSON float array`);
  const result = value.map((item) =>
    item instanceof JsonFloat
      ? item.value
      : typeof item === "bigint"
        ? Number(item)
        : item,
  );
  if (result.some((item) => typeof item !== "number" || !Number.isFinite(item)))
    throw new TypeError(`${context} must contain only finite numbers`);
  return result as number[];
}

function open(path: string, readOnly: boolean, queryBudget: number): Db {
  return connect(path, { readOnly, queryBudget });
}

function mutationOptions(args: string[]): TransactOptions {
  const operationId = option(args, "--operation-id");
  const basis = option(args, "--if-basis-tx");
  return {
    ...(operationId === undefined ? {} : { operationId }),
    ...(basis === undefined ? {} : { ifBasisTx: integer(basis, "basis") }),
  };
}

function inputLines(source: string, maxBytes: number): Iterable<string> {
  return (function* lines(): Generator<string> {
    let descriptor: number | undefined;
    const owned = source !== "-";
    try {
      descriptor = owned ? openSync(source, "r") : 0;
      const buffer = Buffer.allocUnsafe(64 * 1024);
      let decoder = new TextDecoder("utf-8", { fatal: true });
      let pending = "";
      let pendingBytes = 0;
      for (;;) {
        const count = readSync(descriptor, buffer, 0, buffer.length, null);
        if (count === 0) break;
        let start = 0;
        while (start < count) {
          const found = buffer.indexOf(0x0a, start);
          const newline = found >= 0 && found < count ? found : -1;
          const end = newline >= 0 ? newline : count;
          const fragmentBytes = end - start;
          if (fragmentBytes > maxBytes - pendingBytes)
            throw new TooLarge(
              `portable NDJSON payload exceeds ${maxBytes} bytes; split or reduce the encoded record`,
            );
          pendingBytes += fragmentBytes;
          pending += decoder.decode(buffer.subarray(start, end), {
            stream: true,
          });
          if (newline < 0) break;
          pending += decoder.decode();
          const line = pending.endsWith("\r") ? pending.slice(0, -1) : pending;
          pending = "";
          pendingBytes = 0;
          decoder = new TextDecoder("utf-8", { fatal: true });
          start = newline + 1;
          yield line;
        }
      }
      pending += decoder.decode();
      if (pending !== "") yield pending;
    } catch (error) {
      if (error instanceof FGraphError) throw error;
      throw new FormatError(
        `input file ${JSON.stringify(source)} cannot be opened as UTF-8; check the path, permissions, and encoding`,
      );
    } finally {
      if (owned && descriptor !== undefined) closeSync(descriptor);
    }
  })();
}

async function dispatch(
  command: string,
  args: string[],
  path: string,
  implicitDatabasePath: boolean,
  machine: boolean,
  queryBudget: number,
): Promise<void> {
  if (command === "version") {
    removeOptionDelimiter(args);
    if (args.length > 0) usage("version accepts no arguments");
    process.stdout.write(`${VERSION}\n`);
    return;
  }
  validateCommandOptions(command, args);

  if (command === "mcp") {
    const write = flag(args, "--write");
    const embedCommand = option(args, "--embed-cmd");
    removeOptionDelimiter(args);
    if (args.length > 0) usage(`mcp does not accept ${args.join(" ")}`);
    const db = openSelectedDatabase(
      path,
      implicitDatabasePath,
      !write,
      queryBudget,
    );
    try {
      // Keep the optional MCP SDK out of short-lived commands such as get and q.
      const { runMcp } = await import("./mcp.js");
      runMcp(
        db,
        embedCommand === undefined ? { write } : { write, embedCommand },
      );
    } catch (error) {
      // A failed server handoff leaves the CLI responsible for releasing SQLite.
      db.close();
      throw error;
    }
    return;
  }
  const writable = new Set([
    "init",
    "add",
    "retract",
    "declare",
    "shape",
    "schema-apply",
    "apply",
    "restore",
    "undo",
    "excise",
  ]);
  const repair = command === "doctor" && flag(args, "--repair");
  let openedDatabase: Db | undefined;
  const database = (): Db => {
    openedDatabase ??= openSelectedDatabase(
      path,
      implicitDatabasePath,
      !(writable.has(command) || repair),
      queryBudget,
    );
    return openedDatabase;
  };
  try {
    if (command === "init" || command === "info") {
      removeOptionDelimiter(args);
      if (args.length > 0) usage(`${command} accepts no arguments`);
      emit(database().stats(), machine);
    } else if (command === "add") {
      const batchValue = option(args, "--batch-size");
      const batchSize =
        batchValue === undefined
          ? undefined
          : positiveInteger(batchValue, "batch size");
      if (batchSize !== undefined && batchSize > 10_000)
        usage("batch size must be at most 10000");
      const operationIdPrefix = option(args, "--operation-id-prefix");
      const transactionOptions = mutationOptions(args);
      removeOptionDelimiter(args);
      if (args.length !== 1)
        usage("add needs exactly one JSON argument, @file, or -");
      if (
        operationIdPrefix !== undefined &&
        transactionOptions.operationId !== undefined
      )
        usage(
          "choose --operation-id for one transaction or --operation-id-prefix for batches",
        );
      if (operationIdPrefix !== undefined && batchSize === undefined)
        usage("--operation-id-prefix requires --batch-size");
      if (batchSize !== undefined) {
        emit(
          await addBatches(
            database(),
            args[0] as string,
            batchSize,
            transactionOptions,
            operationIdPrefix,
          ),
          machine,
        );
      } else {
        const payloadList = payloads(args[0] as string, "add input");
        if (
          payloadList.length > 1 &&
          transactionOptions.operationId !== undefined
        )
          throw new TypeError(
            "--operation-id requires one JSON transaction, not NDJSON",
          );
        if (
          transactionOptions.ifBasisTx !== undefined &&
          payloadList.length > 1
        )
          throw new TypeError(
            "--if-basis-tx cannot span multiple transactions; use idempotent operation ids",
          );
        const reports = payloadList.map((payload) =>
          database().transact(payload, transactionOptions),
        );
        emit(reports.length === 1 ? reports[0] : reports, machine);
      }
    } else if (command === "retract") {
      const transactionOptions = mutationOptions(args);
      removeOptionDelimiter(args);
      if (args.length < 1 || args.length > 3)
        usage("retract needs entity, optional attribute, and optional value");
      const ref = reference(args[0] as string);
      const attribute = args[1];
      const operation: unknown[] = ["retract", ref];
      if (attribute !== undefined) operation.push(attribute);
      if (args.length === 3)
        operation.push(parseJson(args[2] as string, "retract value"));
      const report = database().transact(operation, transactionOptions);
      emit(report, machine);
    } else if (command === "get") {
      const depth = positiveInteger(
        option(args, "--depth") ?? "1",
        "depth",
        true,
      );
      const atText = option(args, "--at");
      removeOptionDelimiter(args);
      if (args.length !== 1) usage("get needs exactly one entity");
      const entity = reference(args[0] as string);
      const at = atText === undefined ? undefined : integer(atText, "at");
      const target = at === undefined ? database() : database().at(at);
      emit(target.entity(entity, depth), machine);
    } else if (command === "tx") {
      removeOptionDelimiter(args);
      if (args.length !== 1) usage("tx needs exactly one transaction id");
      const transaction = integer(args[0] as string, "transaction");
      emit(database().receipt(transaction), machine);
    } else if (command === "q") {
      const bindingsText = option(args, "--args");
      const atText = option(args, "--at");
      removeOptionDelimiter(args);
      if (args.length !== 1)
        usage("q needs exactly one query JSON argument or @file");
      const query = parseJson(readArgument(args[0] as string), "query");
      const bindings =
        bindingsText === undefined ? {} : parseJson(bindingsText, "query args");
      if (
        query === null ||
        typeof query !== "object" ||
        Array.isArray(query) ||
        bindings === null ||
        typeof bindings !== "object" ||
        Array.isArray(bindings)
      )
        throw new TypeError("query and --args must be JSON objects");
      const at = atText === undefined ? undefined : integer(atText, "at");
      const target = at === undefined ? database() : database().at(at);
      emit(
        target.q(
          query as Record<string, unknown>,
          bindings as Record<string, unknown>,
        ),
        machine,
      );
    } else if (command === "explain") {
      const bindingsText = option(args, "--args");
      removeOptionDelimiter(args);
      if (args.length !== 1)
        usage("explain needs exactly one query JSON argument or @file");
      const query = parseJson(readArgument(args[0] as string), "query");
      const bindings =
        bindingsText === undefined ? {} : parseJson(bindingsText, "query args");
      if (
        query === null ||
        typeof query !== "object" ||
        Array.isArray(query) ||
        bindings === null ||
        typeof bindings !== "object" ||
        Array.isArray(bindings)
      )
        throw new TypeError("query and --args must be JSON objects");
      emit(
        database().explain(
          query as Record<string, unknown>,
          bindings as Record<string, unknown>,
        ),
        machine,
      );
    } else if (command === "datoms") {
      const source = option(args, "--source") ?? "current";
      const componentsText = option(args, "--components");
      const cursor = option(args, "--cursor");
      const limit = positiveInteger(option(args, "--limit") ?? "100", "limit");
      removeOptionDelimiter(args);
      if (args.length > 1) usage("datoms accepts at most one index name");
      if (source !== "current" && source !== "history")
        usage("datoms --source must be current or history");
      const components =
        componentsText === undefined
          ? []
          : parseJson(componentsText, "datom components");
      if (!Array.isArray(components))
        usage("datoms --components must be a JSON array");
      const index = (args[0] ?? "eavt") as "eavt" | "avet" | "vaet";
      emit(
        database().datoms(index, {
          source,
          components,
          limit,
          ...(cursor === undefined ? {} : { cursor }),
        }),
        machine,
      );
    } else if (command === "search") {
      const textFlag = option(args, "--text");
      const vectorText = option(args, "--vector");
      const embedCommand = option(args, "--embed-cmd");
      const k = positiveInteger(option(args, "--k") ?? "10", "k");
      const expand = positiveInteger(
        option(args, "--expand") ?? "0",
        "expand",
        true,
      );
      const vectorAttribute = option(args, "--vector-attribute");
      const textAttributes = options(args, "--text-attribute");
      const filterValues = options(args, "--filter").map((item) =>
        parseJson(item, "search filter"),
      );
      removeOptionDelimiter(args);
      const text = textFlag ?? (args.length > 0 ? args.join(" ") : undefined);
      let embedding =
        vectorText === undefined
          ? undefined
          : vector(parseJson(vectorText, "search vector"), "search vector");
      const graph = database();
      if (
        embedding === undefined &&
        embedCommand !== undefined &&
        text !== undefined
      ) {
        const { embed } = await import("./mcp.js");
        embedding = embed(embedCommand, text);
      }
      const searchOptions: {
        text?: string;
        vector?: number[];
        k: number;
        expand: number;
        vectorAttribute?: string;
        textAttributes?: string[];
        filters: unknown[][];
      } = {
        k,
        expand,
        filters: filterValues as unknown[][],
      };
      if (text !== undefined) searchOptions.text = text;
      if (embedding !== undefined) searchOptions.vector = embedding;
      if (vectorAttribute !== undefined)
        searchOptions.vectorAttribute = vectorAttribute;
      if (textAttributes.length > 0)
        searchOptions.textAttributes = textAttributes;
      emit(graph.search(searchOptions), machine);
    } else if (command === "history" || command === "why") {
      removeOptionDelimiter(args);
      if (args.length < 1 || args.length > 2)
        usage(`${command} needs entity and optional attribute`);
      const entity = reference(args[0] as string);
      const value =
        command === "history"
          ? database().history(entity, args[1])
          : database().why(entity, args[1]);
      emit(value, machine);
    } else if (command === "diff") {
      removeOptionDelimiter(args);
      if (args.length !== 2) usage("diff needs start and end transaction ids");
      const start = integer(args[0] as string, "start transaction");
      const end = integer(args[1] as string, "end transaction");
      emit(database().diff(start, end), machine);
    } else if (command === "declare") {
      const transactionOptions = mutationOptions(args);
      const declaredType = option(args, "--type");
      const doc = option(args, "--doc");
      const dimsText = option(args, "--dims");
      const vectorModel = option(args, "--vector-model");
      const ref = flag(args, "--ref");
      const many = flag(args, "--many");
      const one = flag(args, "--one");
      const unique = flag(args, "--unique");
      const notUnique = flag(args, "--not-unique");
      const nohistory = flag(args, "--nohistory");
      const historyFlag = flag(args, "--history");
      removeOptionDelimiter(args);
      if ((many && one) || (unique && notUnique) || (nohistory && historyFlag))
        usage("declare boolean enable/disable flags are mutually exclusive");
      if (args.length !== 1) usage("declare needs exactly one attribute");
      const declaration: DeclareOptions = {};
      if (declaredType !== undefined) declaration.type = declaredType;
      if (ref) declaration.ref = true;
      if (many || one) declaration.many = many;
      if (unique || notUnique) declaration.unique = unique;
      if (nohistory || historyFlag) declaration.nohistory = nohistory;
      if (dimsText !== undefined)
        declaration.dims = positiveInteger(dimsText, "dims");
      if (doc !== undefined) declaration.doc = doc;
      if (vectorModel !== undefined) declaration.vectorModel = vectorModel;
      if (transactionOptions.operationId !== undefined)
        declaration.operationId = transactionOptions.operationId;
      if (transactionOptions.ifBasisTx !== undefined)
        declaration.ifBasisTx = transactionOptions.ifBasisTx;
      emit(database().declare(args[0] as string, declaration), machine);
    } else if (command === "shape") {
      const transactionOptions = mutationOptions(args);
      const required = options(args, "--required");
      const allowed = options(args, "--allowed");
      const closed = flag(args, "--closed");
      const open = flag(args, "--open");
      removeOptionDelimiter(args);
      if (closed && open)
        usage("shape --closed and --open are mutually exclusive");
      if (args.length !== 1) usage("shape needs exactly one shape name");
      emit(
        database().defineShape(args[0] as string, {
          required,
          allowed,
          closed,
          ...(transactionOptions.operationId === undefined
            ? {}
            : { operationId: transactionOptions.operationId }),
          ...(transactionOptions.ifBasisTx === undefined
            ? {}
            : { ifBasisTx: transactionOptions.ifBasisTx }),
        }),
        machine,
      );
    } else if (command === "validate") {
      removeOptionDelimiter(args);
      if (args.length !== 1) usage("validate needs exactly one entity");
      const entity = reference(args[0] as string);
      emit(database().validate(entity), machine);
    } else if (command === "schema") {
      const includeSystem = flag(args, "--system");
      removeOptionDelimiter(args);
      if (args.length > 1) usage("schema accepts at most one attribute prefix");
      emit(database().schema(args[0], { includeSystem }), machine);
    } else if (command === "schema-export") {
      removeOptionDelimiter(args);
      if (args.length > 0) usage("schema-export accepts no arguments");
      emit(database().schemaManifest(), machine);
    } else if (command === "schema-check") {
      removeOptionDelimiter(args);
      if (args.length !== 1)
        usage("schema-check needs one JSON argument, @file, or -");
      const manifest = parseJson(
        readArgument(args[0] as string),
        "schema manifest",
      );
      emit(database().checkSchemaManifest(manifest), machine);
    } else if (command === "schema-apply") {
      const transactionOptions = mutationOptions(args);
      removeOptionDelimiter(args);
      if (args.length !== 1)
        usage("schema-apply needs one JSON argument, @file, or -");
      const manifest = parseJson(
        readArgument(args[0] as string),
        "schema manifest",
      );
      emit(
        database().applySchemaManifest(manifest, transactionOptions),
        machine,
      );
    } else if (command === "apply") {
      removeOptionDelimiter(args);
      if (args.length > 1) usage("apply accepts at most one event-stream file");
      const lines = inputLines(args[0] ?? "-", MAX_EVENT_BYTES);
      emit(database().applySummary(lines), machine);
    } else if (command === "snapshot") {
      removeOptionDelimiter(args);
      if (args.length > 0)
        usage("snapshot writes to stdout and accepts no arguments");
      database().snapshot(process.stdout);
    } else if (command === "restore") {
      removeOptionDelimiter(args);
      if (args.length > 1) usage("restore accepts at most one snapshot file");
      const lines = inputLines(args[0] ?? "-", MAX_SNAPSHOT_LINE_BYTES);
      const graph = database();
      graph.restore(lines);
      emit({ ok: true, basis_tx: graph._basisTx() }, machine);
    } else if (command === "undo") {
      const transactionOptions = mutationOptions(args);
      removeOptionDelimiter(args);
      if (args.length !== 1) usage("undo needs exactly one transaction id");
      const transaction = integer(args[0] as string, "transaction");
      emit(
        database().undo(transaction, {
          ...(transactionOptions.operationId === undefined
            ? {}
            : { operationId: transactionOptions.operationId }),
          ...(transactionOptions.ifBasisTx === undefined
            ? {}
            : { ifBasisTx: transactionOptions.ifBasisTx }),
        }),
        machine,
      );
    } else if (command === "excise") {
      const transactionOptions = mutationOptions(args);
      removeOptionDelimiter(args);
      if (
        transactionOptions.operationId === undefined ||
        transactionOptions.ifBasisTx === undefined
      )
        usage("excise requires --operation-id and --if-basis-tx");
      if (args.length !== 1) usage("excise needs exactly one entity");
      const entity = reference(args[0] as string);
      emit(
        database().excise(entity, {
          operationId: transactionOptions.operationId,
          ifBasisTx: transactionOptions.ifBasisTx,
        }),
        machine,
      );
    } else if (command === "tail") {
      const since = integer(
        option(args, "--since") ?? GENESIS_TX.toString(),
        "since transaction",
      );
      const keepFollowing = flag(args, "--follow");
      removeOptionDelimiter(args);
      if (args.length > 0) usage("tail accepts only --since and --follow");
      const graph = database();
      if (keepFollowing) {
        for await (const record of graph.follow(since))
          process.stdout.write(`${canonicalJson(record)}\n`);
      } else {
        graph.tail(since, process.stdout);
      }
    } else if (command === "backup") {
      removeOptionDelimiter(args);
      if (args.length !== 1) usage("backup needs exactly one destination");
      await database().backup(args[0] as string);
      emit({ path: args[0] }, machine);
    } else if (command === "doctor") {
      removeOptionDelimiter(args);
      if (args.length > 0) usage("doctor accepts only --repair");
      emit(database().doctor({ repair }), machine);
    }
  } finally {
    openedDatabase?.close();
  }
}

export async function main(argv = process.argv.slice(2)): Promise<number> {
  const args = [...argv];
  try {
    const selectedPath = option(args, "--db");
    const environmentPath = process.env.FGRAPH_DB;
    const path = selectedPath ?? environmentPath ?? DEFAULT_DATABASE_PATH;
    const implicitDatabasePath =
      selectedPath === undefined && environmentPath === undefined;
    const machine = flag(args, "--json");
    const queryBudget = positiveInteger(
      option(args, "--query-budget") ??
        process.env.FGRAPH_QUERY_BUDGET ??
        "100000",
      "query budget",
    );
    if (flag(args, "--version")) {
      process.stdout.write(`${VERSION}\n`);
      return 0;
    }
    const help = flag(args, "--help") || flag(args, "-h");
    if (help) usage(undefined, 0);
    if (args[0] === "--") args.shift();
    if (args.length === 0) usage();
    const command = args.shift() as string;
    await dispatch(
      command,
      args,
      path,
      implicitDatabasePath,
      machine,
      queryBudget,
    );
    return 0;
  } catch (error) {
    if (error instanceof UsageError) return error.exitCode;
    if (error instanceof FGraphError) {
      process.stderr.write(`${error.name}: ${error.message}\n`);
      return 1;
    }
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`Error: ${message}\n`);
    return 1;
  }
}

const entrypoint = process.argv[1];
if (
  entrypoint !== undefined &&
  realpathSync(fileURLToPath(import.meta.url)) === realpathSync(entrypoint)
)
  process.exitCode = await main();
