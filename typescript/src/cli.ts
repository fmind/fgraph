#!/usr/bin/env node
import {
  closeSync,
  createReadStream,
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

const VERSION = "1.0.4";

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

function option(args: string[], name: string): string | undefined {
  let result: string | undefined;
  for (let index = 0; index < args.length; index++) {
    const value = args[index];
    if (value === name) {
      const next = args[index + 1];
      if (next === undefined) usage(`${name} needs a value`);
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
    if (value === name) {
      const next = args[index + 1];
      if (next === undefined) usage(`${name} needs a value`);
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
  for (let index = args.length - 1; index >= 0; index--) {
    if (args[index] === name) {
      found = true;
      args.splice(index, 1);
    }
  }
  return found;
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
        usage(
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
  machine: boolean,
  queryBudget: number,
): Promise<void> {
  if (command === "version") {
    if (args.length > 0) usage("version accepts no arguments");
    process.stdout.write(`${VERSION}\n`);
    return;
  }
  if (command === "mcp") {
    const write = flag(args, "--write");
    const embedCommand = option(args, "--embed-cmd");
    if (args.length > 0) usage(`mcp does not accept ${args.join(" ")}`);
    const db = open(path, !write, queryBudget);
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
  const commands = new Set([
    "init",
    "info",
    "add",
    "retract",
    "get",
    "tx",
    "q",
    "explain",
    "datoms",
    "search",
    "history",
    "why",
    "diff",
    "declare",
    "shape",
    "validate",
    "schema",
    "schema-export",
    "schema-check",
    "schema-apply",
    "apply",
    "snapshot",
    "restore",
    "undo",
    "excise",
    "tail",
    "backup",
    "doctor",
  ]);
  if (!commands.has(command))
    usage(`unknown command ${JSON.stringify(command)}`);
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
  const db = open(path, !(writable.has(command) || repair), queryBudget);
  try {
    if (command === "init" || command === "info") {
      if (args.length > 0) usage(`${command} accepts no arguments`);
      emit(db.stats(), machine);
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
            db,
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
          usage("--operation-id requires one JSON transaction, not NDJSON");
        if (
          transactionOptions.ifBasisTx !== undefined &&
          payloadList.length > 1
        )
          usage(
            "--if-basis-tx cannot span multiple transactions; use idempotent operation ids",
          );
        const reports = payloadList.map((payload) =>
          db.transact(payload, transactionOptions),
        );
        emit(reports.length === 1 ? reports[0] : reports, machine);
      }
    } else if (command === "retract") {
      const transactionOptions = mutationOptions(args);
      if (args.length < 1 || args.length > 3)
        usage("retract needs entity, optional attribute, and optional value");
      const ref = reference(args[0] as string);
      const attribute = args[1];
      const operation: unknown[] = ["retract", ref];
      if (attribute !== undefined) operation.push(attribute);
      if (args.length === 3)
        operation.push(parseJson(args[2] as string, "retract value"));
      const report = db.transact(operation, transactionOptions);
      emit(report, machine);
    } else if (command === "get") {
      const depth = positiveInteger(
        option(args, "--depth") ?? "1",
        "depth",
        true,
      );
      const atText = option(args, "--at");
      if (args.length !== 1) usage("get needs exactly one entity");
      const target = atText === undefined ? db : db.at(integer(atText, "at"));
      emit(target.entity(reference(args[0] as string), depth), machine);
    } else if (command === "tx") {
      if (args.length !== 1) usage("tx needs exactly one transaction id");
      emit(db.receipt(integer(args[0] as string, "transaction")), machine);
    } else if (command === "q") {
      const bindingsText = option(args, "--args");
      const atText = option(args, "--at");
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
      emit(
        (atText === undefined ? db : db.at(integer(atText, "at"))).q(
          query as Record<string, unknown>,
          bindings as Record<string, unknown>,
        ),
        machine,
      );
    } else if (command === "explain") {
      const bindingsText = option(args, "--args");
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
        db.explain(
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
        db.datoms(index, {
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
      if (args.some((item) => item.startsWith("--")))
        usage(
          `unknown search option ${args.find((item) => item.startsWith("--"))}`,
        );
      const text = textFlag ?? (args.length > 0 ? args.join(" ") : undefined);
      let embedding =
        vectorText === undefined
          ? undefined
          : vector(parseJson(vectorText, "search vector"), "search vector");
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
      emit(db.search(searchOptions), machine);
    } else if (command === "history" || command === "why") {
      if (args.length < 1 || args.length > 2)
        usage(`${command} needs entity and optional attribute`);
      const value =
        command === "history"
          ? db.history(reference(args[0] as string), args[1])
          : db.why(reference(args[0] as string), args[1]);
      emit(value, machine);
    } else if (command === "diff") {
      if (args.length !== 2) usage("diff needs start and end transaction ids");
      emit(
        db.diff(
          integer(args[0] as string, "start transaction"),
          integer(args[1] as string, "end transaction"),
        ),
        machine,
      );
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
      emit(db.declare(args[0] as string, declaration), machine);
    } else if (command === "shape") {
      const transactionOptions = mutationOptions(args);
      const required = options(args, "--required");
      const allowed = options(args, "--allowed");
      const closed = flag(args, "--closed");
      const open = flag(args, "--open");
      if (closed && open)
        usage("shape --closed and --open are mutually exclusive");
      if (args.length !== 1) usage("shape needs exactly one shape name");
      emit(
        db.defineShape(args[0] as string, {
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
      if (args.length !== 1) usage("validate needs exactly one entity");
      emit(db.validate(reference(args[0] as string)), machine);
    } else if (command === "schema") {
      const includeSystem = flag(args, "--system");
      if (args.length > 1) usage("schema accepts at most one attribute prefix");
      emit(db.schema(args[0], { includeSystem }), machine);
    } else if (command === "schema-export") {
      if (args.length > 0) usage("schema-export accepts no arguments");
      emit(db.schemaManifest(), machine);
    } else if (command === "schema-check") {
      if (args.length !== 1)
        usage("schema-check needs one JSON argument, @file, or -");
      emit(
        db.checkSchemaManifest(
          parseJson(readArgument(args[0] as string), "schema manifest"),
        ),
        machine,
      );
    } else if (command === "schema-apply") {
      const transactionOptions = mutationOptions(args);
      if (args.length !== 1)
        usage("schema-apply needs one JSON argument, @file, or -");
      emit(
        db.applySchemaManifest(
          parseJson(readArgument(args[0] as string), "schema manifest"),
          transactionOptions,
        ),
        machine,
      );
    } else if (command === "apply") {
      if (args.length > 1) usage("apply accepts at most one event-stream file");
      emit(
        db.applySummary(inputLines(args[0] ?? "-", MAX_EVENT_BYTES)),
        machine,
      );
    } else if (command === "snapshot") {
      if (args.length > 0)
        usage("snapshot writes to stdout and accepts no arguments");
      db.snapshot(process.stdout);
    } else if (command === "restore") {
      if (args.length > 1) usage("restore accepts at most one snapshot file");
      db.restore(inputLines(args[0] ?? "-", MAX_SNAPSHOT_LINE_BYTES));
      emit({ ok: true, basis_tx: db._basisTx() }, machine);
    } else if (command === "undo") {
      const transactionOptions = mutationOptions(args);
      if (args.length !== 1) usage("undo needs exactly one transaction id");
      emit(
        db.undo(integer(args[0] as string, "transaction"), {
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
      if (
        transactionOptions.operationId === undefined ||
        transactionOptions.ifBasisTx === undefined
      )
        usage("excise requires --operation-id and --if-basis-tx");
      if (args.length !== 1) usage("excise needs exactly one entity");
      emit(
        db.excise(reference(args[0] as string), {
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
      if (args.length > 0) usage("tail accepts only --since and --follow");
      if (keepFollowing) {
        for await (const record of db.follow(since))
          process.stdout.write(`${canonicalJson(record)}\n`);
      } else {
        db.tail(since, process.stdout);
      }
    } else if (command === "backup") {
      if (args.length !== 1) usage("backup needs exactly one destination");
      await db.backup(args[0] as string);
      emit({ path: args[0] }, machine);
    } else if (command === "doctor") {
      if (args.length > 0) usage("doctor accepts only --repair");
      emit(db.doctor({ repair }), machine);
    }
  } finally {
    db.close();
  }
}

export async function main(argv = process.argv.slice(2)): Promise<number> {
  const args = [...argv];
  try {
    const path = option(args, "--db") ?? process.env.FGRAPH_DB ?? "fgraph.db";
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
    if (args.length === 0) usage();
    const command = args.shift() as string;
    await dispatch(command, args, path, machine, queryBudget);
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
