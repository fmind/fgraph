# fgraph for TypeScript

An embedded temporal fact store in one SQLite file. fgraph keeps immutable facts, their transaction time and provenance, superseded history, bounded queries, hybrid search, and an MCP server together without a database service.

The strict ESM package requires the Node.js 24 LTS line (24.19 or newer) and uses `better-sqlite3` with lossless 64-bit integers. Install the stable line:

```bash
npm add @fmind-dev/fgraph@^1.0.3
```

For contribution work, install and build this checkout with `npm ci --prefix typescript && npm run build --prefix typescript`.

```typescript
import { connect } from "@fmind-dev/fgraph";

using db = connect("memory.db");
const created = db.transact(
  { id: "ada", "person/name": "Ada Lovelace" },
  { operationId: "person:ada:v1" },
);
console.log(db.entity("ada"));
console.log(db.receipt(created.tx!));
```

The v1 API includes:

- idempotent transactions, cardinality-one compare-and-swap (including the exact `{ "missing": true }` create/delete sentinel), and basis preconditions;
- current and historical Datalog, indexed datom pages, and actual-plan explanations;
- explicit declarations, rich schema snapshots, portable `schema/1` manifests, gradual shapes, and validation;
- bounded keyword/vector search with exact filters and attributable matches;
- portable `event/1` streams with detailed or compact replay results and streaming `snapshot/1` recovery;
- online backup, integrity checks, audited undo, and privacy excision;
- a bounded MCP server that is read-only unless write tools are explicitly enabled.

Use `db.schema()` for rich introspection; `db.schemaManifest()` / `db.checkSchemaManifest()` / `db.applySchemaManifest()` for the portable control plane; and `db.tail()` / `db.apply()` for detailed logical replication. `db.applySummary()` consumes a large event iterable without retaining detailed reports. `db.snapshotLines()` and `db.snapshot(writer)` stream exact retained-state recovery output, while the convenience `db.snapshot()` returns a string. Online backup is asynchronous: `await db.backup("backup.db")` resolves only after the verified file is durably and atomically published without overwriting an existing destination. The competing legacy export/import protocol is intentionally absent from the v1 API.

The `fgraph` executable exposes the same JSON-first CLI and MCP server as the Python and Go peers, including bounded resumable `add --batch-size N --operation-id-prefix PREFIX`. Run the repository's full TypeScript gate with `mise run check:typescript && mise run test:typescript`.

- [Documentation](https://fmind.github.io/fgraph/)
- [Source, examples, and specification](https://github.com/fmind/fgraph)
- [Issue tracker](https://github.com/fmind/fgraph/issues)
