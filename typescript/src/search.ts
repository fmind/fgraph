import { NotFound, TooLarge, TypeError, Unsupported } from "./errors.js";
import { canonicalJson, compareUnicode } from "./jsonio.js";
import type { RenderedFact, SearchResult } from "./models.js";
import { type Db, type RawRow } from "./store.js";
import { VECTOR, encode } from "./values.js";

const RRF_K = 60;
const MAX_CANDIDATES = 500;
const MAX_NEIGHBORS = 100;
const MAX_SEARCH_RESULT_BYTES = 1024 * 1024;
const MAX_MATCH_TEXT_BYTES = 2048;
const MAX_PULL_ATTRIBUTES = 32;
const MAX_PULL_VALUES = 32;

interface Ranked {
  ranks: Map<bigint, number>;
  matched: Map<bigint, RenderedFact>;
  truncated: boolean;
}

function truncateUtf8(value: string, limit: number): string {
  let result = "";
  let size = 0;
  for (const character of value) {
    const bytes = Buffer.byteLength(character, "utf8");
    if (size + bytes > limit) break;
    result += character;
    size += bytes;
  }
  return result;
}

function boundedText(value: string): { value: string; truncated: boolean } {
  if (Buffer.byteLength(value, "utf8") <= MAX_MATCH_TEXT_BYTES)
    return { value, truncated: false };
  const marker = "…";
  const contentLimit = MAX_MATCH_TEXT_BYTES - Buffer.byteLength(marker, "utf8");
  return {
    value: `${truncateUtf8(value, contentLimit)}${marker}`,
    truncated: true,
  };
}

export interface SearchOptions {
  text?: string;
  vector?: number[];
  k?: number;
  expand?: number;
  filters?: unknown[][];
  textAttributes?: string[];
  vectorAttribute?: string;
  explain?: boolean;
  workBudget?: number;
}

function ftsQuery(text: string): string {
  return [...text.matchAll(/[\p{L}\p{N}_]+/gu)]
    .map((match) => `"${match[0].replaceAll('"', '""')}"`)
    .join(" ");
}

function matchedFact(
  db: Db,
  row: RawRow,
  logical?: unknown,
  vectorDimensions?: number,
): RenderedFact {
  // Vector search already validated the joined blob. Rendering an empty
  // logical vector avoids retaining or reloading the winning payload.
  const rendered = db._renderRow(
    row,
    undefined,
    undefined,
    vectorDimensions === undefined ? logical : [],
  );
  if (Number(row.t) === VECTOR) {
    const dimensions =
      vectorDimensions ??
      (
        (logical === undefined
          ? db._logical(VECTOR, row.v)
          : logical) as number[]
      ).length;
    rendered.v = {
      vector_dims: dimensions,
    };
    rendered.value_truncated = true;
  } else if (
    typeof rendered.v === "string" &&
    Buffer.byteLength(rendered.v, "utf8") > MAX_MATCH_TEXT_BYTES
  ) {
    rendered.v = boundedText(rendered.v).value;
    rendered.value_truncated = true;
  }
  const metadata = db._transactionMetadata(row.tx);
  for (const key of ["at", "by", "source"] as const)
    if (Object.hasOwn(metadata, key)) rendered[key] = metadata[key];
  return rendered;
}

function compactPull(db: Db, entity: bigint): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  const attributes = db._connection
    .prepare<[bigint, number], { a: bigint; name: string }>(
      `SELECT f.a,i.name
       FROM fgraph_facts f JOIN fgraph_ids i ON i.id=f.a
       WHERE f.e=? AND f.rx IS NULL
       GROUP BY f.a,i.name ORDER BY i.name COLLATE BINARY LIMIT ?`,
    )
    .all(entity, MAX_PULL_ATTRIBUTES);
  for (const selected of attributes) {
    const schema = db._schema(selected.a);
    const rows = db._connection
      .prepare<[bigint, bigint, number], Pick<RawRow, "t" | "v">>(
        `SELECT f.t,f.v FROM fgraph_facts f
         WHERE f.e=? AND f.a=? AND f.rx IS NULL
         ORDER BY f.id LIMIT ?`,
      )
      .all(entity, selected.a, MAX_PULL_VALUES);
    for (const row of rows) {
      const rendered = db._wire(Number(row.t), row.v);
      if (schema.many) {
        const values = (result[selected.name] as unknown[] | undefined) ?? [];
        values.push(rendered);
        result[selected.name] = values;
      } else result[selected.name] = rendered;
    }
  }
  return result;
}

function eligibleEntities(
  db: Db,
  filters: unknown[][],
  spend: () => void,
): Set<bigint> | null {
  let eligible: Set<bigint> | null = null;
  for (const condition of filters) {
    const attributeName = condition[0] as string;
    const attribute = db._attributeId(attributeName);
    if (attribute === null) return new Set();
    const encoded = db._encodeReadValue(condition[1], db._schema(attribute));
    const owners = new Set<bigint>();
    for (const row of db._connection
      .prepare<[bigint, unknown, number], { e: bigint }>(
        "SELECT e FROM fgraph_facts WHERE a=? AND v=? AND t=? AND rx IS NULL ORDER BY e",
      )
      .iterate(attribute, encoded.stored, encoded.tag)) {
      spend();
      owners.add(row.e);
    }
    if (eligible === null) eligible = owners;
    else {
      const intersection = new Set<bigint>();
      for (const entity of eligible as Set<bigint>)
        if (owners.has(entity)) intersection.add(entity);
      eligible = intersection;
    }
    if (eligible.size === 0) break;
  }
  return eligible;
}

function keyword(
  db: Db,
  text: string | undefined,
  eligible: Set<bigint> | null,
  textAttributes: string[],
  candidateLimit: number,
  spend: () => void,
): Ranked {
  const matched = new Map<bigint, RenderedFact>();
  if (text === undefined)
    return { ranks: new Map(), matched, truncated: false };
  const query = ftsQuery(text);
  if (query === "") return { ranks: new Map(), matched, truncated: false };
  const selected = new Set<bigint>();
  for (const name of textAttributes) {
    const id = db._attributeId(name);
    if (id === null)
      throw new NotFound(
        `text search attribute ${JSON.stringify(name)} was not found`,
      );
    selected.add(id);
  }
  const rows = db._connection
    .prepare<[string], RawRow & { score: number; snippet: string }>(
      "SELECT f.*,rank score,snippet(fgraph_fts,0,'[',']','…',12) snippet " +
        "FROM fgraph_fts JOIN fgraph_facts f ON f.id=fgraph_fts.rowid " +
        "WHERE fgraph_fts MATCH ? AND f.rx IS NULL AND f.e>64 AND f.a>=65 ORDER BY rank,f.id",
    )
    .iterate(query);
  const ranks = new Map<bigint, number>();
  let truncated = false;
  for (const row of rows) {
    spend();
    if (eligible !== null && !eligible.has(row.e)) continue;
    if (selected.size > 0 && !selected.has(row.a)) continue;
    if (ranks.has(row.e)) continue;
    if (ranks.size >= candidateLimit) {
      truncated = true;
      break;
    }
    const rendered = matchedFact(db, row);
    const snippet = boundedText(row.snippet);
    rendered.snippet = snippet.value;
    if (snippet.truncated) rendered.snippet_truncated = true;
    matched.set(row.e, rendered);
    ranks.set(row.e, ranks.size + 1);
  }
  return { ranks, matched, truncated };
}

function cosine(
  left: number[],
  right: number[],
  leftNorm = Math.sqrt(left.reduce((total, value) => total + value * value, 0)),
): number {
  if (left.length !== right.length) return Number.NEGATIVE_INFINITY;
  let dot = 0;
  let rightSquared = 0;
  for (let index = 0; index < left.length; index++) {
    const rightValue = right[index] as number;
    dot += (left[index] as number) * rightValue;
    rightSquared += rightValue * rightValue;
  }
  const rightNorm = Math.sqrt(rightSquared);
  if (leftNorm === 0 || rightNorm === 0) return Number.NEGATIVE_INFINITY;
  return dot / (leftNorm * rightNorm);
}

interface VectorCandidate {
  row: RawRow;
  dimensions: number;
  score: number;
}

function compareVectorCandidates(
  left: VectorCandidate,
  right: VectorCandidate,
): number {
  return (
    right.score - left.score ||
    (left.row.id < right.row.id ? -1 : left.row.id > right.row.id ? 1 : 0)
  );
}

class BoundedVectorRanking {
  readonly #limit: number;
  readonly #heap: VectorCandidate[] = [];
  #count = 0;

  constructor(limit: number) {
    this.#limit = limit;
  }

  add(candidate: VectorCandidate): void {
    this.#count++;
    if (this.#heap.length < this.#limit) {
      this.#heap.push(candidate);
      this.#siftUp(this.#heap.length - 1);
      return;
    }
    const worst = this.#heap[0] as VectorCandidate;
    if (compareVectorCandidates(candidate, worst) >= 0) return;
    this.#heap[0] = candidate;
    this.#siftDown(0);
  }

  result(): { candidates: VectorCandidate[]; truncated: boolean } {
    return {
      candidates: [...this.#heap].sort(compareVectorCandidates),
      truncated: this.#count > this.#limit,
    };
  }

  #siftUp(start: number): void {
    let index = start;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (
        compareVectorCandidates(
          this.#heap[index] as VectorCandidate,
          this.#heap[parent] as VectorCandidate,
        ) <= 0
      )
        break;
      [this.#heap[parent], this.#heap[index]] = [
        this.#heap[index] as VectorCandidate,
        this.#heap[parent] as VectorCandidate,
      ];
      index = parent;
    }
  }

  #siftDown(start: number): void {
    let index = start;
    while (true) {
      const left = index * 2 + 1;
      if (left >= this.#heap.length) return;
      const right = left + 1;
      let worst = left;
      if (
        right < this.#heap.length &&
        compareVectorCandidates(
          this.#heap[right] as VectorCandidate,
          this.#heap[left] as VectorCandidate,
        ) > 0
      )
        worst = right;
      if (
        compareVectorCandidates(
          this.#heap[worst] as VectorCandidate,
          this.#heap[index] as VectorCandidate,
        ) <= 0
      )
        return;
      [this.#heap[index], this.#heap[worst]] = [
        this.#heap[worst] as VectorCandidate,
        this.#heap[index] as VectorCandidate,
      ];
      index = worst;
    }
  }
}

function compactVectorRow(row: RawRow): RawRow {
  return {
    id: row.id,
    e: row.e,
    a: row.a,
    v: row.v,
    t: row.t,
    tx: row.tx,
    rx: row.rx,
  };
}

function semantic(
  db: Db,
  vector: number[] | undefined,
  vectorAttribute: string | undefined,
  eligible: Set<bigint> | null,
  candidateLimit: number,
  spend: () => void,
): Ranked {
  const matched = new Map<bigint, RenderedFact>();
  if (vector === undefined)
    return { ranks: new Map(), matched, truncated: false };
  if (vectorAttribute === undefined)
    throw new TypeError(
      "vector search requires vectorAttribute so embeddings from different models never mix",
    );
  const queryVector = encode({ vector }).logical as number[];
  if (!queryVector.some((value) => value !== 0))
    throw new TypeError("search vector must contain a non-zero value");
  const queryNorm = Math.sqrt(
    queryVector.reduce((total, value) => total + value * value, 0),
  );
  const attribute = db._attributeId(vectorAttribute);
  if (attribute === null)
    throw new NotFound(
      `vector search attribute ${JSON.stringify(vectorAttribute)} was not found`,
    );
  const schema = db._schema(attribute);
  if (schema.type !== "vector")
    throw new TypeError(
      `search attribute ${JSON.stringify(vectorAttribute)} must declare type='vector'`,
    );
  if (schema.dims !== null && schema.dims !== queryVector.length)
    throw new TypeError(
      `search vector has ${queryVector.length} dimensions, but ${JSON.stringify(vectorAttribute)} requires ${schema.dims}`,
    );
  const ranking = new BoundedVectorRanking(candidateLimit);
  let currentEntity: bigint | undefined;
  let currentBest: VectorCandidate | undefined;
  const finishEntity = (): void => {
    if (currentBest !== undefined) ranking.add(currentBest);
    currentBest = undefined;
  };
  for (const row of db._connection
    .prepare<[number, bigint], RawRow & { blob_data: unknown }>(
      "SELECT f.*,b.data blob_data FROM fgraph_facts f " +
        "LEFT JOIN fgraph_blobs b ON b.hash=f.v " +
        "WHERE f.t=? AND f.a=? AND f.rx IS NULL ORDER BY f.e,f.id",
    )
    .iterate(VECTOR, attribute)) {
    spend();
    if (currentEntity !== undefined && row.e !== currentEntity) finishEntity();
    currentEntity = row.e;
    if (eligible !== null && !eligible.has(row.e)) continue;
    const logical = db._logical(VECTOR, row.v, row.blob_data) as number[];
    const score = cosine(queryVector, logical, queryNorm);
    if (!Number.isFinite(score)) continue;
    const candidate: VectorCandidate = {
      row: compactVectorRow(row),
      dimensions: logical.length,
      score,
    };
    if (
      currentBest === undefined ||
      compareVectorCandidates(candidate, currentBest) < 0
    )
      currentBest = candidate;
  }
  finishEntity();
  const ranked = ranking.result();
  const ranks = new Map<bigint, number>();
  ranked.candidates.forEach(({ row, dimensions }, index) => {
    ranks.set(row.e, index + 1);
    const rendered = matchedFact(db, row, undefined, dimensions);
    matched.set(row.e, rendered);
  });
  return { ranks, matched, truncated: ranked.truncated };
}

function expanded(
  db: Db,
  roots: bigint[],
  hops: number,
  spend: () => void,
): { items: SearchResult["expanded"]; truncated: boolean } {
  if (hops === 0) return { items: [], truncated: false };
  const visited = new Set(roots);
  const queue = roots.map((entity) => ({
    entity,
    distance: 0,
    path: [] as RenderedFact[],
  }));
  const result: SearchResult["expanded"] = [];
  while (queue.length > 0 && result.length < MAX_NEIGHBORS) {
    const current = queue.shift();
    if (current === undefined || current.distance >= hops) continue;
    const edges = db._connection
      .prepare<[bigint, bigint], RawRow>(
        "SELECT * FROM fgraph_facts WHERE rx IS NULL AND t=0 AND (e=? OR v=?) ORDER BY id",
      )
      .all(current.entity, current.entity);
    for (const edge of edges) {
      spend();
      const target = edge.e === current.entity ? (edge.v as bigint) : edge.e;
      if (visited.has(target)) continue;
      visited.add(target);
      const path = [...current.path, db._renderRow(edge)];
      queue.push({ entity: target, distance: current.distance + 1, path });
      result.push({
        entity: db._nameOrId(target),
        via: path,
        pull: compactPull(db, target),
      });
      if (result.length === MAX_NEIGHBORS) break;
    }
  }
  return { items: result, truncated: result.length === MAX_NEIGHBORS };
}

function boundResult(result: SearchResult): SearchResult {
  const size = (): number => Buffer.byteLength(canonicalJson(result), "utf8");
  if (size() > MAX_SEARCH_RESULT_BYTES && result.expanded.length > 0) {
    result.expanded = [];
    result.truncated = true;
  }
  if (size() > MAX_SEARCH_RESULT_BYTES) {
    for (const hit of result.hits) hit.matched = [];
    result.truncated = true;
  }
  while (size() > MAX_SEARCH_RESULT_BYTES && result.hits.length > 0) {
    result.hits.pop();
    result.truncated = true;
  }
  return result;
}

export function runSearch(
  db: Db,
  options: SearchOptions,
  basisTx: SearchResult["basis_tx"],
): SearchResult {
  if (options === null || typeof options !== "object" || Array.isArray(options))
    throw new TypeError("search options must be an object");
  const allowed = new Set([
    "text",
    "vector",
    "k",
    "expand",
    "filters",
    "textAttributes",
    "vectorAttribute",
    "explain",
    "workBudget",
  ]);
  const unknown = Object.keys(options).filter((key) => !allowed.has(key));
  if (unknown.length > 0)
    throw new TypeError(`unknown search options: ${unknown.sort().join(", ")}`);
  if (db._asOf !== null)
    throw new Unsupported(
      "search is current-only; use temporal Datalog or datoms for historical retrieval",
    );
  let text = options.text;
  if (text !== undefined && typeof text !== "string")
    throw new TypeError("search text must be a string");
  if (text?.trim() === "") text = undefined;
  if (text === undefined && options.vector === undefined)
    throw new TypeError("search needs text or vector");
  const k = options.k ?? 10;
  const hops = options.expand ?? 0;
  if (!Number.isSafeInteger(k) || k < 1 || k > 100)
    throw new TypeError("search k must be an integer from 1 through 100");
  if (!Number.isSafeInteger(hops) || hops < 0 || hops > 3)
    throw new TypeError("search expand must be an integer from 0 through 3");
  const filters = options.filters ?? [];
  if (
    !Array.isArray(filters) ||
    filters.length > 16 ||
    filters.some(
      (condition) =>
        !Array.isArray(condition) ||
        condition.length !== 2 ||
        typeof condition[0] !== "string",
    )
  )
    throw new TypeError(
      "search filters must contain at most 16 [attribute,value] pairs",
    );
  const textAttributes = options.textAttributes ?? [];
  if (
    !Array.isArray(textAttributes) ||
    textAttributes.length > 16 ||
    textAttributes.some((value) => typeof value !== "string")
  )
    throw new TypeError("textAttributes must contain at most 16 names");
  if (
    options.vectorAttribute !== undefined &&
    (typeof options.vectorAttribute !== "string" ||
      options.vectorAttribute.length === 0)
  )
    throw new TypeError("vectorAttribute must be a non-empty attribute name");
  if (options.vector === undefined && options.vectorAttribute !== undefined)
    throw new TypeError(
      "vectorAttribute requires a vector query; remove it or provide vector",
    );
  if (text === undefined && textAttributes.length > 0)
    throw new TypeError(
      "textAttributes requires a text query; remove it or provide text",
    );
  if (options.explain !== undefined && typeof options.explain !== "boolean")
    throw new TypeError("search explain must be a boolean");
  const vectorAttribute = options.vectorAttribute;
  const budget = options.workBudget ?? db.queryBudget;
  if (!Number.isSafeInteger(budget) || budget <= 0)
    throw new TypeError("search workBudget must be a positive safe integer");
  let remaining = budget;
  let workUsed = 0;
  const spend = (): void => {
    if (remaining === 0)
      throw new TooLarge(
        "search exhausted its work budget; narrow filters or raise the configured budget",
      );
    remaining--;
    workUsed++;
  };
  const eligible = eligibleEntities(db, filters, spend);
  const candidateLimit = Math.min(MAX_CANDIDATES, Math.max(50, 5 * k));
  const keywords = keyword(
    db,
    text,
    eligible,
    textAttributes,
    candidateLimit,
    spend,
  );
  const vectors = semantic(
    db,
    options.vector,
    vectorAttribute,
    eligible,
    candidateLimit,
    spend,
  );
  const scores = new Map<bigint, number>();
  for (const ranking of [keywords.ranks, vectors.ranks])
    for (const [entity, rank] of ranking)
      scores.set(entity, (scores.get(entity) ?? 0) + 1 / (RRF_K + rank));
  const entities = [...scores]
    .sort(
      ([leftEntity, leftScore], [rightEntity, rightScore]) =>
        rightScore - leftScore ||
        compareUnicode(
          String(db._nameOrId(leftEntity)),
          String(db._nameOrId(rightEntity)),
        ),
    )
    .slice(0, k)
    .map(([entity]) => entity);
  const hits = entities.map((entity) => {
    const matched = [
      keywords.matched.get(entity),
      vectors.matched.get(entity),
    ].filter((value): value is RenderedFact => value !== undefined);
    const hit: SearchResult["hits"][number] = {
      entity: db._nameOrId(entity),
      score: scores.get(entity) as number,
      matched,
      pull: compactPull(db, entity),
    };
    if (options.explain === true) {
      hit.ranks = {};
      const keywordRank = keywords.ranks.get(entity);
      const vectorRank = vectors.ranks.get(entity);
      if (keywordRank !== undefined) hit.ranks.keyword = keywordRank;
      if (vectorRank !== undefined) hit.ranks.vector = vectorRank;
    }
    return hit;
  });
  const neighbors = expanded(db, entities, hops, spend);
  return boundResult({
    basis_tx: basisTx,
    hits,
    expanded: neighbors.items,
    truncated: keywords.truncated || vectors.truncated || neighbors.truncated,
    work_used: workUsed,
  });
}
