export type WireInteger = number | bigint;

export interface RenderedFact {
  id: WireInteger;
  e: string | WireInteger;
  a: string;
  v: unknown;
  tx: WireInteger;
  rx: WireInteger | null;
  snippet?: string;
  snippet_truncated?: boolean;
  value_truncated?: boolean;
  [key: string]: unknown;
}

export interface TxReport {
  status: "applied" | "already_applied" | "noop";
  event: string | null;
  basis_tx: WireInteger;
  tx: WireInteger | null;
  at: WireInteger | null;
  ids: Record<string, WireInteger>;
  asserted: RenderedFact[];
  retracted: RenderedFact[];
}

export interface Result {
  columns: string[];
  rows: unknown[][];
}

export interface SearchResult {
  basis_tx: WireInteger;
  hits: Array<{
    entity: string | WireInteger;
    score: number;
    matched: RenderedFact[];
    pull: Record<string, unknown>;
    ranks?: { keyword?: number; vector?: number };
  }>;
  expanded: Array<{
    entity: string | WireInteger;
    via: RenderedFact[];
    pull: Record<string, unknown>;
  }>;
  truncated: boolean;
  work_used: number;
}

export interface AttributeInfo {
  name: string;
  types: string[];
  facts: number;
  many: boolean;
  unique: boolean;
  nohistory: boolean;
  dims?: number;
  doc?: string;
}

export interface SchemaAttribute {
  name: string;
  declared: Record<string, unknown>;
  effective: {
    type: string | null;
    many: boolean;
    unique: boolean;
    nohistory: boolean;
    dims: number | null;
    doc: string | null;
    vector_model: string | null;
  };
  observed: { types: string[]; live_facts: number; entities: number };
}

export interface SchemaSnapshot {
  basis_tx: WireInteger;
  digest: string;
  attributes: SchemaAttribute[];
  shapes: unknown[];
}

export interface SchemaManifestAttribute {
  name: string;
  declared: Record<string, unknown>;
}

export interface SchemaManifestShape {
  name: string;
  required: string[];
  allowed: string[];
  closed: boolean;
}

export interface SchemaManifest {
  fgraph: "schema/1";
  digest: string;
  attributes: SchemaManifestAttribute[];
  shapes: SchemaManifestShape[];
}

export interface SchemaManifestCheck {
  basis_tx: WireInteger;
  valid: boolean;
  current_digest: string;
  desired_digest: string;
  changes: Array<{
    kind: "attribute" | "shape";
    name: string;
    before: unknown;
    after: unknown;
  }>;
}

export interface Datom {
  e: string | WireInteger;
  a: string;
  v: unknown;
  tx: WireInteger;
  added: boolean;
  fact_id: WireInteger;
}

export interface DatomPage {
  basis_tx: WireInteger;
  items: Datom[];
  next_cursor: string | null;
}
