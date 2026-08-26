"""Current-state keyword/vector search with deterministic reciprocal-rank fusion."""

from __future__ import annotations

import math
import re
from collections import defaultdict, deque
from collections.abc import Sequence
from dataclasses import dataclass, field
from heapq import heappush, heapreplace
from typing import Any

from fgraph.errors import NotFound, TooLarge, Unsupported
from fgraph.errors import TypeError as FGraphTypeError
from fgraph.models import SearchResult
from fgraph.store import FIRST_USER_ID
from fgraph.values import VECTOR, _canonical_json_document, encode

RRF_K = 60
MAX_K = 100
MAX_EXPAND = 3
MAX_EXPANDED_NODES = 100
MAX_FILTERS = 16
MAX_TEXT_ATTRIBUTES = 16
MAX_MATCHES_PER_HIT = 8
MAX_MATCH_TEXT_BYTES = 2 * 1024
MAX_RESULT_BYTES = 1024 * 1024
MAX_PULL_ATTRIBUTES = 32
MAX_PULL_VALUES = 32


@dataclass(slots=True)
class _WorkBudget:
    limit: int
    used: int = 0

    def consume(self, amount: int = 1) -> None:
        self.used += amount
        if self.used > self.limit:
            raise TooLarge(
                "search exceeds the configured work budget; narrow text_attributes, vector_attribute, or filters"
            )


@dataclass(slots=True)
class _VectorCandidate:
    score: float
    fact_id: int
    entity: int
    row: dict[str, Any]
    dimensions: int


@dataclass(slots=True)
class _BoundedVectorRanking:
    limit: int
    # Python's min-heap retains the worst candidate at index zero. Negating the
    # fact id makes a later fact worse when scores tie, matching the wire order.
    heap: list[tuple[float, int, _VectorCandidate]] = field(default_factory=list)
    count: int = 0

    def add(self, candidate: _VectorCandidate) -> None:
        self.count += 1
        item = (candidate.score, -candidate.fact_id, candidate)
        if len(self.heap) < self.limit:
            heappush(self.heap, item)
        elif item[:2] > self.heap[0][:2]:
            heapreplace(self.heap, item)

    def result(self) -> tuple[list[_VectorCandidate], bool]:
        ranked = sorted(
            (item[2] for item in self.heap),
            key=lambda candidate: (-candidate.score, candidate.fact_id),
        )
        return ranked, self.count > self.limit


def _fts_query(text: str) -> str:
    tokens = re.findall(r"\w+", text, flags=re.UNICODE)
    return " ".join(f'"{token.replace(chr(34), chr(34) * 2)}"' for token in tokens)


def _keyword(
    db: Any,
    text: str | None,
    text_attributes: Sequence[str],
    eligible: set[int] | None,
    candidate_limit: int,
    work: _WorkBudget,
) -> tuple[dict[int, int], dict[int, list[dict[str, Any]]], bool]:
    matched: dict[int, list[dict[str, Any]]] = defaultdict(list)
    if text is None:
        return {}, matched, False
    query = _fts_query(text)
    if not query:
        return {}, matched, False
    ranks: dict[int, int] = {}
    conditions = ["fgraph_fts MATCH ?", "f.rx IS NULL", "f.a>=?"]
    parameters: list[Any] = [query, FIRST_USER_ID]
    if text_attributes:
        attribute_ids: list[int] = []
        for name in text_attributes:
            identifier = db._names.get(name)  # noqa: SLF001
            if identifier is None:
                raise NotFound(f"text search attribute {name!r} was not found; declare or populate it first")
            schema = db._schema(identifier)  # noqa: SLF001
            if schema.type not in (None, "text"):
                raise FGraphTypeError(
                    f"text search attribute {name!r} is {schema.type!r}, not text; choose a text attribute"
                )
            attribute_ids.append(identifier)
        conditions.append(f"f.a IN ({','.join('?' for _ in attribute_ids)})")
        parameters.extend(attribute_ids)
    search_sql = (
        "SELECT f.*, rank AS score, "  # noqa: S608
        "snippet(fgraph_fts, 0, '[', ']', '…', 12) AS snippet "
        "FROM fgraph_fts JOIN fgraph_facts f ON f.id=fgraph_fts.rowid "
        f"WHERE {' AND '.join(conditions)} "
        "ORDER BY rank, f.id LIMIT ?"
    )
    rows = db._connection.execute(  # noqa: SLF001
        search_sql,
        (*parameters, db._query_budget + 1),  # noqa: SLF001
    )
    truncated = False
    for candidate in rows:
        work.consume()
        entity = int(candidate["e"])
        if eligible is not None and entity not in eligible:
            continue
        if entity in ranks:
            continue
        if len(ranks) >= candidate_limit:
            truncated = True
            break
        ranks[entity] = len(ranks) + 1
        rendered = _matched_fact(db, candidate)
        snippet, snippet_truncated = _bounded_text(str(candidate["snippet"]))
        rendered["snippet"] = snippet
        if snippet_truncated:
            rendered["snippet_truncated"] = True
        matched[entity].append(rendered)
    return ranks, matched, truncated


def _bounded_text(value: str) -> tuple[str, bool]:
    raw = value.encode()
    if len(raw) <= MAX_MATCH_TEXT_BYTES:
        return value, False
    marker = "…"
    # The marker is part of the public byte cap, and decoding may discard only
    # an incomplete trailing code point from the bounded prefix.
    prefix_limit = MAX_MATCH_TEXT_BYTES - len(marker.encode())
    return raw[:prefix_limit].decode(errors="ignore") + marker, True


def _matched_fact(
    db: Any,
    row: Any,
    logical: Any = None,
    *,
    vector_dimensions: int | None = None,
) -> dict[str, Any]:
    if vector_dimensions is not None:
        # Vector evidence exposes only its dimension count. Passing an empty
        # logical vector avoids a blob point-read and never materializes the
        # winning payload a second time.
        rendered = db._render_row(row, logical_override=())  # noqa: SLF001
    elif logical is None:
        rendered = db._render_row(row)  # noqa: SLF001
    else:
        rendered = db._render_row(row, logical_override=logical)  # noqa: SLF001
    if int(row["t"]) == VECTOR:
        if vector_dimensions is None:
            vector = db._logical(VECTOR, row["v"]) if logical is None else logical  # noqa: SLF001
            vector_dimensions = len(vector)
        rendered["v"] = {"vector_dims": vector_dimensions}
        rendered["value_truncated"] = True
    elif isinstance(rendered.get("v"), str):
        rendered["v"], truncated = _bounded_text(rendered["v"])
        if truncated:
            rendered["value_truncated"] = True
    metadata = db._tx_metadata(int(row["tx"]))  # noqa: SLF001
    rendered.update({key: metadata[key] for key in ("at", "by", "source") if key in metadata})
    return rendered


def _compact_pull(db: Any, entity: int) -> dict[str, Any]:
    result: dict[str, Any] = {}
    attributes = db._connection.execute(  # noqa: SLF001
        """SELECT f.a,i.name
        FROM fgraph_facts f JOIN fgraph_ids i ON i.id=f.a
        WHERE f.e=? AND f.rx IS NULL
        GROUP BY f.a,i.name ORDER BY i.name COLLATE BINARY LIMIT ?""",
        (entity, MAX_PULL_ATTRIBUTES),
    )
    for selected in attributes:
        attribute_id = int(selected["a"])
        attribute = str(selected["name"])
        schema = db._schema(attribute_id)  # noqa: SLF001
        rows = db._connection.execute(  # noqa: SLF001
            """SELECT f.t,f.v FROM fgraph_facts f
            WHERE f.e=? AND f.a=? AND f.rx IS NULL
            ORDER BY f.id LIMIT ?""",
            (entity, attribute_id, MAX_PULL_VALUES),
        )
        for row in rows:
            rendered = db._wire(int(row["t"]), row["v"])  # noqa: SLF001
            if schema.many:
                result.setdefault(attribute, []).append(rendered)
            else:
                result[attribute] = rendered
    return result


def _cosine(left: Sequence[float], right: Sequence[float], left_norm: float | None = None) -> float:
    if len(left) != len(right):
        return -math.inf
    if left_norm is None:
        left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm == 0 or right_norm == 0:
        return -math.inf
    return sum(a * b for a, b in zip(left, right, strict=True)) / (left_norm * right_norm)


def _semantic(
    db: Any,
    vector: Sequence[float] | None,
    attribute: str | None,
    eligible: set[int] | None,
    candidate_limit: int,
    work: _WorkBudget,
) -> tuple[dict[int, int], dict[int, list[dict[str, Any]]], bool]:
    matched: dict[int, list[dict[str, Any]]] = defaultdict(list)
    if vector is None:
        return {}, matched, False
    encoded = encode({"vector": list(vector)})
    query_vector = tuple(float(value) for value in encoded.logical)
    if not any(query_vector):
        raise FGraphTypeError(
            "search vector is all zeroes; provide a non-zero embedding so cosine similarity is defined"
        )
    query_norm = math.sqrt(sum(value * value for value in query_vector))
    conditions = ["f.t=?", "f.rx IS NULL", "f.a>=?"]
    parameters: list[Any] = [VECTOR, FIRST_USER_ID]
    if attribute is not None:
        attribute_id = db._names.get(attribute)  # noqa: SLF001
        if attribute_id is None:
            raise NotFound(f"search attribute {attribute!r} was not found; declare and populate it first")
        schema = db._schema(attribute_id)  # noqa: SLF001
        if schema.type not in (None, "vector"):
            raise FGraphTypeError(f"search attribute {attribute!r} is not vector-typed; declare it with type='vector'")
        conditions.append("f.a=?")
        parameters.append(attribute_id)
        fixed = schema.dims or db._inferred_vector_dims(attribute_id)  # noqa: SLF001
        if fixed is not None and fixed != len(query_vector):
            raise FGraphTypeError(
                f"search vector has {len(query_vector)} dimensions, but {attribute!r} requires {fixed}; "
                "embed with the same model used for that attribute"
            )
    rows = db._connection.execute(  # noqa: SLF001
        "SELECT f.*, b.hash AS fgraph_blob_hash, b.data AS fgraph_blob_data "  # noqa: S608
        "FROM fgraph_facts AS f LEFT JOIN fgraph_blobs AS b ON b.hash=f.v "
        f"WHERE {' AND '.join(conditions)} ORDER BY f.e, f.id LIMIT ?",
        (*parameters, db._query_budget + 1),  # noqa: SLF001
    )
    ranking = _BoundedVectorRanking(candidate_limit)
    current_entity: int | None = None
    current_best: _VectorCandidate | None = None
    saw_row = False

    def finish_entity() -> None:
        nonlocal current_best
        if current_best is not None:
            ranking.add(current_best)
            current_best = None

    for row in rows:
        saw_row = True
        work.consume()
        entity = int(row["e"])
        if current_entity is not None and entity != current_entity:
            finish_entity()
        current_entity = entity
        if eligible is not None and entity not in eligible:
            continue
        candidate = db._logical_indirect(  # noqa: SLF001
            VECTOR,
            row["v"],
            row["fgraph_blob_data"],
            found=row["fgraph_blob_hash"] is not None,
        )
        score = _cosine(query_vector, candidate, query_norm)
        if math.isfinite(score):
            fact_id = int(row["id"])
            ranked = _VectorCandidate(
                score,
                fact_id,
                entity,
                {name: row[name] for name in ("id", "e", "a", "v", "t", "tx", "rx")},
                len(candidate),
            )
            if (
                current_best is None
                or score > current_best.score
                or (score == current_best.score and fact_id < current_best.fact_id)
            ):
                current_best = ranked
    finish_entity()
    if attribute is not None and schema.type is None and not saw_row:
        raise FGraphTypeError(f"search attribute {attribute!r} is not vector-typed; declare it with type='vector'")
    scored, truncated = ranking.result()
    ranks: dict[int, int] = {}
    for candidate in scored:
        ranks[candidate.entity] = len(ranks) + 1
        matched[candidate.entity].append(_matched_fact(db, candidate.row, vector_dimensions=candidate.dimensions))
    return ranks, matched, truncated


def _prepare_filters(db: Any, filters: Sequence[Sequence[Any]]) -> list[tuple[int | None, Any]]:
    prepared: list[tuple[int | None, Any]] = []
    for name, value in filters:
        attribute = db._names.get(name)  # noqa: SLF001
        encoded = None if attribute is None else db._encode_read_value(value, db._schema(attribute))  # noqa: SLF001
        prepared.append((attribute, encoded))
    return prepared


def _eligible_entities(
    db: Any,
    filters: Sequence[tuple[int | None, Any]],
    work: _WorkBudget,
) -> set[int] | None:
    eligible: set[int] | None = None
    for attribute, encoded in filters:
        if attribute is None:
            return set()
        owners: set[int] = set()
        rows = db._connection.execute(  # noqa: SLF001
            "SELECT e FROM fgraph_facts WHERE a=? AND v=? AND t=? AND rx IS NULL ORDER BY e",
            (attribute, encoded.stored, encoded.tag),
        )
        for row in rows:
            work.consume()
            owners.add(int(row["e"]))
        eligible = owners if eligible is None else eligible & owners
        if not eligible:
            break
    return eligible


def _expanded(db: Any, roots: Sequence[int], hops: int, work: _WorkBudget) -> tuple[list[dict[str, Any]], bool]:
    if hops == 0:
        return [], False
    root_set = set(roots)
    visited = set(roots)
    queue = deque((root, 0, []) for root in roots)
    result: list[dict[str, Any]] = []
    while queue:
        entity, distance, path = queue.popleft()
        if distance >= hops:
            continue
        edges = db._connection.execute(  # noqa: SLF001
            "SELECT * FROM fgraph_facts WHERE rx IS NULL AND t=0 AND (e=? OR v=?) ORDER BY id", (entity, entity)
        ).fetchall()
        for edge in edges:
            work.consume()
            target = int(edge["v"]) if int(edge["e"]) == entity else int(edge["e"])
            if target in visited:
                continue
            visited.add(target)
            next_path = [*path, db._render_row(edge)]  # noqa: SLF001
            queue.append((target, distance + 1, next_path))
            if target not in root_set:
                result.append(
                    {
                        "entity": db._name_or_id(target),  # noqa: SLF001
                        "via": next_path,
                        "pull": _compact_pull(db, target),
                    }
                )
                if len(result) >= MAX_EXPANDED_NODES:
                    return result, True
    return result, False


def _bounded_result(
    basis: int,
    hits: list[dict[str, Any]],
    expanded: list[dict[str, Any]],
    truncated: bool,
    work_used: int,
) -> SearchResult:
    def size() -> int:
        return len(
            _canonical_json_document(
                {
                    "basis_tx": basis,
                    "hits": hits,
                    "expanded": expanded,
                    "truncated": truncated,
                    "work_used": work_used,
                }
            ).encode()
        )

    if size() > MAX_RESULT_BYTES:
        expanded.clear()
        truncated = True
    if size() > MAX_RESULT_BYTES:
        for hit in hits:
            hit["matched"] = []
        truncated = True
    while hits and size() > MAX_RESULT_BYTES:
        hits.pop()
        truncated = True
    return SearchResult(basis, hits, expanded, truncated, work_used)


def search(
    db: Any,
    text: str | None,
    vector: Sequence[float] | None,
    k: int,
    expand: int,
    filters: Sequence[Sequence[Any]],
    vector_attribute: str | None,
    text_attributes: Sequence[str] = (),
) -> SearchResult:
    """Implement SPEC section 9 over current live facts."""
    if db._as_of is not None:  # noqa: SLF001
        raise Unsupported("search on a historical view is unavailable in API v1; search current facts or query at(t)")
    if text is not None and not isinstance(text, str):
        raise FGraphTypeError(f"search text {text!r} is invalid; provide a string")
    if text is not None and not text.strip():
        text = None
    if text is None and vector is None:
        raise FGraphTypeError("search needs text or vector; pass at least one retrieval signal")
    if vector is not None and (not isinstance(vector, Sequence) or isinstance(vector, (str, bytes, bytearray))):
        raise FGraphTypeError(f"search vector {vector!r} is invalid; provide a non-empty array of finite numbers")
    if vector is not None and (not isinstance(vector_attribute, str) or not vector_attribute.strip()):
        raise FGraphTypeError("vector search requires vector_attribute; select the embedding attribute and model")
    if vector is None and vector_attribute is not None:
        raise FGraphTypeError("vector_attribute requires a vector query; remove it or provide vector")
    if not isinstance(k, int) or isinstance(k, bool) or not 1 <= k <= MAX_K:
        raise FGraphTypeError(f"search k={k!r} is invalid; use an integer from 1 through {MAX_K}")
    if not isinstance(expand, int) or isinstance(expand, bool) or not 0 <= expand <= MAX_EXPAND:
        raise FGraphTypeError(f"search expand={expand!r} is invalid; use zero through {MAX_EXPAND} hops")
    if not isinstance(filters, Sequence) or isinstance(filters, (str, bytes, bytearray)):
        raise FGraphTypeError("search filters must be an array of [attribute, value] pairs")
    if len(filters) > MAX_FILTERS:
        raise FGraphTypeError(f"search has {len(filters)} filters; use at most {MAX_FILTERS}")
    if not isinstance(text_attributes, Sequence) or isinstance(text_attributes, (str, bytes, bytearray)):
        raise FGraphTypeError("text_attributes must be an array of attribute names")
    if len(text_attributes) > MAX_TEXT_ATTRIBUTES or any(not isinstance(item, str) for item in text_attributes):
        raise FGraphTypeError(f"text_attributes must contain at most {MAX_TEXT_ATTRIBUTES} attribute names")
    if any(not item for item in text_attributes):
        raise FGraphTypeError("text_attributes cannot contain empty names")
    if any(
        not isinstance(condition, Sequence)
        or isinstance(condition, (str, bytes, bytearray))
        or len(condition) != 2
        or not isinstance(condition[0], str)
        for condition in filters
    ):
        raise FGraphTypeError(
            "every search filter must be ['namespace/attribute', value]; correct the malformed filter"
        )
    with db._read_snapshot():  # noqa: SLF001
        basis = db._latest_tx()  # noqa: SLF001
        prepared_filters = _prepare_filters(db, filters)
        candidate_limit = min(500, max(50, 5 * k))
        work = _WorkBudget(db._query_budget)  # noqa: SLF001
        eligible = _eligible_entities(db, prepared_filters, work)
        keyword, keyword_matches, keyword_truncated = _keyword(
            db, text, text_attributes, eligible, candidate_limit, work
        )
        semantic, semantic_matches, semantic_truncated = _semantic(
            db, vector, vector_attribute, eligible, candidate_limit, work
        )
        scores: dict[int, float] = defaultdict(float)
        for ranked in (keyword, semantic):
            for entity, rank in ranked.items():
                scores[entity] += 1 / (RRF_K + rank)
        ranked_entities = sorted(
            scores,
            key=lambda entity: (-scores[entity], str(db._name_or_id(entity))),  # noqa: SLF001
        )
        ranked_entities = ranked_entities[:k]
        hits = [
            {
                "entity": db._name_or_id(entity),  # noqa: SLF001
                "score": scores[entity],
                "matched": [*keyword_matches[entity], *semantic_matches[entity]],
                "pull": _compact_pull(db, entity),
            }
            for entity in ranked_entities
        ]
        expanded, neighbors_truncated = _expanded(db, ranked_entities, expand, work)
        return _bounded_result(
            basis,
            hits,
            expanded,
            keyword_truncated or semantic_truncated or neighbors_truncated,
            work.used,
        )
