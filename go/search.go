package fgraph

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	searchCandidateLimit = 50 // compatibility default for internal helpers
	maxSearchCandidates  = 500
	maxSearchK           = 100
	maxSearchExpand      = 3
	maxSearchFilters     = 16
	maxSearchAttributes  = 16
	maxExpandedEntities  = 100
	maxMatchedFacts      = 8
	maxPullAttributes    = 32
	maxPullValues        = 32
	maxMatchedValueBytes = 2 * 1024
	maxSearchOutputBytes = 1_048_576
	keywordPageMinimum   = 256
	rrfK                 = 60.0
)

type rankedFact struct {
	fact   Fact
	entity int64
	score  float64
}

type rankedRawFact struct {
	raw        rawFact
	entity     int64
	score      float64
	dimensions int
}

type rankedRawHeap []rankedRawFact

func (items rankedRawHeap) Len() int { return len(items) }

func (items rankedRawHeap) Less(i, j int) bool {
	// The heap root is the worst retained result so a better candidate can
	// replace it in O(log k). Later fact ids lose deterministic score ties.
	if items[i].score == items[j].score {
		return items[i].raw.id > items[j].raw.id
	}
	return items[i].score < items[j].score
}

func (items rankedRawHeap) Swap(i, j int) { items[i], items[j] = items[j], items[i] }

func (items *rankedRawHeap) Push(value any) {
	candidate, ok := value.(rankedRawFact)
	if !ok {
		// container/heap is an untyped standard-library boundary; only this
		// package calls it, so another element type is an internal invariant bug.
		panic(fmt.Sprintf("rankedRawHeap received %T", value))
	}
	*items = append(*items, candidate)
}

func (items *rankedRawHeap) Pop() any {
	prior := *items
	last := len(prior) - 1
	value := prior[last]
	prior[last] = rankedRawFact{}
	*items = prior[:last]
	return value
}

type boundedRawRanking struct {
	heap  rankedRawHeap
	limit int
	total int
}

func newBoundedRawRanking(limit int) *boundedRawRanking {
	return &boundedRawRanking{heap: make(rankedRawHeap, 0, limit), limit: limit}
}

func betterRankedRaw(candidate, prior rankedRawFact) bool {
	return candidate.score > prior.score || (candidate.score == prior.score && candidate.raw.id < prior.raw.id)
}

func (ranking *boundedRawRanking) add(candidate rankedRawFact) {
	ranking.total++
	if len(ranking.heap) < ranking.limit {
		heap.Push(&ranking.heap, candidate)
		return
	}
	if betterRankedRaw(candidate, ranking.heap[0]) {
		ranking.heap[0] = candidate
		heap.Fix(&ranking.heap, 0)
	}
}

func (ranking *boundedRawRanking) result() ([]rankedRawFact, bool) {
	result := append([]rankedRawFact(nil), ranking.heap...)
	sort.Slice(result, func(i, j int) bool { return betterRankedRaw(result[i], result[j]) })
	return result, ranking.total > ranking.limit
}

type candidateBatch struct {
	items     []rankedFact
	truncated bool
}

type searchWork struct {
	limit int
	used  int
}

func (work *searchWork) spend() error {
	if work.used >= work.limit {
		return fail(ErrTooLarge, "search exhausted its work budget; narrow filters or raise the configured query budget")
	}
	work.used++
	return nil
}

func (work *searchWork) remaining() int { return max(0, work.limit-work.used) }

type expansionEdge struct {
	fact     rawFact
	neighbor int64
}

type resolvedSearchFilter struct {
	value storedValue
	a     int64
}

func (db *DB) Search(ctx context.Context, options SearchOpts) (SearchResult, error) {
	if db.asOf != nil {
		return SearchResult{}, fail(ErrUnsupported, "search is unavailable on past views in v1; query or pull the as-of view instead")
	}
	if strings.TrimSpace(options.Text) == "" && len(options.Vector) == 0 {
		return SearchResult{}, fail(ErrType, "search needs text or vector; provide at least one retrieval signal")
	}
	if strings.TrimSpace(options.Text) == "" && len(options.TextAttributes) > 0 {
		return SearchResult{}, fail(ErrType, "TextAttributes requires a non-blank text query")
	}
	if len(options.Vector) == 0 && options.VectorAttribute != "" {
		return SearchResult{}, fail(ErrType, "VectorAttribute requires a vector query")
	}
	if options.K == 0 {
		options.K = 10
	}
	if options.K < 1 || options.K > maxSearchK || options.Expand < 0 || options.Expand > maxSearchExpand {
		return SearchResult{}, fail(ErrType, "search k=%d expand=%d is invalid; use k 1..%d and expand 0..%d", options.K, options.Expand, maxSearchK, maxSearchExpand)
	}
	if len(options.Filters) > maxSearchFilters || len(options.TextAttributes) > maxSearchAttributes {
		return SearchResult{}, fail(ErrTooLarge, "search has %d filters and %d text attributes; use at most %d and %d", len(options.Filters), len(options.TextAttributes), maxSearchFilters, maxSearchAttributes)
	}
	if len(options.Vector) > 0 {
		if err := validateCosineVector(options.Vector); err != nil {
			return SearchResult{}, err
		}
		if strings.TrimSpace(options.VectorAttribute) == "" {
			return SearchResult{}, fail(ErrType, "vector search requires exactly one VectorAttribute")
		}
	}
	candidateLimit := min(maxSearchCandidates, max(searchCandidateLimit, 5*options.K))
	result := SearchResult{Hits: []SearchHit{}, Expanded: []SearchHit{}}
	err := db.withRead(ctx, func(runner sqlRunner) error {
		basis, err := db.basisOn(ctx, runner)
		if err != nil {
			return wrap(ErrQuery, err, "cannot pin search basis")
		}
		result.BasisTx = basis
		work := &searchWork{limit: db.store.queryBudget}
		filters, possible, err := db.resolveSearchFilters(ctx, runner, options.Filters)
		if err != nil {
			return err
		}
		if !possible {
			result.WorkUsed = work.used
			return nil
		}
		eligible, err := db.eligibleSearchEntities(ctx, runner, filters, work)
		if err != nil {
			return err
		}
		lists := [][]rankedFact{}
		if strings.TrimSpace(options.Text) != "" {
			keyword, err := db.keywordCandidatesBounded(ctx, runner, options.Text, options.TextAttributes, eligible, candidateLimit, work)
			if err != nil {
				return err
			}
			lists = append(lists, keyword.items)
			result.Truncated = result.Truncated || keyword.truncated
		}
		if len(options.Vector) > 0 {
			semantic, err := db.vectorCandidatesBounded(ctx, runner, options.Vector, options.VectorAttribute, eligible, candidateLimit, work)
			if err != nil {
				return err
			}
			lists = append(lists, semantic.items)
			result.Truncated = result.Truncated || semantic.truncated
		}
		scores := map[int64]float64{}
		matched := map[int64][]Fact{}
		for _, list := range lists {
			bestRank := map[int64]int{}
			for index, candidate := range list {
				rank := index + 1
				if _, exists := bestRank[candidate.entity]; !exists {
					bestRank[candidate.entity] = rank
				}
				matched[candidate.entity] = appendUniqueFact(matched[candidate.entity], candidate.fact)
			}
			for entity, rank := range bestRank {
				scores[entity] += 1 / (rrfK + float64(rank))
			}
		}
		entities := make([]int64, 0, len(scores))
		for entity := range scores {
			entities = append(entities, entity)
		}
		sort.Slice(entities, func(i, j int) bool {
			if scores[entities[i]] == scores[entities[j]] {
				return fmt.Sprint(db.displayEntity(entities[i])) < fmt.Sprint(db.displayEntity(entities[j]))
			}
			return scores[entities[i]] > scores[entities[j]]
		})
		if len(entities) > options.K {
			entities = entities[:options.K]
		}
		for _, entity := range entities {
			pull, err := db.pullEntityCompact(ctx, runner, entity)
			if err != nil {
				return err
			}
			facts := matched[entity]
			if len(facts) > maxMatchedFacts {
				result.Truncated = true
				facts = facts[:maxMatchedFacts]
			}
			for index := range facts {
				facts[index] = boundMatchedFact(facts[index])
			}
			result.Hits = append(result.Hits, SearchHit{
				Entity: db.displayEntity(entity), Score: scores[entity], Matched: facts, Pull: pull,
			})
		}
		if options.Expand > 0 {
			expanded, truncated, err := db.expandSearch(ctx, runner, entities, options.Expand, work)
			if err != nil {
				return err
			}
			result.Expanded = expanded
			result.Truncated = result.Truncated || truncated
		}
		result.WorkUsed = work.used
		boundSearchOutput(&result)
		return nil
	})
	return result, err
}

func validateCosineVector(vector []float32) error {
	if len(vector) == 0 {
		return fail(ErrType, "search vector is empty; provide a non-empty embedding")
	}
	norm := float64(0)
	for _, component := range vector {
		if math.IsNaN(float64(component)) || math.IsInf(float64(component), 0) {
			return fail(ErrType, "search vector contains a non-finite component; provide finite float32 values")
		}
		norm += float64(component) * float64(component)
	}
	if norm == 0 {
		return fail(ErrType, "search vector is all zero; provide a non-zero embedding for cosine similarity")
	}
	return nil
}

func appendUniqueFact(facts []Fact, candidate Fact) []Fact {
	for _, fact := range facts {
		if fact.ID == candidate.ID {
			return facts
		}
	}
	return append(facts, candidate)
}

func boundMatchedFact(fact Fact) Fact {
	if fact.Tag == TagVector {
		dimensions := 0
		if wrapper, ok := fact.V.(map[string]any); ok {
			switch vector := wrapper["vector_dims"].(type) {
			case int:
				dimensions = vector
			case int64:
				dimensions = int(vector)
			}
			switch vector := wrapper["vector"].(type) {
			case []any:
				dimensions = len(vector)
			case []float32:
				dimensions = len(vector)
			case []float64:
				dimensions = len(vector)
			}
		}
		fact.V = map[string]any{"vector_dims": dimensions}
		fact.ValueTruncated = true
	} else if text, ok := fact.V.(string); ok {
		fact.V, fact.ValueTruncated = boundMatchedText(text)
	}
	if fact.Snippet != "" {
		fact.Snippet, fact.SnippetTruncated = boundMatchedText(fact.Snippet)
	}
	return fact
}

func boundMatchedText(text string) (string, bool) {
	if len(text) <= maxMatchedValueBytes {
		return text, false
	}
	const ellipsis = "…"
	end := maxMatchedValueBytes - len(ellipsis)
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end] + ellipsis, true
}

func boundSearchOutput(result *SearchResult) {
	encodedSize := searchOutputSize(result)
	if encodedSize <= maxSearchOutputBytes {
		return
	}
	result.Truncated = true
	if len(result.Expanded) > 0 {
		result.Expanded = []SearchHit{}
		encodedSize = searchOutputSize(result)
	}
	if encodedSize > maxSearchOutputBytes {
		for index := range result.Hits {
			result.Hits[index].Matched = nil
		}
		encodedSize = searchOutputSize(result)
	}
	for encodedSize > maxSearchOutputBytes && len(result.Hits) > 0 {
		result.Hits = result.Hits[:len(result.Hits)-1]
		encodedSize = searchOutputSize(result)
	}
}

func searchOutputSize(result *SearchResult) int {
	encoded, err := marshalOrderedObject([]Field{
		{Name: "hits", Value: result.Hits},
		{Name: "expanded", Value: result.Expanded},
		{Name: "basis_tx", Value: result.BasisTx},
		{Name: "truncated", Value: result.Truncated},
		{Name: "work_used", Value: result.WorkUsed},
	})
	if err != nil {
		// The caller cannot safely reduce an output that has no JSON size.
		return 0
	}
	return len(encoded)
}

func ftsQuery(text string) string {
	words := []string{}
	var word strings.Builder
	flush := func() {
		if word.Len() > 0 {
			words = append(words, word.String())
			word.Reset()
		}
	}
	for _, current := range text {
		if current == '_' || unicode.IsLetter(current) || unicode.IsNumber(current) {
			word.WriteRune(current)
		} else {
			flush()
		}
	}
	flush()
	phrases := make([]string, len(words))
	for i, token := range words {
		phrases[i] = `"` + token + `"`
	}
	return strings.Join(phrases, " ")
}

func (db *DB) keywordCandidatesBounded(
	ctx context.Context,
	runner sqlRunner,
	text string,
	attributes []string,
	eligible map[int64]bool,
	limit int,
	work *searchWork,
) (candidateBatch, error) {
	match := ftsQuery(text)
	if match == "" {
		return candidateBatch{items: []rankedFact{}}, nil
	}
	query := `SELECT f.id,f.e,f.a,f.v,f.t,f.tx,f.rx,
		rank,snippet(fgraph_fts,0,'[',']','…',12)
		FROM fgraph_fts JOIN fgraph_facts f ON f.id=fgraph_fts.rowid
		WHERE fgraph_fts MATCH ? AND f.rx IS NULL AND f.a>=?`
	args := make([]any, 0, 4)
	args = append(args, match, FirstUserID)
	selected := map[int64]bool{}
	if len(attributes) > 0 {
		for _, attribute := range attributes {
			id, found := db.store.names[attribute]
			if !found {
				return candidateBatch{}, fail(ErrNotFound, "text attribute %q does not exist", attribute)
			}
			selected[id] = true
		}
	}
	query += " AND f.e>=?"
	args = append(args, FirstUserID)
	query += " ORDER BY rank,f.id LIMIT ? OFFSET ?"
	result := []rankedFact{}
	seen := map[int64]bool{}
	pageSize := max(keywordPageMinimum, limit*4)
	offset := 0
	for {
		pageLimit := min(pageSize, work.remaining()+1)
		pageArgs := append(append([]any(nil), args...), pageLimit, offset)
		rows, err := runner.QueryContext(ctx, query, pageArgs...)
		if err != nil {
			return candidateBatch{}, wrap(ErrQuery, err, "full-text query %q is invalid; use words or a valid FTS5 query", text)
		}
		rowCount := 0
		pageErr := func() (resultErr error) {
			defer func() {
				resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "full-text candidate rows"))
			}()
			for rows.Next() {
				rowCount++
				var raw rawFact
				var rank float64
				var snippet string
				if err := rows.Scan(&raw.id, &raw.e, &raw.a, &raw.v, &raw.t, &raw.tx, &raw.rx, &rank, &snippet); err != nil {
					return wrap(ErrFormat, err, "cannot decode full-text search candidate")
				}
				if err := work.spend(); err != nil {
					return err
				}
				if eligible != nil && !eligible[raw.e] {
					continue
				}
				if len(selected) > 0 && !selected[raw.a] {
					continue
				}
				if seen[raw.e] {
					continue
				}
				fact, err := db.renderRaw(ctx, runner, raw, nil)
				if err != nil {
					return err
				}
				fact, err = db.withAssertingTransaction(ctx, runner, fact)
				if err != nil {
					return err
				}
				fact.Snippet = snippet
				seen[raw.e] = true
				result = append(result, rankedFact{fact: fact, entity: raw.e, score: -rank})
				if len(result) > limit {
					break
				}
			}
			if err := rows.Err(); err != nil {
				return wrap(ErrFormat, err, "cannot finish full-text search")
			}
			return nil
		}()
		if pageErr != nil {
			return candidateBatch{}, pageErr
		}
		if len(result) > limit {
			return candidateBatch{items: result[:limit], truncated: true}, nil
		}
		if rowCount < pageLimit {
			return candidateBatch{items: result}, nil
		}
		offset += rowCount
	}
}

func (db *DB) withAssertingTransaction(ctx context.Context, runner sqlRunner, fact Fact) (Fact, error) {
	info, err := db.transactionInfo(ctx, runner, fact.Tx)
	if err != nil {
		return Fact{}, err
	}
	fact.At, fact.By, fact.Source = info.at, info.by, info.source
	fact.presence |= info.presence & (factAtPresent | factByPresent | factSourcePresent)
	return fact, nil
}

func (db *DB) vectorCandidatesBounded(
	ctx context.Context,
	runner sqlRunner,
	queryVector []float32,
	attribute string,
	eligible map[int64]bool,
	limit int,
	work *searchWork,
) (candidateBatch, error) {
	query := `SELECT f.id,f.e,f.a,f.v,f.t,f.tx,f.rx,b.data
		FROM fgraph_facts f LEFT JOIN fgraph_blobs b ON b.hash=f.v
		WHERE f.t=7 AND f.rx IS NULL`
	args := []any{}
	if attribute != "" {
		attrID, found := db.store.names[attribute]
		if !found {
			return candidateBatch{}, fail(ErrNotFound, "vector attribute %q does not exist; use a known vector attribute", attribute)
		}
		schema, err := db.schemaFor(ctx, runner, attrID, nil)
		if err != nil {
			return candidateBatch{}, err
		}
		// Undeclared attributes become vector-capable when the first vector write
		// persists fgraph/dims. An explicit non-vector type remains authoritative.
		if schema.typeName != "vector" && (schema.typeName != "" || !schema.dimsSet) {
			return candidateBatch{}, fail(ErrType, "search attribute %q is not vector-typed; declare it with Type(\"vector\")", attribute)
		}
		if schema.dimsSet && schema.dims != int64(len(queryVector)) {
			return candidateBatch{}, fail(ErrType, "query vector has %d dimensions but %q declares %d; provide a matching embedding", len(queryVector), attribute, schema.dims)
		}
		query += " AND f.a=?"
		args = append(args, attrID)
	}
	query += " ORDER BY f.e,f.id LIMIT ?"
	args = append(args, work.remaining()+1)
	rows, err := runner.QueryContext(ctx, query, args...)
	if err != nil {
		return candidateBatch{}, wrap(ErrFormat, err, "cannot load vector search candidates")
	}
	queryNorm := vectorNorm(queryVector)
	ranking := newBoundedRawRanking(limit)
	matchedDims := false
	sawFact := false
	var currentEntity int64
	var currentBest rankedRawFact
	hasCurrentEntity := false
	hasCurrentBest := false
	finishEntity := func() {
		if hasCurrentBest {
			ranking.add(currentBest)
			hasCurrentBest = false
		}
	}
	err = func() (resultErr error) {
		defer func() {
			resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "joined vector candidate rows"))
		}()
		for rows.Next() {
			var raw rawFact
			var blob any
			if scanErr := rows.Scan(
				&raw.id, &raw.e, &raw.a, &raw.v,
				&raw.t, &raw.tx, &raw.rx, &blob,
			); scanErr != nil {
				return wrap(ErrFormat, scanErr, "cannot decode joined vector candidate")
			}
			sawFact = true
			if spendErr := work.spend(); spendErr != nil {
				return spendErr
			}
			if hasCurrentEntity && raw.e != currentEntity {
				finishEntity()
			}
			currentEntity = raw.e
			hasCurrentEntity = true
			if eligible != nil && !eligible[raw.e] {
				continue
			}
			logical, logicalErr := db.logicalIndirectValue(raw.v, raw.t, blob)
			if logicalErr != nil {
				return logicalErr
			}
			vector, ok := logical.([]float32)
			if !ok {
				return fail(ErrFormat, "vector fact %d decoded as %T; restore a valid database", raw.id, logical)
			}
			if len(vector) != len(queryVector) {
				continue
			}
			matchedDims = true
			similarity := cosineWithNorm(queryVector, vector, queryNorm)
			if math.IsInf(similarity, -1) {
				continue
			}
			candidate := rankedRawFact{
				raw: raw, entity: raw.e, score: similarity, dimensions: len(vector),
			}
			if !hasCurrentBest || betterRankedRaw(candidate, currentBest) {
				currentBest = candidate
				hasCurrentBest = true
			}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return wrap(ErrFormat, rowsErr, "cannot finish joined vector candidates")
		}
		finishEntity()
		return nil
	}()
	if err != nil {
		return candidateBatch{}, err
	}
	if attribute != "" && sawFact && !matchedDims {
		return candidateBatch{}, fail(ErrType, "query vector has %d dimensions but %q stores another dimension; provide a matching embedding", len(queryVector), attribute)
	}
	rankedRaw, truncated := ranking.result()
	ranked := make([]rankedFact, 0, len(rankedRaw))
	for _, candidate := range rankedRaw {
		fact, err := db.renderRawLogical(ctx, runner, candidate.raw, []float32{}, nil)
		if err != nil {
			return candidateBatch{}, err
		}
		fact.V = map[string]any{"vector_dims": candidate.dimensions}
		fact.ValueTruncated = true
		fact, err = db.withAssertingTransaction(ctx, runner, fact)
		if err != nil {
			return candidateBatch{}, err
		}
		ranked = append(ranked, rankedFact{fact: fact, entity: candidate.entity, score: candidate.score})
	}
	return candidateBatch{items: ranked, truncated: truncated}, nil
}

func rankRawEntityCandidatesBounded(input []rankedRawFact, limit int) ([]rankedRawFact, bool) {
	best := map[int64]rankedRawFact{}
	for _, candidate := range input {
		prior, found := best[candidate.entity]
		if !found || betterRankedRaw(candidate, prior) {
			best[candidate.entity] = candidate
		}
	}
	ranking := newBoundedRawRanking(limit)
	for _, candidate := range best {
		ranking.add(candidate)
	}
	return ranking.result()
}

func (db *DB) resolveSearchFilters(
	ctx context.Context,
	runner sqlRunner,
	filters [][]any,
) ([]resolvedSearchFilter, bool, error) {
	result := make([]resolvedSearchFilter, 0, len(filters))
	for _, filter := range filters {
		if len(filter) != 2 {
			return nil, false, fail(ErrType, "search filter has %d items; use [attribute,value]", len(filter))
		}
		attr, ok := filter[0].(string)
		if !ok {
			return nil, false, fail(ErrType, "search filter attribute has type %T; use a name", filter[0])
		}
		attrID, found := db.store.names[attr]
		if !found {
			return nil, false, nil
		}
		schema, err := db.schemaFor(ctx, runner, attrID, nil)
		if err != nil {
			return nil, false, err
		}
		value, err := db.resolveReadFactValue(ctx, runner, attr, schema, filter[1])
		if err != nil {
			return nil, false, err
		}
		result = append(result, resolvedSearchFilter{a: attrID, value: value})
	}
	return result, true, nil
}

func (db *DB) eligibleSearchEntities(
	ctx context.Context,
	runner sqlRunner,
	filters []resolvedSearchFilter,
	work *searchWork,
) (eligible map[int64]bool, resultErr error) {
	for _, filter := range filters {
		owners, ownersErr := db.searchFilterOwners(ctx, runner, filter, work)
		if ownersErr != nil {
			return nil, ownersErr
		}
		if eligible == nil {
			eligible = owners
		} else {
			for entity := range eligible {
				if !owners[entity] {
					delete(eligible, entity)
				}
			}
		}
		if len(eligible) == 0 {
			break
		}
	}
	return eligible, nil
}

func (db *DB) searchFilterOwners(
	ctx context.Context,
	runner sqlRunner,
	filter resolvedSearchFilter,
	work *searchWork,
) (owners map[int64]bool, resultErr error) {
	rows, err := runner.QueryContext(ctx, `SELECT e FROM fgraph_facts
		WHERE a=? AND v=? AND t=? AND rx IS NULL ORDER BY e`, filter.a, filter.value.storage, filter.value.tag)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot enumerate search filter attribute %d", filter.a)
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "search filter owner rows")) }()
	owners = map[int64]bool{}
	for rows.Next() {
		if err := work.spend(); err != nil {
			return nil, err
		}
		var entity int64
		if err := rows.Scan(&entity); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode search filter owner")
		}
		owners[entity] = true
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish search filter owners")
	}
	return owners, nil
}

func cosine(left, right []float32) float64 {
	return cosineWithNorm(left, right, vectorNorm(left))
}

func vectorNorm(vector []float32) float64 {
	squared := float64(0)
	for _, value := range vector {
		asFloat := float64(value)
		squared += asFloat * asFloat
	}
	return math.Sqrt(squared)
}

func cosineWithNorm(left, right []float32, leftNorm float64) float64 {
	if len(left) != len(right) {
		return math.Inf(-1)
	}
	dot, rightSquared := float64(0), float64(0)
	for i := range left {
		l, r := float64(left[i]), float64(right[i])
		dot += l * r
		rightSquared += r * r
	}
	rightNorm := math.Sqrt(rightSquared)
	if leftNorm == 0 || rightNorm == 0 {
		return math.Inf(-1)
	}
	return dot / (leftNorm * rightNorm)
}

func (db *DB) pullEntityCompact(ctx context.Context, runner sqlRunner, entity int64) (result map[string]any, resultErr error) {
	type selectedAttribute struct {
		name string
		id   int64
	}
	rows, err := runner.QueryContext(ctx, `SELECT f.a,i.name
		FROM fgraph_facts f JOIN fgraph_ids i ON i.id=f.a
		WHERE f.e=? AND f.rx IS NULL
		GROUP BY f.a,i.name ORDER BY i.name COLLATE BINARY LIMIT ?`, entity, maxPullAttributes)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot read compact search attributes for entity %d", entity)
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "compact search attribute rows")) }()
	attributes := []selectedAttribute{}
	for rows.Next() {
		var attribute selectedAttribute
		if err := rows.Scan(&attribute.id, &attribute.name); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode compact search attribute")
		}
		attributes = append(attributes, attribute)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish compact search attributes")
	}
	if err := rows.Close(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot close compact search attribute rows")
	}
	result = map[string]any{}
	for _, attribute := range attributes {
		schema, err := db.schemaFor(ctx, runner, attribute.id, nil)
		if err != nil {
			return nil, err
		}
		err = func() (valueErr error) {
			valueRows, queryErr := runner.QueryContext(ctx, `SELECT f.v,f.t FROM fgraph_facts f
				WHERE f.e=? AND f.a=? AND f.rx IS NULL
				ORDER BY f.id LIMIT ?`, entity, attribute.id, maxPullValues)
			if queryErr != nil {
				return wrap(ErrFormat, queryErr, "cannot read compact search values for attribute %q", attribute.name)
			}
			defer func() { valueErr = joinErrors(valueErr, wrapClose(valueRows.Close(), "compact search value rows")) }()
			for valueRows.Next() {
				var raw any
				var tag Tag
				if scanErr := valueRows.Scan(&raw, &tag); scanErr != nil {
					return wrap(ErrFormat, scanErr, "cannot decode compact search value for attribute %q", attribute.name)
				}
				logical, logicalErr := db.logicalValue(ctx, runner, raw, tag)
				if logicalErr != nil {
					return logicalErr
				}
				rendered := db.renderLogical(logical, tag)
				if !schema.many {
					result[attribute.name] = rendered
					continue
				}
				values := make([]any, 0, 1)
				if existing, found := result[attribute.name]; found {
					var ok bool
					values, ok = existing.([]any)
					if !ok {
						return fail(ErrFormat, "many-valued search projection %q collided with scalar data", attribute.name)
					}
				}
				result[attribute.name] = append(values, rendered)
			}
			if rowsErr := valueRows.Err(); rowsErr != nil {
				return wrap(ErrFormat, rowsErr, "cannot finish compact search values for attribute %q", attribute.name)
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (db *DB) expandSearch(ctx context.Context, runner sqlRunner, seeds []int64, hops int, workOptions ...*searchWork) ([]SearchHit, bool, error) {
	work := &searchWork{limit: db.store.queryBudget}
	if len(workOptions) > 0 && workOptions[0] != nil {
		work = workOptions[0]
	}
	seen := map[int64]bool{}
	paths := map[int64][]any{}
	frontier := append([]int64(nil), seeds...)
	for _, seed := range seeds {
		seen[seed] = true
		paths[seed] = []any{}
	}
	expanded := []SearchHit{}
	truncated := false
	for depth := 0; depth < hops && len(frontier) > 0; depth++ {
		next := []int64{}
		for _, current := range frontier {
			rows, err := runner.QueryContext(ctx, `SELECT id,e,a,v,t,tx,rx FROM fgraph_facts
				WHERE t=0 AND rx IS NULL AND (e=? OR v=?) ORDER BY id`, current, current)
			if err != nil {
				return nil, false, wrap(ErrFormat, err, "cannot expand graph around entity %d", current)
			}
			edges, err := readExpansionEdges(rows, current)
			if err != nil {
				return nil, false, err
			}
			for _, edge := range edges {
				if err := work.spend(); err != nil {
					return nil, false, err
				}
				if seen[edge.neighbor] {
					continue
				}
				seen[edge.neighbor] = true
				if len(expanded) >= maxExpandedEntities {
					return expanded, true, nil
				}
				rendered, err := db.renderRaw(ctx, runner, edge.fact, nil)
				if err != nil {
					return nil, false, err
				}
				path := append(append([]any(nil), paths[current]...), rendered)
				paths[edge.neighbor] = path
				pull, err := db.pullEntityCompact(ctx, runner, edge.neighbor)
				if err != nil {
					return nil, false, err
				}
				expanded = append(expanded, SearchHit{Entity: db.displayEntity(edge.neighbor), Matched: []Fact{}, Pull: pull, Via: path})
				next = append(next, edge.neighbor)
			}
		}
		frontier = next
	}
	return expanded, truncated, nil
}

func readExpansionEdges(rows *sql.Rows, current int64) (edges []expansionEdge, resultErr error) {
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "graph expansion rows"))
	}()
	for rows.Next() {
		var fact rawFact
		if err := rows.Scan(&fact.id, &fact.e, &fact.a, &fact.v, &fact.t, &fact.tx, &fact.rx); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode graph expansion edge")
		}
		to := asInt64(fact.v)
		if fact.e == current {
			edges = append(edges, expansionEdge{neighbor: to, fact: fact})
		} else {
			edges = append(edges, expansionEdge{neighbor: fact.e, fact: fact})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish graph expansion")
	}
	return edges, nil
}
