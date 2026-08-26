package fgraph

import (
	"container/heap"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"unicode/utf8"
)

type recordingSearchRunner struct {
	sqlRunner
	queries []string
}

func (runner *recordingSearchRunner) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	runner.queries = append(runner.queries, query)
	return runner.sqlRunner.QueryContext(ctx, query, args...)
}

func TestCompactSearchPullBoundsAttributesValuesAndQueries(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "a/many", Many()); err != nil {
		t.Fatal(err)
	}
	manyValues := make([]any, 40)
	entity := E{
		"id":          "compact/target",
		"a/many":      manyValues,
		"search/text": "compact window needle",
		"z/hidden":    "outside the compact pull",
	}
	for index := range 40 {
		manyValues[index] = int64(index)
	}
	for index := range 31 {
		entity[fmt.Sprintf("a/%02d", index)] = int64(index)
	}
	_, err := db.Transact(ctx, entity)
	if err != nil {
		t.Fatal(err)
	}
	entityID := db.store.names["compact/target"]

	if err := db.withRead(ctx, func(runner sqlRunner) error {
		recorded := &recordingSearchRunner{sqlRunner: runner}
		pull, pullErr := db.pullEntityCompact(ctx, recorded, entityID)
		if pullErr != nil {
			return pullErr
		}
		if len(pull) != 32 {
			t.Fatalf("compact pull has %d attributes: %#v", len(pull), pull)
		}
		for index := range 31 {
			if pull[fmt.Sprintf("a/%02d", index)] != int64(index) {
				t.Fatalf("compact pull omitted a/%02d: %#v", index, pull)
			}
		}
		many, ok := pull["a/many"].([]any)
		if !ok || len(many) != 32 {
			t.Fatalf("compact many value = %#v", pull["a/many"])
		}
		attributeQueries, valueQueries := 0, 0
		for _, query := range recorded.queries {
			if strings.Contains(query, "GROUP BY f.a,i.name ORDER BY i.name COLLATE BINARY LIMIT ?") {
				attributeQueries++
			}
			if strings.Contains(query, "f.e=? AND f.a=?") && strings.Contains(query, "ORDER BY f.id LIMIT ?") {
				valueQueries++
			}
		}
		if attributeQueries != 1 || valueQueries != 32 {
			t.Fatalf("compact pull query shape: attributes=%d values=%d queries=%#v", attributeQueries, valueQueries, recorded.queries)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSearchValidationFilterAndCandidateLimits(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "search/vector", Type("vector")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "search/link", Ref()); err != nil {
		t.Fatal(err)
	}
	items := make([]any, 0, 55)
	for index := range 55 {
		items = append(items, E{
			"id": fmt.Sprintf("candidate-%02d", index), "search/text": "common token",
			"search/vector": Vector([]float32{1, float32(index + 1)}), "search/group": int64(index % 2),
		})
	}
	if _, err := db.Transact(ctx, items); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "linked", "search/text": "linked", "search/link": RefTo("candidate-00")}); err != nil {
		t.Fatal(err)
	}

	for _, options := range []SearchOpts{
		{},
		{Text: "  "},
		{Text: "x", K: -1},
		{Text: "x", Expand: -1},
		{Vector: []float32{float32(math.NaN())}},
		{Vector: []float32{float32(math.Inf(1))}},
	} {
		if _, err := db.Search(ctx, options); !errors.Is(err, ErrType) {
			t.Errorf("invalid search %#v error = %v", options, err)
		}
	}
	if _, err := db.atTx(GenesisTx).Search(ctx, SearchOpts{Text: "x"}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("past search error = %v", err)
	}
	if _, err := db.Search(ctx, SearchOpts{Vector: []float32{1}, VectorAttribute: "search/vector"}); !errors.Is(err, ErrType) {
		t.Fatalf("dimension mismatch error = %v", err)
	}
	if _, err := db.Search(ctx, SearchOpts{Vector: []float32{1}, K: 100}); !errors.Is(err, ErrType) {
		t.Fatalf("unscoped vector error = %v", err)
	}

	filtered, err := db.Search(ctx, SearchOpts{Text: "common", K: 100, Filters: [][]any{{"search/group", int64(1)}}})
	if err != nil || len(filtered.Hits) != 27 {
		t.Fatalf("numeric filter = %d hits, %v", len(filtered.Hits), err)
	}
	if limited, err := db.Search(ctx, SearchOpts{Text: "common", K: 1}); err != nil || len(limited.Hits) != 1 {
		t.Fatalf("K limit = %+v, %v", limited, err)
	}
	for _, filter := range [][][]any{
		{{"search/group"}}, {{int64(1), int64(1)}}, {{"search/group", make(chan int)}},
	} {
		if _, err := db.Search(ctx, SearchOpts{Text: "common", Filters: filter}); !errors.Is(err, ErrType) {
			t.Errorf("invalid filter %#v error = %v", filter, err)
		}
	}
	if result, err := db.Search(ctx, SearchOpts{Text: "common", Filters: [][]any{{"missing/attr", int64(1)}}}); err != nil || len(result.Hits) != 0 {
		t.Errorf("missing attribute filter = %+v, %v", result, err)
	}
	if _, err := db.Search(ctx, SearchOpts{Text: "common", Filters: [][]any{{"search/link", RefTo("missing")}}}); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing reference filter error = %v", err)
	}
	if result, err := db.Search(ctx, SearchOpts{Text: "linked", Filters: [][]any{{"search/link", RefTo("candidate-00")}}, Expand: 2}); err != nil || len(result.Hits) != 1 {
		t.Fatalf("reference filter/expand = %+v, %v", result, err)
	}
	if result, err := db.Search(ctx, SearchOpts{Text: "linked", Filters: [][]any{{"search/link", RefTo(db.store.names["candidate-00"])}}}); err != nil || len(result.Hits) != 1 {
		t.Fatalf("numeric reference filter = %+v, %v", result, err)
	}

	fact := Fact{ID: 7}
	if got := appendUniqueFact([]Fact{fact}, fact); len(got) != 1 {
		t.Fatalf("duplicate matched fact = %#v", got)
	}
	if got := cosine([]float32{0}, []float32{1}); !math.IsInf(got, -1) {
		t.Fatalf("zero cosine = %v", got)
	}
	if got := cosineWithNorm([]float32{1}, []float32{1, 0}, 1); !math.IsInf(got, -1) {
		t.Fatalf("mismatched cosine = %v", got)
	}
	rawRanked, truncated := rankRawEntityCandidatesBounded([]rankedRawFact{
		{raw: rawFact{id: 3}, entity: 1, score: 1},
		{raw: rawFact{id: 2}, entity: 1, score: 2},
		{raw: rawFact{id: 1}, entity: 2, score: 2},
	}, 1)
	if !truncated || len(rawRanked) != 1 || rawRanked[0].raw.id != 1 {
		t.Fatalf("bounded raw ranking = %#v, truncated=%t", rawRanked, truncated)
	}
}

func TestSelectedUndeclaredVectorAttribute(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{
		"id": "implicit-vector", "search/implicit": Vector([]float32{1, 0}),
	}); err != nil {
		t.Fatal(err)
	}
	result, err := db.Search(ctx, SearchOpts{
		Vector: []float32{1, 0}, VectorAttribute: "search/implicit", K: 1,
	})
	if err != nil || len(result.Hits) != 1 || result.Hits[0].Entity != "implicit-vector" {
		t.Fatalf("implicit vector search = %+v, %v", result, err)
	}
	if _, err := db.Transact(ctx, E{"id": "plain", "search/plain": "text"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Search(ctx, SearchOpts{
		Vector: []float32{1, 0}, VectorAttribute: "search/plain",
	}); !errors.Is(err, ErrType) {
		t.Fatalf("selected undeclared non-vector error = %v", err)
	}
}

func TestKeywordSearchContinuesPastFilteredPages(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	items := make([]any, 1_300)
	for index := range items {
		items[index] = E{
			"id":               fmt.Sprintf("paged-%03d", index),
			"search/text":      "common paged token",
			"search/alternate": "common paged token",
			"search/group":     index >= 299,
		}
	}
	if _, err := db.Transact(ctx, items); err != nil {
		t.Fatal(err)
	}

	result, err := db.Search(ctx, SearchOpts{
		Text: "common paged", K: 1, Filters: [][]any{{"search/group", true}},
	})
	if err != nil || len(result.Hits) != 1 || result.Hits[0].Entity != "paged-299" {
		t.Fatalf("filtered paged keyword search = %#v, %v", result, err)
	}
	selected, err := db.Search(ctx, SearchOpts{
		Text: "common paged", TextAttributes: []string{"search/alternate"}, K: 2,
	})
	if err != nil || len(selected.Hits) != 2 {
		t.Fatalf("selected paged keyword search = %#v, %v", selected, err)
	}
}

func TestSearchFilterReadValueResolutionBoundaries(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{"id": "known-filter-ref"}); err != nil {
		t.Fatal(err)
	}
	err := db.withRead(ctx, func(runner sqlRunner) error {
		if value, valueErr := db.resolveReadFactValue(ctx, runner, "filter/ref", attributeSchema{typeName: "ref"}, RefTo("known-filter-ref")); valueErr != nil || value.tag != TagRef {
			t.Errorf("known reference = %+v, %v", value, valueErr)
		}
		for _, target := range []any{"missing-filter-ref", true} {
			if _, valueErr := db.resolveReadFactValue(ctx, runner, "filter/ref", attributeSchema{typeName: "ref"}, RefTo(target)); valueErr == nil {
				t.Errorf("invalid reference target %#v accepted", target)
			}
		}
		if _, valueErr := db.resolveReadFactValue(ctx, runner, "filter/text", attributeSchema{typeName: "text"}, int64(1)); !errors.Is(valueErr, ErrType) {
			t.Errorf("typed filter mismatch = %v", valueErr)
		}
		if value, valueErr := db.resolveReadFactValue(ctx, runner, "filter/vector", attributeSchema{typeName: "vector", dims: 2, dimsSet: true}, Vector([]float32{1, 0})); valueErr != nil || value.tag != TagVector {
			t.Errorf("valid vector filter = %+v, %v", value, valueErr)
		}
		if _, valueErr := db.resolveReadFactValue(ctx, runner, "filter/vector", attributeSchema{typeName: "vector", dims: 2, dimsSet: true}, Vector([]float32{1})); !errors.Is(valueErr, ErrType) {
			t.Errorf("vector filter dimensions = %v", valueErr)
		}
		if _, valueErr := db.resolveReadFactValue(ctx, runner, "filter/value", attributeSchema{}, make(chan int)); !errors.Is(valueErr, ErrType) {
			t.Errorf("unsupported filter value = %v", valueErr)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestKeywordCandidateTruncationRequiresHiddenEntity(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	items := make([]any, searchCandidateLimit)
	for index := range items {
		items[index] = E{"id": fmt.Sprintf("exact-%02d", index), "search/text": "exact boundary token"}
	}
	if _, err := db.Transact(ctx, items); err != nil {
		t.Fatal(err)
	}
	exact, err := db.Search(ctx, SearchOpts{Text: "exact boundary", K: 10})
	if err != nil {
		t.Fatal(err)
	}
	if exact.Truncated {
		t.Fatal("exactly 50 eligible entities reported hidden candidate loss")
	}
	if _, transactErr := db.Transact(ctx, E{"id": "hidden-50", "search/text": "exact boundary token"}); transactErr != nil {
		t.Fatal(transactErr)
	}
	hidden, err := db.Search(ctx, SearchOpts{Text: "exact boundary", K: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !hidden.Truncated {
		t.Fatal("51st eligible entity did not report candidate truncation")
	}
}

func TestMatchedTextAndSnippetUseExactUTF8ByteLimit(t *testing.T) {
	oversized := strings.Repeat("é", maxMatchedValueBytes)
	got := boundMatchedFact(Fact{V: oversized, Snippet: oversized})
	value, ok := got.V.(string)
	if !ok || len(value) > maxMatchedValueBytes || !utf8.ValidString(value) || !got.ValueTruncated {
		t.Fatalf("bounded matched value = %#v", got)
	}
	if len(got.Snippet) > maxMatchedValueBytes || !utf8.ValidString(got.Snippet) || !got.SnippetTruncated {
		t.Fatalf("bounded matched snippet = %#v", got)
	}
	vector := boundMatchedFact(Fact{Tag: TagVector, V: map[string]any{"vector_dims": int64(3)}})
	renderedVector, ok := vector.V.(map[string]any)
	if !ok || renderedVector["vector_dims"] != 3 {
		t.Fatalf("int64 vector dimensions = %#v", vector)
	}
}

func TestVectorSearchDoesNotReloadJoinedBlobs(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "search/vector-joined", Type("vector")); err != nil {
		t.Fatal(err)
	}
	items := make([]any, 80)
	for index := range items {
		items[index] = E{
			"id":                   fmt.Sprintf("joined-%02d", index),
			"search/vector-joined": Vector([]float32{1, float32(index + 1)}),
		}
	}
	if _, err := db.Transact(ctx, items); err != nil {
		t.Fatal(err)
	}
	if err := db.withRead(ctx, func(runner sqlRunner) error {
		counted := &countingQueryRunner{sqlRunner: runner}
		batch, err := db.vectorCandidatesBounded(
			ctx, counted, []float32{1, 1}, "search/vector-joined", nil, 50, &searchWork{limit: 1_000},
		)
		if err != nil {
			return err
		}
		if len(batch.items) != 50 || !batch.truncated {
			t.Fatalf("vector batch = %d, truncated=%t", len(batch.items), batch.truncated)
		}
		if counted.blobQueries != 0 {
			t.Fatalf("vector search reloaded %d blobs after candidate scan", counted.blobQueries)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBoundedVectorRankingRetainsOnlyTheCandidateWindow(t *testing.T) {
	items := rankedRawHeap{}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("ranked heap accepted an invalid element")
			}
		}()
		items.Push("not a ranked candidate")
	}()
	heap.Push(&items, rankedRawFact{raw: rawFact{id: 9}, entity: 9, score: 0.5})
	popped, ok := heap.Pop(&items).(rankedRawFact)
	if !ok || popped.raw.id != 9 || len(items) != 0 {
		t.Fatalf("ranked heap pop = %#v, remaining=%#v", popped, items)
	}

	ranking := newBoundedRawRanking(2)
	for _, candidate := range []rankedRawFact{
		{raw: rawFact{id: 4}, entity: 4, score: 0.1},
		{raw: rawFact{id: 3}, entity: 3, score: 0.9},
		{raw: rawFact{id: 2}, entity: 2, score: 0.9},
		{raw: rawFact{id: 1}, entity: 1, score: 0.8},
	} {
		ranking.add(candidate)
	}

	result, truncated := ranking.result()
	if !truncated || len(ranking.heap) != 2 || len(result) != 2 {
		t.Fatalf("bounded ranking retained %d candidates, result=%d truncated=%t", len(ranking.heap), len(result), truncated)
	}
	if result[0].raw.id != 2 || result[1].raw.id != 3 {
		t.Fatalf("bounded ranking order = %#v", result)
	}
}
