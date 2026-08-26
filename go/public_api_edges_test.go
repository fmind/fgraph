package fgraph

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestQuerySourceParsingAndHistoricalMaterialization(t *testing.T) {
	ctx := context.Background()
	for name, value := range map[string]any{
		"array query":    []any{},
		"numeric source": map[string]any{"source": int64(1)},
		"unknown source": map[string]any{"source": "archive"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseQuery(value); !errors.Is(err, ErrQuery) {
				t.Fatalf("ParseQuery(%#v) error = %v, want QueryError", value, err)
			}
		})
	}
	parsed, parseErr := ParseQuery(map[string]any{
		"find": []any{"?value"}, "where": []any{[]any{"source/entity", "source/value", "?value"}},
		"source": "history",
	})
	if parseErr != nil || parsed.Source != "history" {
		t.Fatalf("history query = %#v, %v", parsed, parseErr)
	}

	db := fixedDB(t, ":memory:")
	first, firstErr := db.Transact(ctx, E{"id": "source/entity", "source/value": "old"})
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	second, secondErr := db.Transact(ctx, E{"id": "source/entity", "source/value": "new"})
	if secondErr != nil {
		t.Fatal(secondErr)
	}
	if _, err := db.Query(ctx, Q{Find: []any{"?e"}, Where: []any{[]any{"?e", "source/value", "_"}}, Source: "archive"}, nil); !errors.Is(err, ErrQuery) {
		t.Fatalf("invalid public query source error = %v", err)
	}

	var current, history, historical []queryFact
	if err := db.withRead(ctx, func(runner sqlRunner) error {
		var readErr error
		current, readErr = db.queryFacts(ctx, runner)
		if readErr != nil {
			return readErr
		}
		history, readErr = db.queryFacts(ctx, runner, "history")
		return readErr
	}); err != nil {
		t.Fatal(err)
	}
	view, viewErr := db.At(ctx, first.Tx)
	if viewErr != nil {
		t.Fatal(viewErr)
	}
	if err := view.withRead(ctx, func(runner sqlRunner) error {
		var readErr error
		historical, readErr = view.queryFacts(ctx, runner, "history")
		return readErr
	}); err != nil {
		t.Fatal(err)
	}

	matching := func(facts []queryFact) []queryFact {
		result := []queryFact{}
		for _, fact := range facts {
			if fact.a == "source/value" {
				result = append(result, fact)
			}
		}
		return result
	}
	current = matching(current)
	history = matching(history)
	historical = matching(historical)
	if len(current) != 1 || current[0].cell.value != "new" || !current[0].added {
		t.Fatalf("current materialized facts = %#v", current)
	}
	if len(history) != 3 || history[0].cell.value != "old" || !history[0].added || history[1].tx != second.Tx || history[1].added || history[2].cell.value != "new" {
		t.Fatalf("history materialized facts = %#v", history)
	}
	if len(historical) != 1 || historical[0].cell.value != "old" || !historical[0].added {
		t.Fatalf("historical materialized facts = %#v", historical)
	}
	if got := initialBindingNames(nil); len(got) != 0 {
		t.Fatalf("empty initial bindings = %#v", got)
	}
}

func TestPublicPullAndQueryRejectCorruptLogicalFacts(t *testing.T) {
	ctx := context.Background()
	if _, err := fixedDB(t, ":memory:").Pull(ctx, map[string]any{"eid": int64(1)}, []any{"*"}); !errors.Is(err, ErrType) {
		t.Fatalf("malformed pull selector error = %v, want TypeError", err)
	}

	newCorruptDB := func(t *testing.T) *DB {
		t.Helper()
		db := fixedDB(t, ":memory:")
		if _, err := db.Declare(ctx, "corrupt/count", Type("int")); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Transact(ctx, E{"id": "corrupt/entity", "corrupt/count": int64(1)}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.store.sql.ExecContext(ctx, `UPDATE fgraph_facts SET v='not-an-integer' WHERE a=(SELECT id FROM fgraph_ids WHERE name='corrupt/count')`); err != nil {
			t.Fatal(err)
		}
		return db
	}

	pullDB := newCorruptDB(t)
	if _, err := pullDB.Pull(ctx, "corrupt/entity", []any{"*"}); !errors.Is(err, ErrFormat) {
		t.Fatalf("corrupt Pull error = %v, want FormatError", err)
	}
	queryDB := newCorruptDB(t)
	if _, err := queryDB.Query(ctx, Q{
		Find:  []any{[]any{"pull", "?entity", []any{"*"}}},
		Where: []any{[]any{"?entity", "corrupt/count", "_"}},
	}, nil); !errors.Is(err, ErrFormat) {
		t.Fatalf("corrupt pull query error = %v, want FormatError", err)
	}
}

func TestQueryValidationPropagatesNestedClauseErrors(t *testing.T) {
	if err := validateFindItems([]any{[]any{"count", int64(1)}}); !errors.Is(err, ErrQuery) {
		t.Fatalf("invalid aggregate variable error = %v", err)
	}

	not := []any{
		[]any{"?entity", "node/name", "_"},
		Object{Fields: []Field{{Name: "not", Value: []any{
			[]any{"?entity", "node/name", "_"},
			[]any{"=", "?missing", "x"},
		}}}},
	}
	if err := validateClauseBindings(not, nil); !errors.Is(err, ErrQuery) {
		t.Fatalf("nested not validation error = %v", err)
	}
	orClause := Object{Fields: []Field{{
		Name: "or",
		Value: []any{
			[]any{[]any{"?entity", "node/name", "_"}},
			[]any{[]any{"=", "?missing", "x"}},
		},
	}}}
	or := []any{orClause}
	if err := validateClauseBindings(or, nil); !errors.Is(err, ErrQuery) {
		t.Fatalf("nested or validation error = %v", err)
	}

	if got := queryNeedsMaterializedFacts([]any{[]any{"pull", "?entity", []any{"*"}}}); !got {
		t.Fatal("pull query did not request materialized facts")
	}
	if got := queryNeedsMaterializedFacts([]any{"?entity"}); got {
		t.Fatal("scalar query unexpectedly requested materialized facts")
	}
	if !reflect.DeepEqual(initialBindingNames([]binding{{"?x": {}}, {"?x": {}}}), map[string]bool{"?x": true}) {
		t.Fatal("shared query input binding was not retained")
	}
}
