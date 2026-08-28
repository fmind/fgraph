package fgraph

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
)

func TestQueryParserAndPrimitiveMatrix(t *testing.T) {
	if _, err := ParseQuery(map[string]any{"find": make(chan int)}); !errors.Is(err, ErrQuery) {
		t.Fatalf("unencodable query error = %v", err)
	}
	if _, err := ParseQuery(map[string]any{"find": []any{"?e"}, "where": []any{}, "limti": 1}); !errors.Is(err, ErrQuery) {
		t.Fatalf("unknown query field error = %v", err)
	}
	if _, err := ParseQuery(map[string]any{"find": "?x", "where": []any{}}); !errors.Is(err, ErrQuery) {
		t.Fatalf("invalid query fields error = %v", err)
	}
	db := fixedDB(t, ":memory:")
	if _, err := db.QueryJSON(context.Background(), map[string]any{"find": "?x"}, nil); !errors.Is(err, ErrQuery) {
		t.Fatalf("QueryJSON parser error = %v", err)
	}

	for _, test := range []struct {
		clause []any
		want   bool
	}{{[]any{"=", 1, 1}, true}, {[]any{"bogus", 1, 1}, false}, {[]any{1, 2, 3}, false}, {[]any{"="}, false}} {
		if got := isPredicateClause(test.clause); got != test.want {
			t.Errorf("isPredicateClause(%v) = %t", test.clause, got)
		}
	}

	for _, test := range []struct {
		left    cell
		right   cell
		op      string
		want    bool
		wantErr bool
	}{
		{op: "=", left: cell{tag: TagInt, value: int64(1)}, right: cell{tag: TagInt, value: int64(1)}, want: true},
		{op: "=", left: cell{tag: TagInt, value: int64(1)}, right: cell{tag: TagFloat, value: float64(1)}, want: true},
		{op: "!=", left: cell{tag: TagInt, value: int64(1)}, right: cell{tag: TagFloat, value: float64(1)}, want: false},
		{op: "=", left: cell{tag: TagRef, value: int64(65)}, right: cell{tag: TagInt, value: int64(65)}, want: false},
		{op: "!=", left: cell{tag: TagRef, value: int64(65)}, right: cell{tag: TagInt, value: int64(65)}, want: true},
		{op: "contains", left: cell{tag: TagText, value: "alpha"}, right: cell{tag: TagText, value: "ph"}, want: true},
		{op: "starts-with", left: cell{tag: TagText, value: "alpha"}, right: cell{tag: TagText, value: "al"}, want: true},
		{op: "contains", left: cell{tag: TagInt, value: int64(1)}, right: cell{tag: TagText, value: "1"}, wantErr: true},
		{op: "<", left: cell{tag: TagInt, value: int64(1)}, right: cell{tag: TagFloat, value: float64(2)}, want: true},
		{op: "<=", left: cell{tag: TagText, value: "a"}, right: cell{tag: TagText, value: "a"}, want: true},
		{op: ">", left: cell{tag: TagFloat, value: float64(2)}, right: cell{tag: TagInt, value: int64(1)}, want: true},
		{op: ">=", left: cell{tag: TagText, value: "b"}, right: cell{tag: TagText, value: "a"}, want: true},
		{op: "<", left: cell{tag: TagRef, value: int64(65)}, right: cell{tag: TagInt, value: int64(66)}, wantErr: true},
		{op: "<", left: cell{tag: TagBool, value: true}, right: cell{tag: TagText, value: "a"}, wantErr: true},
		{op: "bogus", left: cell{}, right: cell{}, wantErr: true},
	} {
		got, err := compareCells(test.op, test.left, test.right)
		if got != test.want || (err != nil) != test.wantErr {
			t.Errorf("compareCells(%s) = %t,%v", test.op, got, err)
		}
	}
	for _, value := range []any{int64(1), float64(1), float32(1)} {
		if _, ok := numeric(value); !ok {
			t.Errorf("numeric(%T) rejected", value)
		}
	}
	if _, ok := numeric("1"); ok {
		t.Fatal("text accepted as numeric")
	}
	if got, ok := orderedCompare("a", "b"); !ok || got >= 0 {
		t.Fatalf("ordered text = %d,%t", got, ok)
	}
	if got, ok := orderedCompare(false, true); !ok || got >= 0 {
		t.Fatalf("ordered bool = %d,%t", got, ok)
	}
	if predicateCellsEqual(
		cell{tag: TagInt, value: "malformed"},
		cell{tag: TagInt, value: "malformed"},
	) {
		t.Fatal("malformed numeric cells compared equal")
	}
}

func TestQueryClauseAndProjectionErrors(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{"id": "rule-seed", "node/name": "seed"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "node/link", Ref(), Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "a", "node/name": "Alpha", "node/value": 1, "node/link": []any{RefTo("b")}},
		E{"id": "b", "node/name": "Beta", "node/value": 2},
	}); err != nil {
		t.Fatal(err)
	}

	assertQueryError := func(query Q, args map[string]any) {
		t.Helper()
		if _, err := db.Query(ctx, query, args); !errors.Is(err, ErrQuery) {
			t.Fatalf("query %#v error = %v", query, err)
		}
	}
	baseFind := []any{"?e"}
	for _, where := range [][]any{
		{[]any{"?e", "node/name"}},
		{[]any{"?e", 1, "?v"}},
		{[]any{true, "node/name", "Alpha"}},
		{[]any{"?e", "node/value", "?v"}, []any{"contains", "?v", "x"}},
		{[]any{"?e", "node/value", "?v"}, []any{"<", "?v", true}},
		{Object{Fields: nil}},
		{Object{Fields: []Field{{Name: "not", Value: true}}}},
		{Object{Fields: []Field{{Name: "or", Value: true}}}},
		{Object{Fields: []Field{{Name: "rule", Value: true}}}},
		{Object{Fields: []Field{{Name: "unknown", Value: []any{}}}}},
		{Object{Fields: []Field{{Name: "or", Value: []any{}}}}},
		{Object{Fields: []Field{{Name: "or", Value: []any{true}}}}},
	} {
		assertQueryError(Q{Find: baseFind, Where: where}, nil)
	}
	assertQueryError(Q{Find: []any{"?e"}, Where: []any{[]any{"=", "?missing", 1}}}, nil)
	assertQueryError(Q{Find: []any{"?e"}, Where: []any{[]any{"?e", "node/name", "_"}}, In: []string{"?input"}}, nil)
	assertQueryError(Q{Find: []any{"?e"}, Where: []any{[]any{"?e", "node/name", "_"}}, Offset: -1}, nil)
	assertQueryError(Q{Find: []any{"?e"}, Where: []any{[]any{"?e", "node/name", "_"}}, Limit: queryLimit(-1)}, nil)
	for _, order := range [][]any{{true}, {[]any{"?e"}}, {[]any{"?e", "sideways"}}, {[]any{"?missing", "asc"}}} {
		assertQueryError(Q{Find: []any{"?e"}, Where: []any{[]any{"?e", "node/name", "_"}}, Order: order}, nil)
	}
	for _, find := range [][]any{
		{true},
		{[]any{"pull", 1, []any{"*"}}},
		{[]any{"pull", "?v", []any{"*"}}},
		{[]any{"pull", "?e", true}},
		{[]any{"pull", "?e", []any{true}}},
		{[]any{"pull", "?e", []any{map[string]any{"node/link": true}}}},
		{[]any{"pull", "?e", []any{map[string]any{"BAD": []any{}}}}},
		{[]any{"pull", "?e", []any{map[string]any{"node/name": []any{"node/name"}}}}},
	} {
		where := []any{[]any{"?e", "node/value", "?v"}}
		assertQueryError(Q{Find: find, Where: where}, nil)
	}

	empty, err := db.Query(ctx, Q{Find: []any{
		[]any{"count", "?e"}, []any{"sum", "?e"}, []any{"avg", "?e"}, []any{"min", "?e"}, []any{"max", "?e"},
	}, Where: []any{[]any{"?e", "missing/attr", "_"}}}, nil)
	if err != nil || len(empty.Rows) != 1 || empty.Rows[0][0] != int64(0) ||
		empty.Rows[0][1] != nil || empty.Rows[0][2] != nil || empty.Rows[0][3] != nil || empty.Rows[0][4] != nil {
		t.Fatalf("empty aggregates = %#v, %v", empty, err)
	}
	paged, err := db.Query(ctx, Q{Find: []any{"?e"}, Where: []any{[]any{"?e", "node/name", "_"}}, Offset: 99}, nil)
	if err != nil || len(paged.Rows) != 0 {
		t.Fatalf("offset past end = %#v, %v", paged, err)
	}
	ordered, err := db.Query(ctx, Q{
		Find:  []any{"?name"},
		Where: []any{[]any{"?e", "node/name", "?name"}, []any{"?e", "node/value", "?value"}},
		Order: []any{[]any{"?value", "desc"}},
	}, nil)
	if err != nil || len(ordered.Rows) != 2 || ordered.Rows[0][0] != "Beta" {
		t.Fatalf("unprojected numeric order = %#v, %v", ordered, err)
	}
	assertQueryError(Q{
		Find: []any{"?e"}, Where: []any{
			[]any{"?e", "node/name", "_"},
			Object{Fields: []Field{{Name: "not", Value: []any{[]any{"?other", "node/name", "Beta"}}}}},
		},
	}, nil)
	assertQueryError(Q{
		Find:  []any{[]any{"count", "?e"}, []any{"pull", "?e", []any{"*"}}},
		Where: []any{[]any{"?e", "node/name", "_"}},
	}, nil)
	assertQueryError(Q{
		Find:  []any{[]any{"pull", "?e", []any{"INVALID"}}},
		Where: []any{[]any{"?e", "node/name", "_"}},
	}, nil)
}

func TestQueryAggregateAndRuleErrorMatrix(t *testing.T) {
	for _, test := range []struct {
		name   string
		values []cell
	}{
		{name: "sum", values: []cell{{tag: TagText, value: "x"}}},
		{name: "avg", values: []cell{{tag: TagText, value: "x"}}},
		{name: "min", values: []cell{{tag: TagText, value: "x"}}},
		{name: "max", values: []cell{{tag: TagInt, value: int64(1)}, {tag: TagBool, value: true}}},
		{name: "unknown"},
	} {
		if _, err := aggregate(test.name, test.values); !errors.Is(err, ErrQuery) {
			t.Errorf("aggregate %s error = %v", test.name, err)
		}
	}
	for _, name := range []string{"sum", "avg"} {
		if _, err := aggregate(name, []cell{{tag: TagFloat, value: math.MaxFloat64}, {tag: TagFloat, value: math.MaxFloat64}}); !errors.Is(err, ErrQuery) {
			t.Fatalf("non-finite float %s error = %v", name, err)
		}
	}
	if _, err := aggregate("sum", []cell{
		{tag: TagInt, value: int64(math.MaxInt64)}, {tag: TagInt, value: int64(1)},
	}); !errors.Is(err, ErrQuery) {
		t.Fatalf("overflowing integer sum error = %v", err)
	}
	for _, raw := range [][]any{{true}, {[]any{"?x"}}, {[]any{"?x", "bad"}}, {[]any{"?missing", "asc"}}} {
		if _, err := parseOrderBound(raw, []string{"?x"}, nil, true); !errors.Is(err, ErrQuery) {
			t.Errorf("parseOrder(%v) error = %v", raw, err)
		}
	}

	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	assertRuleError := func(rules []any, where Object) {
		t.Helper()
		query := Q{Find: []any{"?x"}, Where: []any{where}, Rules: rules}
		if _, err := db.Query(ctx, query, nil); !errors.Is(err, ErrQuery) {
			t.Fatalf("rules %#v error = %v", rules, err)
		}
	}
	invoke := func(items ...any) Object { return Object{Fields: []Field{{Name: "rule", Value: items}}} }
	for _, rules := range [][]any{
		{true},
		{map[string]any{"head": true, "body": []any{}}},
		{map[string]any{"head": []any{1}, "body": []any{}}},
		{map[string]any{"head": []any{"r", "x"}, "body": []any{}}},
	} {
		assertRuleError(rules, invoke("r", "?x"))
	}
	assertRuleError(nil, invoke())
	assertRuleError(nil, invoke(1, "?x"))
	assertRuleError(nil, invoke("missing", "?x"))
	assertRuleError([]any{map[string]any{
		"head": []any{"r", "?x"}, "body": []any{[]any{"?e", "node/name", "?x"}},
	}}, invoke("r"))
	assertRuleError([]any{
		map[string]any{"head": []any{"a", "?x"}, "body": []any{invoke("b", "?x")}},
		map[string]any{"head": []any{"b", "?x"}, "body": []any{invoke("a", "?x")}},
	}, invoke("a", "?x"))
	assertRuleError([]any{
		map[string]any{"head": []any{"r", "?x"}, "body": []any{[]any{"?e", "node/name", "?x"}}},
		map[string]any{"head": []any{"r", "?x", "?y"}, "body": []any{[]any{"?e", "node/name", "?x"}}},
	}, invoke("r", "?x"))
	assertRuleError([]any{
		map[string]any{"head": []any{"a", "?x"}, "body": []any{invoke("missing", "?x")}},
	}, invoke("a", "?x"))
	assertRuleError([]any{
		map[string]any{"head": []any{"r", "?x"}, "body": []any{}},
	}, invoke("r", "?x"))
}

func TestQueryInternalBranchMatrix(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "node/link", Ref(), Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "a", "node/name": "A", "node/value": 1, "node/link": []any{RefTo("b")}},
		E{"id": "b", "node/name": "B", "node/value": 2, "node/link": []any{RefTo("a")}},
	}); err != nil {
		t.Fatal(err)
	}
	var evaluator *queryEvaluator
	if err := db.withRead(ctx, func(runner sqlRunner) error {
		evaluator = &queryEvaluator{db: db, ctx: ctx, runner: runner, relations: map[string][][]cell{}, rules: map[string][]ruleDef{}}
		for _, value := range []any{
			true, int64(1), float64(1.5), "text", Instant(1), Bytes([]byte{1}), Vector([]float32{1}), JSON(E{"x": 1}),
			RefTo("a"), RefTo("missing"), RefTo(db.store.names["a"]), RefTo(int(db.store.names["a"])),
		} {
			if _, err := evaluator.constantCell(value); err != nil {
				t.Errorf("constantCell(%#v) = %v", value, err)
			}
		}
		for _, value := range []any{make(chan int), RefTo(true)} {
			if _, err := evaluator.constantCell(value); !errors.Is(err, ErrQuery) {
				t.Errorf("invalid constantCell(%#v) = %v", value, err)
			}
		}
		if _, err := evaluator.matchTerm(make(chan int), cell{}, binding{}, false); !errors.Is(err, ErrQuery) {
			t.Errorf("invalid value match error = %v", err)
		}
		if _, err := evaluator.matchTerm(true, cell{tag: TagRef, value: int64(1)}, binding{}, true); !errors.Is(err, ErrQuery) {
			t.Errorf("invalid entity match error = %v", err)
		}
		if _, err := evaluator.evalPattern([]any{true, "node/name", "A"}, []binding{{}}); !errors.Is(err, ErrQuery) {
			t.Errorf("pattern entity error = %v", err)
		}
		if _, err := evaluator.evalPattern([]any{"?e", "node/name", make(chan int)}, []binding{{}}); !errors.Is(err, ErrQuery) {
			t.Errorf("pattern value error = %v", err)
		}
		for _, clause := range [][]any{
			{"=", make(chan int), 1}, {"=", 1, make(chan int)}, {"contains", 1, "x"},
		} {
			if _, err := evaluator.evalPredicate(clause, []binding{{}}); !errors.Is(err, ErrQuery) {
				t.Errorf("predicate %#v error = %v", clause, err)
			}
		}
		if _, err := evaluator.evalNot([]any{[]any{"?e", "bad", "x"}}, []binding{{"?e": {tag: TagRef, value: int64(1)}}}); !errors.Is(err, ErrQuery) {
			t.Errorf("not inner error = %v", err)
		}
		if _, err := evaluator.evalOr([]any{[]any{[]any{"?e", "bad", "x"}}}, []binding{{}}); !errors.Is(err, ErrQuery) {
			t.Errorf("or inner error = %v", err)
		}
		if got, err := evaluator.evalOr([]any{
			[]any{[]any{"?e", "node/name", "A"}}, []any{[]any{"?e", "node/name", "A"}},
		}, []binding{{}}); err != nil || len(got) != 1 {
			t.Errorf("or distinct = %#v, %v", got, err)
		}

		if _, _, err := evaluator.projectRow([]any{"?missing"}, binding{}); !errors.Is(err, ErrQuery) {
			t.Errorf("unbound projection error = %v", err)
		}
		for _, find := range [][]any{
			{true},
			{[]any{"pull", true, []any{"*"}}},
			{[]any{"pull", "?e", []any{"*"}}},
			{[]any{"pull", "?e", true}},
		} {
			if _, _, err := evaluator.projectRow(find, binding{"?e": {tag: TagInt, value: int64(1)}}); !errors.Is(err, ErrQuery) {
				t.Errorf("invalid projectRow %#v error = %v", find, err)
			}
		}
		aID := db.store.names["a"]
		if all, err := evaluator.pullPattern(aID, []any{"*"}, 1, map[int64]bool{}); err != nil || all["node/name"] != "A" {
			t.Errorf("pull all = %#v, %v", all, err)
		}
		if missing, err := evaluator.pullPattern(aID, []any{"missing/attr"}, 1, map[int64]bool{}); err != nil || len(missing) != 0 {
			t.Errorf("pull missing = %#v, %v", missing, err)
		}
		if _, err := evaluator.pullPattern(aID, []any{map[string]any{"node/link": true}}, 1, map[int64]bool{}); !errors.Is(err, ErrQuery) {
			t.Errorf("nested pull type error = %v", err)
		}
		if cycle, err := evaluator.pullPattern(aID, []any{map[string]any{"node/link": []any{map[string]any{"node/link": []any{"node/name"}}}}}, 1, map[int64]bool{aID: true}); err != nil || cycle == nil {
			t.Errorf("cycle pull = %#v, %v", cycle, err)
		}

		if _, _, err := evaluator.aggregateRow([]any{[]any{"count", true}}, []binding{{}}); !errors.Is(err, ErrQuery) {
			t.Errorf("aggregate variable error = %v", err)
		}
		if _, _, err := evaluator.aggregateRow([]any{[]any{"sum", "?v"}}, []binding{{"?v": {tag: TagText, value: "x"}}}); !errors.Is(err, ErrQuery) {
			t.Errorf("aggregate row value error = %v", err)
		}

		evaluator.rules["bad"] = []ruleDef{{name: "bad", args: []string{"?x"}, body: []any{[]any{"?e", "bad", "?x"}}}}
		if err := evaluator.deriveRelation("bad"); !errors.Is(err, ErrQuery) {
			t.Errorf("derive relation clause error = %v", err)
		}
		evaluator.rules["unbound"] = []ruleDef{{name: "unbound", args: []string{"?x"}, body: []any{[]any{"?e", "node/name", "A"}}}}
		if err := evaluator.deriveRelation("unbound"); !errors.Is(err, ErrQuery) {
			t.Errorf("derive relation head error = %v", err)
		}
		evaluator.rules["r"] = []ruleDef{{name: "r", args: []string{"?x"}}}
		evaluator.relations["r"] = [][]cell{{{tag: TagRef, value: aID}}}
		if _, err := evaluator.evalRule([]any{"r", true}, []binding{{}}); !errors.Is(err, ErrQuery) {
			t.Errorf("rule term error = %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Query(ctx, Q{Find: []any{"?x"}, In: []string{"?x"}}, map[string]any{"?x": make(chan int)}); !errors.Is(err, ErrQuery) {
		t.Fatalf("invalid input constant error = %v", err)
	}
	if result, err := db.Qry(ctx, Q{Find: []any{"?x"}, In: []string{"?x"}, Limit: queryLimit(1)}, map[string]any{"?x": 1}); err != nil || len(result.Rows) != 1 {
		t.Fatalf("Qry alias = %#v, %v", result, err)
	}
	if reflectStrings([]string{"a"}, []string{"b"}) || reflectStrings([]string{"a"}, []string{"a", "b"}) {
		t.Fatal("different string slices compared equal")
	}
	if allAggregates([]any{[]any{"count", "?x"}, "?x"}) {
		t.Fatal("mixed projection reported all aggregates")
	}
	for _, item := range []any{true, []any{"pull", "?e", []any{"*"}}, []any{"bogus", "?e"}} {
		_ = findLabel(item)
		_ = findAggregate(item)
	}
}

func TestQueryStructuralKeysCannotCollideOnTextDelimiters(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "key/a", Type("text"), Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "key/b", Type("text"), Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "key-a", "key/a": []any{"x", "x;?b=4:y"}},
		E{"id": "key-b", "key/b": []any{"z", "y;?b=4:z"}},
	}); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query(ctx, Q{
		Find: []any{"?a", "?b"},
		Where: []any{Object{Fields: []Field{{Name: "or", Value: []any{
			[]any{
				[]any{"?ea", "key/a", "?a"},
				[]any{"?eb", "key/b", "?b"},
			},
		}}}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 4 {
		t.Fatalf("delimiter-like values produced %d Cartesian rows: %#v", len(result.Rows), result.Rows)
	}
}

func TestAggregateProjectionUsesRenderedSetSemantics(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "number/value", Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "numbers", "number/value": []any{int64(1), float64(1)}}); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query(ctx, Q{
		Find:  []any{"?value", []any{"count", "?entity"}},
		Where: []any{[]any{"?entity", "number/value", "?value"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != int64(1) || result.Rows[0][1] != int64(1) {
		t.Fatalf("rendered aggregate set = %#v", result.Rows)
	}
}

func TestUndeclaredAttributeConstantsUseValueIndex(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", WithClock(func() int64 { return 1_767_225_600_000_000 }), WithQueryBudget(3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	})
	items := make([]any, 100)
	for index := range items {
		items[index] = E{"id": fmt.Sprintf("item/%d", index), "item/group": int64(index)}
	}
	if _, transactErr := db.Transact(ctx, items); transactErr != nil {
		t.Fatal(transactErr)
	}
	result, err := db.Query(ctx, Q{Find: []any{"?entity"}, Where: []any{[]any{"?entity", "item/group", int64(77)}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || fmt.Sprint(result.Rows[0][0]) != "map[ref:item/77]" {
		t.Fatalf("indexed result = %#v", result.Rows)
	}
}

func TestPullQueryStreamsTargetFactsAndChargesWork(t *testing.T) {
	ctx := context.Background()
	db, openErr := Open(":memory:", WithClock(func() int64 { return 1_767_225_600_000_000 }), WithQueryBudget(3))
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { closeTest(t, db) })
	if _, declareErr := db.Declare(ctx, "unrelated/count", Type("int")); declareErr != nil {
		t.Fatal(declareErr)
	}
	if _, transactErr := db.Transact(ctx, E{"id": "unrelated", "unrelated/count": int64(1)}); transactErr != nil {
		t.Fatal(transactErr)
	}
	if _, transactErr := db.Transact(ctx, E{"id": "pull/target", "pull/name": "target", "pull/enabled": true}); transactErr != nil {
		t.Fatal(transactErr)
	}
	if _, execErr := db.store.sql.ExecContext(ctx, `UPDATE fgraph_facts SET v='corrupt' WHERE a=(SELECT id FROM fgraph_ids WHERE name='unrelated/count')`); execErr != nil {
		t.Fatal(execErr)
	}

	query := Q{
		Find:  []any{[]any{"pull", "?entity", []any{"*"}}},
		Where: []any{[]any{"?entity", "pull/name", "target"}},
	}
	result, queryErr := db.Query(ctx, query, nil)
	if queryErr != nil || len(result.Rows) != 1 {
		t.Fatalf("targeted pull = %#v, %v", result, queryErr)
	}

	db.store.queryBudget = 2 // One candidate plus two projected facts needs three work units.
	if _, budgetErr := db.Query(ctx, query, nil); !errors.Is(budgetErr, ErrTooLarge) {
		t.Fatalf("pull work budget error = %v, want TooLarge", budgetErr)
	}
}
