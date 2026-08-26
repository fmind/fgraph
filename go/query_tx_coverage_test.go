package fgraph

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestStoredCellCoversEveryPortableStorageTag(t *testing.T) {
	longText := strings.Repeat("t", BlobThreshold+1)
	longBytes := []byte(strings.Repeat("b", BlobThreshold+1))
	tests := []struct {
		name    string
		input   cell
		wantTag Tag
	}{
		{name: "ref", input: cell{tag: TagRef, value: int64(65)}, wantTag: TagRef},
		{name: "bool false", input: cell{tag: TagBool, value: false}, wantTag: TagBool},
		{name: "bool true", input: cell{tag: TagBool, value: true}, wantTag: TagBool},
		{name: "int", input: cell{tag: TagInt, value: int64(7)}, wantTag: TagInt},
		{name: "float", input: cell{tag: TagFloat, value: 1.25}, wantTag: TagFloat},
		{name: "inline text", input: cell{tag: TagText, value: "text"}, wantTag: TagText},
		{name: "indirect text", input: cell{tag: TagTextRef, value: longText}, wantTag: TagTextRef},
		{name: "instant", input: cell{tag: TagInstant, value: int64(1)}, wantTag: TagInstant},
		{name: "inline bytes", input: cell{tag: TagBytes, value: []byte{1, 2}}, wantTag: TagBytes},
		{name: "indirect bytes", input: cell{tag: TagBytesRef, value: longBytes}, wantTag: TagBytesRef},
		{name: "vector", input: cell{tag: TagVector, value: []float32{1, 2}}, wantTag: TagVector},
		{name: "json", input: cell{tag: TagJSON, value: E{"ok": true}}, wantTag: TagJSON},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := storedCell(test.input)
			if err != nil {
				t.Fatalf("storedCell(%#v): %v", test.input, err)
			}
			if got.tag != test.wantTag {
				t.Fatalf("stored tag = %d, want %d", got.tag, test.wantTag)
			}
			if got.storage == nil {
				t.Fatal("stored value has nil storage")
			}
		})
	}

	invalid := []struct {
		name  string
		input cell
	}{
		{name: "bool type", input: cell{tag: TagBool, value: "true"}},
		{name: "float type", input: cell{tag: TagFloat, value: "1.0"}},
		{name: "nonfinite float", input: cell{tag: TagFloat, value: math.NaN()}},
		{name: "text type", input: cell{tag: TagText, value: int64(1)}},
		{name: "invalid text", input: cell{tag: TagText, value: string([]byte{0xff})}},
		{name: "instant range", input: cell{tag: TagInstant, value: maxInstantMicros + 1}},
		{name: "bytes type", input: cell{tag: TagBytes, value: "bytes"}},
		{name: "vector type", input: cell{tag: TagVector, value: []float64{1}}},
		{name: "nonfinite vector", input: cell{tag: TagVector, value: []float32{float32(math.Inf(1))}}},
		{name: "json type", input: cell{tag: TagJSON, value: make(chan int)}},
		{name: "unknown tag", input: cell{tag: Tag(99), value: true}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if _, err := storedCell(test.input); err == nil {
				t.Fatalf("storedCell(%#v) unexpectedly succeeded", test.input)
			}
		})
	}
}

func TestIndexedQueriesUseDeclaredStorageAndRespectPlannerBarriers(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	declarations := []struct {
		attr string
		opts []DeclareOption
	}{
		{attr: "query/bool", opts: []DeclareOption{Type("bool")}},
		{attr: "query/int", opts: []DeclareOption{Type("int")}},
		{attr: "query/float", opts: []DeclareOption{Type("float")}},
		{attr: "query/text", opts: []DeclareOption{Type("text")}},
		{attr: "query/instant", opts: []DeclareOption{Type("instant")}},
		{attr: "query/bytes", opts: []DeclareOption{Type("bytes")}},
		{attr: "query/vector", opts: []DeclareOption{Type("vector"), Dims(2)}},
		{attr: "query/json", opts: []DeclareOption{Type("json")}},
		{attr: "query/ref", opts: []DeclareOption{Ref()}},
	}
	for _, declaration := range declarations {
		if _, err := db.Declare(ctx, declaration.attr, declaration.opts...); err != nil {
			t.Fatalf("declare %s: %v", declaration.attr, err)
		}
	}
	longText := strings.Repeat("indexed text ", BlobThreshold)
	longBytes := []byte(strings.Repeat("z", BlobThreshold))
	values := map[string]any{
		"query/bool":    true,
		"query/int":     int64(7),
		"query/float":   1.25,
		"query/text":    longText,
		"query/instant": Instant(123),
		"query/bytes":   Bytes(longBytes),
		"query/vector":  Vector([]float32{1, 2}),
		"query/json":    JSON(E{"nested": []any{int64(1), true}}),
		"query/ref":     RefTo("query-target"),
	}
	seed := E{"id": "query-source"}
	for attr, value := range values {
		seed[attr] = value
	}
	if _, err := db.Transact(ctx, []any{E{"id": "query-target"}, seed}); err != nil {
		t.Fatal(err)
	}
	for attr, value := range values {
		t.Run(attr, func(t *testing.T) {
			result, err := db.Query(ctx, Q{
				Find: []any{"?entity"},
				In:   []string{"?needle"},
				Where: []any{
					[]any{"?entity", attr, "?needle"},
				},
			}, map[string]any{"?needle": value})
			if err != nil {
				t.Fatalf("indexed query: %v", err)
			}
			if !reflect.DeepEqual(result.Rows, [][]any{{map[string]any{"ref": "query-source"}}}) {
				t.Fatalf("indexed rows = %#v", result.Rows)
			}
		})
	}

	if _, err := db.Transact(ctx, []any{"retract", "query-source", "query/int", int64(7)}); err != nil {
		t.Fatal(err)
	}
	history, err := db.Query(ctx, Q{
		Find:   []any{"?added"},
		Source: "history",
		Where:  []any{[]any{"query-source", "query/int", int64(7), "_", "?added"}},
		Order:  []any{[]any{"?added", "asc"}},
	}, nil)
	if err != nil || !reflect.DeepEqual(history.Rows, [][]any{{false}, {true}}) {
		t.Fatalf("history rows = %#v, %v", history.Rows, err)
	}
	removed, err := db.Query(ctx, Q{
		Find:   []any{"?tx"},
		Source: "history",
		Where:  []any{[]any{"query-source", "query/int", int64(7), "?tx", false}},
	}, nil)
	if err != nil || len(removed.Rows) != 1 {
		t.Fatalf("removed history rows = %#v, %v", removed.Rows, err)
	}

	// Pattern blocks may be reordered for index access, while predicates and
	// rule calls remain semantic barriers in their original positions.
	ruleBarrier := Object{Fields: []Field{{Name: "rule", Value: []any{"reachable", "?entity"}}}}
	clauses := []any{
		[]any{"?entity", "?attribute", "?value"},
		[]any{"?entity", "query/text", "?text"},
		[]any{"starts-with", "?text", "indexed"},
		[]any{"query-source", "query/bool", true},
		ruleBarrier,
		[]any{"?entity", "query/ref", "?target"},
	}
	planned := orderQueryClauses(clauses, nil)
	wantOrdinals := []int{1, 0, 2, 3, 4, 5}
	for index, want := range wantOrdinals {
		if planned[index].ordinal != want {
			t.Fatalf("planned ordinals = %#v, want %#v", planned, wantOrdinals)
		}
	}

	accessCases := []struct {
		bound   map[string]bool
		access  string
		pattern []any
	}{
		{pattern: []any{1, "query/int", 7}, access: "eavt/exact"},
		{pattern: []any{1, "query/int", "?v"}, access: "eavt/ea"},
		{pattern: []any{"?e", "query/int", 7}, access: "avet"},
		{pattern: []any{1, "?a", "?v"}, access: "eavt/e"},
		{pattern: []any{"?e", "query/int", "?v"}, access: "avet/a"},
		{pattern: []any{"?e", "?a", 7}, access: "value-scan"},
		{pattern: []any{"?e", "?a", "?v"}, access: "scan"},
	}
	for _, test := range accessCases {
		_, access := patternAccessRank(test.pattern, test.bound)
		if access != test.access {
			t.Errorf("pattern %v access = %q, want %q", test.pattern, access, test.access)
		}
	}
	if got := initialBindingNames([]binding{{"?both": {}, "?left": {}}, {"?both": {}, "?right": {}}}); !reflect.DeepEqual(got, map[string]bool{"?both": true}) {
		t.Fatalf("common initial bindings = %#v", got)
	}

	if err := db.withRead(ctx, func(runner sqlRunner) error {
		evaluator := &queryEvaluator{
			ctx: ctx, runner: runner, db: db, source: "current",
			rules: map[string][]ruleDef{}, relations: map[string][][]cell{}, budget: DefaultQueryBudget,
		}
		if _, err := evaluator.evalIndexedPattern(
			[]any{"?entity", "query/bool", "?value"},
			binding{"?value": {tag: TagBool, value: "malformed"}},
		); !errors.Is(err, ErrQuery) {
			t.Errorf("malformed indexed value error = %v", err)
		}
		if _, err := evaluator.evalIndexedPattern(
			[]any{"?entity", "query/bool", true, "_", "?added"},
			binding{"?added": {tag: TagBool, value: "malformed"}},
		); !errors.Is(err, ErrQuery) {
			t.Errorf("malformed added binding error = %v", err)
		}

		canceled, cancel := context.WithCancel(ctx)
		cancel()
		canceledEvaluator := &queryEvaluator{
			ctx: canceled, runner: runner, db: db, source: "current", budget: 1,
			rules:     map[string][]ruleDef{"r": {{name: "r", args: []string{"?x"}}}},
			relations: map[string][][]cell{"r": {{{tag: TagInt, value: int64(1)}}}},
		}
		if _, err := canceledEvaluator.evalPattern([]any{"?e", "query/int", "?v"}, []binding{{}}); !errors.Is(err, ErrQuery) {
			t.Errorf("canceled pattern error = %v", err)
		}
		if _, err := canceledEvaluator.evalPredicate([]any{"=", 1, 1}, []binding{{}}); !errors.Is(err, ErrQuery) {
			t.Errorf("canceled predicate error = %v", err)
		}
		if _, err := canceledEvaluator.evalRule([]any{"r", "?x"}, []binding{{}}); !errors.Is(err, ErrQuery) {
			t.Errorf("canceled rule error = %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCASTransactionsEnforceMissingIsolationAndCardinality(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "cas/value", Type("int")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "cas/many", Type("int"), Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "cas/key", Type("text"), Unique()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "cas-present", "cas/value": int64(1), "cas/many": []any{int64(1)}, "cas/key": "present"},
		E{"id": "cas-empty"},
	}); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		kind error
		data any
		name string
	}{
		{name: "wrong arity", data: []any{"cas", "cas-present"}, kind: ErrType},
		{name: "missing entity", data: []any{"cas", "cas-absent", "cas/value", int64(1), int64(2)}, kind: ErrNotFound},
		{name: "attribute type", data: []any{"cas", "cas-present", true, int64(1), int64(2)}, kind: ErrType},
		{name: "missing attribute", data: []any{"cas", "cas-present", "cas/absent", int64(1), int64(2)}, kind: ErrNotFound},
		{name: "many attribute", data: []any{"cas", "cas-present", "cas/many", int64(1), int64(2)}, kind: ErrSchema},
		{name: "mismatched expected", data: []any{"cas", "cas-present", "cas/value", int64(2), int64(3)}, kind: ErrConflict},
		{name: "invalid missing sentinel", data: []any{"cas", "cas-empty", "cas/value", E{"missing": false}, int64(1)}, kind: ErrType},
		{name: "missing expected but present", data: []any{"cas", "cas-present", "cas/value", E{"missing": true}, int64(2)}, kind: ErrConflict},
		{name: "literal expected but missing", data: []any{"cas", "cas-empty", "cas/value", int64(1), int64(2)}, kind: ErrConflict},
		{name: "replacement type", data: []any{"cas", "cas-present", "cas/value", int64(1), "wrong"}, kind: ErrType},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := db.Transact(ctx, test.data); !errors.Is(err, test.kind) {
				t.Fatalf("CAS error = %v, want %v", err, test.kind)
			}
		})
	}

	created, err := db.Transact(ctx, []any{"cas", "cas-empty", "cas/value", E{"missing": true}, int64(9)})
	if err != nil || created.Tx == 0 {
		t.Fatalf("missing CAS = %+v, %v", created, err)
	}
	if _, err := db.Transact(ctx, []any{"cas", "cas-empty", "cas/value", E{"missing": true}, int64(10)}); !errors.Is(err, ErrConflict) {
		t.Fatalf("repeated missing CAS error = %v", err)
	}

	for _, test := range []struct {
		data any
		name string
	}{
		{name: "whole entity retract", data: []any{
			[]any{"cas", "cas-present", "cas/value", int64(1), int64(2)},
			[]any{"retract", "cas-present"},
		}},
		{name: "same attribute retract", data: []any{
			[]any{"cas", "cas-present", "cas/value", int64(1), int64(2)},
			[]any{"retract", "cas-present", "cas/value"},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := db.Transact(ctx, test.data); !errors.Is(err, ErrConflict) {
				t.Fatalf("CAS isolation error = %v", err)
			}
		})
	}

	if err := db.withRead(ctx, func(runner sqlRunner) error {
		allocator, err := newAllocator(ctx, runner, db.store)
		if err != nil {
			return err
		}
		plan := &transactionPlan{tempids: map[string]int64{}, allocator: allocator}
		for _, spec := range []any{
			int(db.store.names["cas-present"]),
			int64(db.store.names["cas-present"]),
			float64(db.store.names["cas-present"]),
			[]any{"cas/key", "present"},
		} {
			id, found, resolveErr := db.resolveOperationEntity(ctx, runner, plan, spec, false)
			if resolveErr != nil || !found || id != db.store.names["cas-present"] {
				t.Errorf("resolve operation entity %#v = %d,%t,%v", spec, id, found, resolveErr)
			}
		}
		if _, found, err := db.resolveOperationEntity(
			ctx, runner, plan, Object{Fields: []Field{{Name: "tmp", Value: "uncreated"}}}, false,
		); err != nil || found {
			t.Errorf("uncreated object tempid = %t,%v", found, err)
		}
		if id, found, err := db.resolveOperationEntity(
			ctx, runner, plan, Object{Fields: []Field{{Name: "tmp", Value: "created-object"}}}, true,
		); err != nil || !found || id <= GenesisTx {
			t.Errorf("created object tempid = %d,%t,%v", id, found, err)
		}
		if _, found, err := db.resolveOperationEntity(ctx, runner, plan, int64(999_999), false); err != nil || found {
			t.Errorf("unknown numeric read entity = %t,%v", found, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	entity := db.store.names["cas-present"]
	attribute := db.store.names["cas/value"]
	if _, err := db.store.sql.ExecContext(ctx,
		"INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (?,?,?,?,?,NULL)",
		entity, attribute, int64(99), TagInt, GenesisTx,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{"cas", "cas-present", "cas/value", int64(1), int64(2)}); !errors.Is(err, ErrFormat) {
		t.Fatalf("corrupt cardinality-one CAS error = %v", err)
	}
}

func TestTransactionOptionsFailBeforeMutationAndAliasesAgree(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	seed, err := db.Transact(ctx, E{"id": "stable", "option/value": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "bad-hash", "option/value": make(chan int)}, WithOperationID("bad-hash")); !errors.Is(err, ErrType) {
		t.Fatalf("unhashable operation request error = %v", err)
	}
	if _, err := db.Entity(ctx, "bad-hash"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unhashable operation mutated state: %v", err)
	}
	if _, err := db.Transact(ctx, E{"id": "stale", "option/value": int64(2)}, WithBasisTx(seed.Tx-1)); !errors.Is(err, ErrConflict) {
		t.Fatalf("WithBasisTx stale basis error = %v", err)
	}
	if _, err := db.Entity(ctx, "stale"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale basis mutated state: %v", err)
	}
}

func TestPublicPullNestedReverseAndHistorical(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "pull/child", Ref()); err != nil {
		t.Fatal(err)
	}
	first, transactErr := db.Transact(ctx, []any{
		E{"id": "pull/parent", "pull/name": "parent", "pull/child": RefTo("pull/leaf")},
		E{"id": "pull/leaf", "pull/name": "old"},
	})
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	if _, err := db.Transact(ctx, E{"id": "pull/leaf", "pull/name": "new"}); err != nil {
		t.Fatal(err)
	}

	pattern := []any{
		"pull/name",
		map[string]any{"pull/child": []any{"pull/name"}},
		"pull/_child",
	}
	pulled, err := db.Pull(ctx, "pull/parent", pattern)
	if err != nil {
		t.Fatal(err)
	}
	child, ok := pulled["pull/child"].(map[string]any)
	if !ok || child["pull/name"] != "new" || pulled["pull/name"] != "parent" {
		t.Fatalf("current pull = %#v", pulled)
	}
	leaf, err := db.Pull(ctx, "pull/leaf", []any{"*", "pull/_child"})
	if err != nil {
		t.Fatal(err)
	}
	parents, ok := leaf["pull/_child"].([]any)
	if !ok || len(parents) != 1 {
		t.Fatalf("reverse/star pull = %#v", leaf)
	}
	parentRef, parentOK := objectMap(parents[0])
	if !parentOK || parentRef["ref"] != "pull/parent" || leaf["pull/name"] != "new" {
		t.Fatalf("reverse/star pull = %#v", leaf)
	}

	historical, err := db.At(ctx, first.Tx)
	if err != nil {
		t.Fatal(err)
	}
	old, err := historical.Pull(ctx, "pull/parent", pattern)
	if err != nil {
		t.Fatal(err)
	}
	oldChild, ok := old["pull/child"].(map[string]any)
	if !ok || oldChild["pull/name"] != "old" {
		t.Fatalf("historical pull = %#v", old)
	}
}

func TestPublicPullValidationAndMissingEntity(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{"id": "pull/item", "pull/value": "text"}); err != nil {
		t.Fatal(err)
	}

	invalid := [][]any{
		{"invalid"},
		{map[string]any{"pull/value": true}},
		{map[string]any{"pull/value": []any{"pull/value"}, "pull/other": []any{"pull/value"}}},
		{map[string]any{"pull/_value": []any{"pull/value"}}},
	}
	for _, pattern := range invalid {
		if _, err := db.Pull(ctx, "pull/item", pattern); !errors.Is(err, ErrQuery) {
			t.Errorf("Pull(%#v) error = %v", pattern, err)
		}
	}
	if _, err := db.Pull(ctx, "pull/item", []any{map[string]any{"pull/value": []any{"pull/value"}}}); !errors.Is(err, ErrQuery) {
		t.Fatalf("nested non-reference pull error = %v", err)
	}
	if _, err := db.Pull(ctx, "pull/missing", []any{"*"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing entity pull error = %v", err)
	}

	closed, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := closed.Pull(ctx, "pull/item", []any{"*"}); !errors.Is(err, ErrFormat) {
		t.Fatalf("closed pull error = %v", err)
	}
}
