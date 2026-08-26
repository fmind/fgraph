package fgraph

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
)

func TestTransactionEventFactoryAndVectorModelValidation(t *testing.T) {
	ctx := context.Background()
	factoryFailure := errors.New("event source unavailable")
	for _, test := range []struct {
		kind    error
		factory EventIDFactory
		name    string
	}{
		{name: "factory error", kind: factoryFailure, factory: func() (string, error) { return "", factoryFailure }},
		{name: "malformed uuid", kind: ErrType, factory: func() (string, error) { return "not-a-uuid", nil }},
		{name: "non-v4 uuid", kind: ErrType, factory: func() (string, error) { return "11111111-1111-1111-8111-111111111111", nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := Open(":memory:", WithClock(func() int64 { return 1_767_225_600_000_000 }), WithEventIDFactory(test.factory))
			if err != nil {
				t.Fatal(err)
			}
			defer closeTest(t, db)
			if _, err := db.Transact(ctx, E{"id": "event/failure", "event/value": int64(1)}); !errors.Is(err, test.kind) {
				t.Fatalf("event factory transaction error = %v", err)
			}
			if _, err := db.Entity(ctx, "event/failure"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("failed event factory mutated database: %v", err)
			}
		})
	}
	if _, err := Open(":memory:", WithEventIDFactory(nil)); !errors.Is(err, ErrType) {
		t.Fatalf("nil event factory error = %v", err)
	}

	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "model/empty", Type("vector"), VectorModel("")); !errors.Is(err, ErrSchema) {
		t.Fatalf("empty vector model error = %v", err)
	}
	if _, err := db.Declare(ctx, "model/text", Type("text"), VectorModel("model")); !errors.Is(err, ErrSchema) {
		t.Fatalf("non-vector model error = %v", err)
	}
}

func TestPendingIdentityCompactionRemapsReferencesAndTempIDs(t *testing.T) {
	ctx := context.Background()
	runner := openScriptedSQL(t, scriptedSQL{})
	root := &store{sql: runner, names: map[string]int64{}}
	alloc := &allocator{
		runner: runner, store: root, first: 65, next: 68, dirty: true,
		ids: map[string]int64{"kept/name": 65}, gids: map[int64]string{},
		pending: []pendingIdentity{
			{id: 65, kind: identityNamed, name: "kept/name"},
			{id: 66, kind: identityAnonymous},
			{id: 67, kind: identityAnonymous},
		},
	}
	plan := &transactionPlan{allocator: alloc, tempids: map[string]int64{"dropped": 66, "kept": 67}}
	assertions := []plannedFact{{
		e: 65, a: 65, value: storedValue{logical: int64(67), storage: int64(67), tag: TagRef},
	}}
	txFacts := []plannedFact{{
		a: 67, value: storedValue{logical: int64(67), storage: int64(67), tag: TagRef},
	}}
	if err := compactPendingAllocations(ctx, runner, plan, assertions, txFacts); err != nil {
		t.Fatal(err)
	}
	if alloc.next != 67 || len(alloc.pending) != 2 || plan.tempids["kept"] != 66 {
		t.Fatalf("compacted allocation = next %d pending %#v tempids %#v", alloc.next, alloc.pending, plan.tempids)
	}
	if _, exists := plan.tempids["dropped"]; exists {
		t.Fatalf("canceled tempid survived: %#v", plan.tempids)
	}
	if assertions[0].value.storage != int64(66) || txFacts[0].a != 66 || txFacts[0].value.storage != int64(66) {
		t.Fatalf("references were not remapped: %#v %#v", assertions, txFacts)
	}
}

func TestPendingIdentityCompactionSQLFailure(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("identity rewrite failed")
	runner := openScriptedSQL(t, scriptedSQL{execs: []scriptedExec{{contains: "UPDATE fgraph_ids", err: failure}}})
	alloc := &allocator{
		runner: runner, store: &store{sql: runner, names: map[string]int64{}}, first: 65, next: 67,
		ids: map[string]int64{"kept": 66}, pending: []pendingIdentity{{id: 65, kind: identityAnonymous}, {id: 66, kind: identityNamed, name: "kept"}},
	}
	plan := &transactionPlan{allocator: alloc, tempids: map[string]int64{}}
	if err := compactPendingAllocations(ctx, runner, plan, nil, nil); !errors.Is(err, ErrFormat) {
		t.Fatalf("compaction SQL failure = %v", err)
	}
}

func TestPlannedDeclarationPhysicalTypeValidation(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	for _, test := range []struct {
		name string
		fact plannedFact
	}{
		{name: "type", fact: plannedFact{e: 65, a: 8, value: storedValue{storage: int64(1)}}},
		{name: "doc", fact: plannedFact{e: 65, a: 10, value: storedValue{logical: int64(1)}}},
		{name: "model", fact: plannedFact{e: 65, a: 14, value: storedValue{logical: int64(1)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := &transactionPlan{allocator: &allocator{ids: map[string]int64{}}, assertions: []plannedFact{test.fact}}
			if err := db.validatePlannedDeclarations(ctx, db.store.sql, plan); !errors.Is(err, ErrFormat) {
				t.Fatalf("planned declaration error = %v", err)
			}
		})
	}
}

func TestFinalSchemaAndWorkingStateSQLFaults(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("transaction state fault")
	for _, test := range []struct {
		name string
		rule scriptedQuery
	}{
		{name: "query", rule: scriptedQuery{contains: "SELECT id,e,a,v,t,tx,rx", err: failure}},
		{name: "scan", rule: scriptedQuery{contains: "SELECT id,e,a,v,t,tx,rx", columns: make([]string, 7), rows: [][]driver.Value{{int64(1)}}}},
	} {
		t.Run("schema "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
			plan := &transactionPlan{allocator: &allocator{runner: runner, store: db.store, ids: map[string]int64{}}, tempids: map[string]int64{}}
			if _, err := db.finalSchemaForPlan(ctx, runner, 65, plan, nil); !errors.Is(err, ErrFormat) {
				t.Fatalf("final schema fault = %v", err)
			}
		})
	}

	for _, test := range []struct {
		name string
		rule scriptedQuery
	}{
		{name: "query", rule: scriptedQuery{contains: "SELECT id,e,a,v,t,tx,rx", err: failure}},
		{name: "scan", rule: scriptedQuery{contains: "SELECT id,e,a,v,t,tx,rx", columns: make([]string, 7), rows: [][]driver.Value{{int64(1)}}}},
	} {
		t.Run("working "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			root := &store{sql: runner, names: map[string]int64{}}
			db := &DB{store: root, exec: runner}
			plan := &transactionPlan{
				allocator: &allocator{runner: runner, store: root, ids: map[string]int64{}}, tempids: map[string]int64{},
				assertions: []plannedFact{{e: 65, a: 66, value: storedValue{storage: int64(1), tag: TagInt}}},
			}
			if _, err := db.workingFactsForPlan(ctx, runner, plan); !errors.Is(err, ErrFormat) {
				t.Fatalf("working state fault = %v", err)
			}
		})
	}
}
