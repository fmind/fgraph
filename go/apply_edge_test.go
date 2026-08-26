package fgraph

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"strings"
	"testing"
)

func TestApplySelectorAndWireValueBoundaries(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("selector query failed")
	runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{
		{contains: "SELECT id", err: failure},
	}})
	db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}

	if _, err := db.applySelector(ctx, runner, ""); !errors.Is(err, ErrType) {
		t.Fatalf("invalid named selector error = %v", err)
	}
	if _, err := db.applySelector(ctx, runner, map[string]any{"eid": 1}); !errors.Is(err, ErrType) {
		t.Fatalf("non-text eid selector error = %v", err)
	}
	if _, err := db.applySelector(ctx, runner, map[string]any{"eid": "11111111-1111-4111-8111-111111111111"}); !errors.Is(err, ErrFormat) {
		t.Fatalf("eid query error = %v", err)
	}
	if _, err := db.applyWireValue(ctx, runner, "bad", "ref"); !errors.Is(err, ErrType) {
		t.Fatalf("malformed ref value error = %v", err)
	}
	if _, err := db.applyWireValue(ctx, runner, map[string]any{"ref": map[string]any{"eid": 1}}, "ref"); !errors.Is(err, ErrType) {
		t.Fatalf("malformed ref selector error = %v", err)
	}
	if got, err := db.applyWireValue(ctx, runner, int64(7), "int"); err != nil || got != int64(7) {
		t.Fatalf("integer wire value = %#v, %v", got, err)
	}

	existing := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{{
		contains: "SELECT id", columns: []string{"id"}, rows: [][]driver.Value{{int64(77)}},
	}}})
	existingDB := &DB{store: &store{sql: existing, names: map[string]int64{}}, exec: existing}
	if got, err := existingDB.applySelector(ctx, existing, map[string]any{"eid": "11111111-1111-4111-8111-111111111111"}); err != nil || got != int64(77) {
		t.Fatalf("existing eid selector = %#v, %v", got, err)
	}

	missing := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{{
		contains: "SELECT id", columns: []string{"id"},
	}}})
	missingDB := &DB{store: &store{sql: missing, names: map[string]int64{}}, exec: missing}
	got, err := missingDB.applySelector(ctx, missing, map[string]any{"eid": "11111111-1111-4111-8111-111111111111"})
	if err != nil || got != Tmp("event:11111111-1111-4111-8111-111111111111") {
		t.Fatalf("new eid selector = %#v, %v", got, err)
	}
}

func TestPreallocatePortableIdentityBoundaries(t *testing.T) {
	ctx := context.Background()
	uuid := "11111111-1111-4111-8111-111111111111"
	noRows := scriptedQuery{contains: "SELECT id", columns: []string{"id"}}

	for _, test := range []struct {
		prepare func(*transactionPlan)
		name    string
		value   any
		kind    error
		rule    scriptedQuery
	}{
		{name: "invalid name", value: "", kind: ErrType, rule: noRows},
		{name: "invalid eid", value: map[string]any{"eid": "bad"}, kind: ErrType, rule: noRows},
		{name: "lookup failure", value: map[string]any{"eid": uuid}, kind: ErrFormat, rule: scriptedQuery{contains: "SELECT id", err: errors.New("lookup failed")}},
		{name: "duplicate eid", value: map[string]any{"eid": uuid}, kind: ErrConflict, rule: noRows, prepare: func(plan *transactionPlan) {
			plan.tempids["event:"+uuid] = 65
		}},
		{name: "allocator exhaustion", value: map[string]any{"eid": uuid}, kind: ErrTooLarge, rule: noRows, prepare: func(plan *transactionPlan) {
			plan.allocator.next = math.MaxInt64
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			root := &store{sql: runner, names: map[string]int64{}}
			plan := &transactionPlan{
				allocator: &allocator{runner: runner, store: root, ids: map[string]int64{}, gids: map[int64]string{}, next: 65},
				tempids:   map[string]int64{},
			}
			if test.prepare != nil {
				test.prepare(plan)
			}
			db := &DB{store: root, exec: runner}
			if err := db.preallocateEventIdentities(ctx, runner, plan, []any{test.value}); !errors.Is(err, test.kind) {
				t.Fatalf("preallocate error = %v", err)
			}
		})
	}

	existing := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{{
		contains: "SELECT id", columns: []string{"id"}, rows: [][]driver.Value{{int64(70)}},
	}}})
	root := &store{sql: existing, names: map[string]int64{}}
	plan := &transactionPlan{allocator: &allocator{runner: existing, store: root, ids: map[string]int64{}, gids: map[int64]string{}, next: 65}, tempids: map[string]int64{}}
	if err := (&DB{store: root, exec: existing}).preallocateEventIdentities(ctx, existing, plan, []any{map[string]any{"eid": uuid}}); err != nil || len(plan.tempids) != 0 {
		t.Fatalf("existing preallocated identity = %#v, %v", plan.tempids, err)
	}
}

func TestApplyDuplicateReceiptCorruptionAndOversizedCanonicalEvent(t *testing.T) {
	ctx := context.Background()
	source := fixedDB(t, ":memory:")
	if _, err := source.Transact(ctx, E{"id": "apply/item", "apply/value": "value"}); err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	if err := source.Tail(ctx, &stream, GenesisTx); err != nil {
		t.Fatal(err)
	}
	target := fixedDB(t, ":memory:")
	if _, err := target.Apply(ctx, bytes.NewReader(stream.Bytes())); err != nil {
		t.Fatal(err)
	}
	var tx int64
	if err := target.store.sql.QueryRowContext(ctx, "SELECT tx FROM fgraph_events WHERE tx>?", GenesisTx).Scan(&tx); err != nil {
		t.Fatal(err)
	}
	if _, err := target.store.sql.ExecContext(ctx, "DELETE FROM fgraph_facts WHERE e=? AND a=1", tx); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Apply(ctx, bytes.NewReader(stream.Bytes())); !errors.Is(err, ErrFormat) {
		t.Fatalf("duplicate corrupted receipt error = %v", err)
	}

	oversized := map[string]any{
		"fgraph": "event/1", "event": "11111111-1111-4111-8111-111111111111", "at": int64(1),
		"created": []any{}, "asserted": []any{}, "retracted": []any{},
		"meta": strings.Repeat("x", maxPortableLineBytes),
	}
	if _, _, err := canonicalEventData(oversized); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized canonical event error = %v", err)
	}
}

func TestApplySummaryIsCompactIdempotentAndTyped(t *testing.T) {
	ctx := context.Background()
	source := fixedDB(t, ":memory:")
	if _, err := source.Transact(ctx, E{"id": "summary/item", "summary/value": "value"}); err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	if err := source.Tail(ctx, &stream, GenesisTx); err != nil {
		t.Fatal(err)
	}

	target := fixedDB(t, ":memory:")
	first, err := target.ApplySummary(ctx, bytes.NewReader(stream.Bytes()))
	if err != nil || first.Events != 1 || first.Applied != 1 || first.BasisTx <= GenesisTx {
		t.Fatalf("first summary = %#v, %v", first, err)
	}
	retried, err := target.ApplySummary(ctx, bytes.NewReader(stream.Bytes()))
	if err != nil || retried.Events != 1 || retried.AlreadyApplied != 1 || retried.BasisTx != first.BasisTx {
		t.Fatalf("retried summary = %#v, %v", retried, err)
	}
	if _, err := target.ApplySummary(ctx, nil); !errors.Is(err, ErrType) {
		t.Fatalf("nil summary reader error = %v", err)
	}
	if _, err := target.ApplySummary(ctx, errorReader{}); !errors.Is(err, ErrFormat) {
		t.Fatalf("failed summary reader error = %v", err)
	}
}

func TestAtomicPortableWriteSavepointFailures(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("savepoint failed")
	for _, test := range []struct {
		name string
		rule scriptedExec
	}{
		{name: "begin", rule: scriptedExec{contains: "SAVEPOINT", err: failure}},
		{name: "release", rule: scriptedExec{contains: "RELEASE", err: failure}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{
				queries: []scriptedQuery{{contains: "PRAGMA data_version", columns: []string{"v"}, rows: [][]driver.Value{{int64(1)}}}, {contains: "SELECT id,name,gid", columns: []string{"id", "name", "gid"}}},
				execs:   []scriptedExec{test.rule},
			})
			db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
			err := db.atomicPortableWrite(ctx, "test", func(sqlRunner, *sql.Conn) error { return nil })
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("savepoint failure = %v", err)
			}
		})
	}

	runner := openScriptedSQL(t, scriptedSQL{})
	db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
	if err := db.atomicPortableWrite(ctx, "test", func(sqlRunner, *sql.Conn) error { return nil }); err != nil {
		t.Fatalf("successful savepoint write = %v", err)
	}
	if db.store.dataVersion != -1 {
		t.Fatalf("successful savepoint did not invalidate caches: %d", db.store.dataVersion)
	}
}
