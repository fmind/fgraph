package fgraph

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func queryLimit(value int) *int { return &value }

func physicalState(t *testing.T, db *DB) map[string][]string {
	t.Helper()
	queries := map[string]string{
		"meta":     "SELECT key,value FROM fgraph_meta ORDER BY key",
		"ids":      "SELECT id,name FROM fgraph_ids ORDER BY id",
		"facts":    "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts ORDER BY id",
		"blobs":    "SELECT hash,data FROM fgraph_blobs ORDER BY hash",
		"fts":      "SELECT rowid,text FROM fgraph_fts ORDER BY rowid",
		"sequence": "SELECT name,seq FROM sqlite_sequence ORDER BY name",
	}
	state := make(map[string][]string, len(queries))
	for name, query := range queries {
		rows, err := db.store.sql.QueryContext(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			closeTest(t, rows)
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			targets := make([]any, len(columns))
			for index := range values {
				targets[index] = &values[index]
			}
			if err := rows.Scan(targets...); err != nil {
				closeTest(t, rows)
				t.Fatal(err)
			}
			encoded, err := json.Marshal(values)
			if err != nil {
				closeTest(t, rows)
				t.Fatal(err)
			}
			state[name] = append(state[name], string(encoded))
		}
		if err := rows.Err(); err != nil {
			closeTest(t, rows)
			t.Fatal(err)
		}
		closeTest(t, rows)
	}
	return state
}

func TestApplyRollsBackCompleteStream(t *testing.T) {
	db := fixedDB(t, ":memory:")
	before := physicalState(t, db)
	first := fmt.Sprintf(
		`{"fgraph":"event/1","event":"11111111-1111-4111-8111-111111111111","at":1767225601000000,"created":["note","note/text"],"asserted":[["note","note/text",%q,"text"]],"retracted":[]}`+"\n",
		strings.Repeat("x", 300),
	)

	_, err := db.Apply(context.Background(), strings.NewReader(first+"not-json\n"))

	if !errors.Is(err, ErrType) {
		t.Fatalf("apply error = %v", err)
	}
	if after := physicalState(t, db); !reflect.DeepEqual(after, before) {
		t.Fatalf("partial apply state = %#v, want %#v", after, before)
	}
}

func TestAllocatorExhaustionIsTypedAtomicAndDiagnosable(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_meta SET value=? WHERE key='next_id'", int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	before := physicalState(t, db)
	if _, err := db.Transact(ctx, E{"id": "overflow", "item/value": int64(1)}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("allocator exhaustion error = %v, want TooLarge", err)
	}
	if after := physicalState(t, db); !reflect.DeepEqual(after, before) {
		t.Fatalf("allocator exhaustion mutated state: after=%#v before=%#v", after, before)
	}
	if _, err := db.store.sql.ExecContext(ctx, "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,?,NULL,?)", int64(math.MaxInt64), "corrupt/max", GenesisTx); err != nil {
		t.Fatal(err)
	}
	report, err := db.Doctor(ctx)
	if err != nil || report.OK || !strings.Contains(strings.Join(report.Problems, "\n"), "allocator exhausted") {
		t.Fatalf("doctor allocator report = %+v, %v", report, err)
	}
}

func TestDoctorRejectsBrokenIdentityAndTransactionLinks(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		corruptDB func(*testing.T, *DB, TxReport)
		name      string
		problem   string
	}{
		{name: "identity", problem: "invalid identity ids", corruptDB: func(t *testing.T, db *DB, _ TxReport) {
			_, err := db.store.sql.ExecContext(ctx, "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (-1,'corrupt/identity',NULL,64)")
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "fact", problem: "invalid fact identifiers", corruptDB: func(t *testing.T, db *DB, _ TxReport) {
			_, err := db.store.sql.ExecContext(ctx, "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (-1,2,'corrupt',4,64,NULL)")
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "named transaction", problem: "named identities overlap transaction receipts", corruptDB: func(t *testing.T, db *DB, _ TxReport) {
			if _, err := db.store.sql.ExecContext(ctx, "PRAGMA ignore_check_constraints=ON"); err != nil {
				t.Fatal(err)
			}
			_, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_ids SET name='corrupt/transaction',gid=NULL WHERE id=64")
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "temporal identity", problem: "invalid temporal identities", corruptDB: func(t *testing.T, db *DB, _ TxReport) {
			later, err := db.Transact(ctx, E{"id": "later-subject", "item/value": int64(2)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.store.sql.ExecContext(ctx, "UPDATE fgraph_ids SET created_tx=? WHERE name='doctor-subject'", later.Tx); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "asserting transaction", problem: "cannot be reconstructed: missing timestamp", corruptDB: func(t *testing.T, db *DB, receipt TxReport) {
			_, err := db.store.sql.ExecContext(ctx, "DELETE FROM fgraph_facts WHERE e=? AND a=1 AND tx=e", receipt.Tx)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "retracting transaction", problem: "cannot be reconstructed: missing timestamp", corruptDB: func(t *testing.T, db *DB, _ TxReport) {
			retraction, err := db.Retract(ctx, "doctor-subject", "item/value")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.store.sql.ExecContext(ctx, "DELETE FROM fgraph_facts WHERE e=? AND a=1 AND tx=e", retraction.Tx); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			receipt, err := db.Transact(ctx, E{"id": "doctor-subject", "item/value": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			test.corruptDB(t, db, receipt)
			report, doctorErr := db.Doctor(ctx)
			if doctorErr != nil || report.OK || !strings.Contains(strings.Join(report.Problems, "\n"), test.problem) {
				t.Fatalf("Doctor() = %+v, %v, want %q", report, doctorErr, test.problem)
			}
			if _, repairErr := db.Doctor(ctx, true); !errors.Is(repairErr, ErrFormat) {
				t.Fatalf("repair error = %v, want FormatError", repairErr)
			}
		})
	}
}

func TestDoctorRejectsBlobContentCorruption(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{"id": "vector", "item/vector": Vector([]float32{1, 2})}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_blobs SET data=zeroblob(length(data))"); err != nil {
		t.Fatal(err)
	}
	report, err := db.Doctor(ctx)
	if err != nil || report.OK || !strings.Contains(strings.Join(report.Problems, "\n"), "invalid indirect blobs: 1") {
		t.Fatalf("Doctor() = %+v, %v", report, err)
	}
	if _, err := db.Doctor(ctx, true); !errors.Is(err, ErrFormat) {
		t.Fatalf("repair error = %v, want FormatError", err)
	}
}

func TestApplyRollsBackCompleteStreamInsideSpeculation(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	err := db.Speculate(ctx, func(spec *DB) error {
		valid := `{"fgraph":"event/1","event":"11111111-1111-4111-8111-111111111111","at":1767225601000000,"created":["nested","note/text"],"asserted":[["nested","note/text","temporary","text"]],"retracted":[]}` + "\n"
		if _, err := spec.Apply(ctx, strings.NewReader(valid+"not-json\n")); !errors.Is(err, ErrType) {
			return fmt.Errorf("nested apply error = %w", err)
		}
		if _, err := spec.Entity(ctx, "nested"); !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("partial nested apply entity error: %w", err)
		}
		if _, err := spec.Transact(ctx, E{"id": "after-import", "note/text": "usable"}); err != nil {
			return fmt.Errorf("transaction after nested rollback: %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCommittedWritesPublishNameCacheLazily(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	report, err := db.Transact(ctx, E{"id": "direct", "item/value": int64(1)})
	if err != nil || report.Tx == 0 || db.store.dataVersion < 0 {
		t.Fatalf("committed transaction cache state = report %#v, version %d, err %v", report, db.store.dataVersion, err)
	}
	if entity, err := db.Entity(ctx, "direct"); err != nil || entity["item/value"] != int64(1) {
		t.Fatalf("lazy transaction cache refresh = %#v, %v", entity, err)
	}

	stream := `{"fgraph":"event/1","event":"11111111-1111-4111-8111-111111111111","at":1767225605000000,"created":["merged"],"asserted":[["merged","item/value",2,"int"]],"retracted":[]}` + "\n"
	if _, err := db.Apply(ctx, strings.NewReader(stream)); err != nil || db.store.dataVersion != -1 {
		t.Fatalf("committed apply cache version = %d, %v", db.store.dataVersion, err)
	}
	if entity, err := db.Entity(ctx, "merged"); err != nil || entity["item/value"] != int64(2) {
		t.Fatalf("lazy apply cache refresh = %#v, %v", entity, err)
	}
}

func TestSequentialWritesKeepIdentityCacheCurrent(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	initialVersion := db.store.dataVersion
	for index := range 10 {
		if _, err := db.Transact(ctx, E{"id": fmt.Sprintf("cached/%d", index), "item/value": int64(index)}); err != nil {
			t.Fatal(err)
		}
		if db.store.dataVersion != initialVersion {
			t.Fatalf("write %d invalidated identity cache: got %d, want %d", index, db.store.dataVersion, initialVersion)
		}
	}
}

func TestSearchFiltersSystemFactsBeforeRankingAndAddsProvenance(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	for index := range 55 {
		if _, err := db.Transact(ctx, E{}, WithSource("needle"), WithBy(fmt.Sprintf("noise-%d", index))); err != nil {
			t.Fatal(err)
		}
	}
	report, err := db.Transact(
		ctx,
		E{"id": "domain", "note/text": "the needle belongs to the application"},
		WithSource("project.md"),
		WithBy("agent"),
	)
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Search(ctx, SearchOpts{Text: "needle"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].Entity != "domain" {
		t.Fatalf("search hits = %#v", result.Hits)
	}
	matched := result.Hits[0].Matched
	if len(matched) != 1 || matched[0].At != report.At || matched[0].By != "agent" || matched[0].Source != "project.md" {
		t.Fatalf("matched provenance = %#v", matched)
	}
}

func TestQueryBudgetAndExplicitZeroLimit(t *testing.T) {
	ctx := context.Background()
	db, openErr := Open(":memory:", WithClock(func() int64 { return 1_767_225_600_000_000 }), WithQueryBudget(1))
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { closeTest(t, db) })
	if _, err := db.Transact(ctx, E{"id": "one", "item/value": int64(1)}); err != nil {
		t.Fatal(err)
	}
	query := Q{Find: []any{"?e"}, Where: []any{[]any{"?e", "item/value", "_"}}}
	if result, err := db.Query(ctx, query, nil); err != nil || len(result.Rows) != 1 {
		t.Fatalf("one work unit = %#v, %v", result, err)
	}
	if _, err := db.Transact(ctx, E{"id": "two", "item/value": int64(2)}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(ctx, query, nil); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("budget error = %v", err)
	}
	zero := 0
	query.Limit = &zero
	// A larger connection proves a constructed query can distinguish an
	// explicit zero limit from an omitted limit.
	unbounded := fixedDB(t, ":memory:")
	if _, err := unbounded.Transact(ctx, E{"id": "one", "item/value": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if result, err := unbounded.Query(ctx, query, nil); err != nil || len(result.Rows) != 0 {
		t.Fatalf("explicit zero limit = %#v, %v", result, err)
	}
}

func TestQueryBudgetUsesSetSemanticsBetweenPatterns(t *testing.T) {
	ctx := context.Background()
	db, openErr := Open(":memory:", WithClock(func() int64 { return 1_767_225_600_000_000 }), WithQueryBudget(3))
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { closeTest(t, db) })
	if _, err := db.Declare(ctx, "item/tags", Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "one", "item/name": "One", "item/tags": []any{"a", "b"}}); err != nil {
		t.Fatal(err)
	}
	query := Q{Find: []any{"?e"}, Where: []any{
		[]any{"?e", "item/tags", "_"},
		[]any{"?e", "item/name", "_"},
	}}
	result, err := db.Query(ctx, query, nil)
	if err != nil || !reflect.DeepEqual(result.Rows, [][]any{{map[string]any{"ref": "one"}}}) {
		t.Fatalf("set-semantic work budget = %#v, %v", result, err)
	}
}

func TestQueryBudgetUsesSetSemanticsAfterRules(t *testing.T) {
	ctx := context.Background()
	db, openErr := Open(":memory:", WithClock(func() int64 { return 1_767_225_600_000_000 }), WithQueryBudget(7))
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { closeTest(t, db) })
	if _, err := db.Declare(ctx, "item/tags", Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "one", "item/name": "One", "item/tags": []any{"a", "b"}}); err != nil {
		t.Fatal(err)
	}
	rule := map[string]any{
		"head": []any{"has-tag", "?entity", "?tag"},
		"body": []any{[]any{"?entity", "item/tags", "?tag"}},
	}
	invoke := Object{Fields: []Field{{Name: "rule", Value: []any{"has-tag", "?e", "_"}}}}
	query := Q{Find: []any{"?e"}, Rules: []any{rule}, Where: []any{
		invoke,
		[]any{"?e", "item/name", "_"},
	}}
	result, err := db.Query(ctx, query, nil)
	if err != nil || !reflect.DeepEqual(result.Rows, [][]any{{map[string]any{"ref": "one"}}}) {
		t.Fatalf("rule set-semantic work budget = %#v, %v", result, err)
	}
}

func TestQueryBudgetShortCircuitsUnresolvedPatternConstants(t *testing.T) {
	ctx := context.Background()
	db, openErr := Open(":memory:", WithClock(func() int64 { return 1_767_225_600_000_000 }), WithQueryBudget(1))
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { closeTest(t, db) })
	if _, err := db.Declare(ctx, "item/link", Ref(), Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "one", "item/value": int64(1), "item/link": RefTo("target-one")},
		E{"id": "two", "item/value": int64(2), "item/link": RefTo("target-two")},
	}); err != nil {
		t.Fatal(err)
	}
	for name, query := range map[string]Q{
		"entity": {Find: []any{"?value"}, Where: []any{[]any{"missing", "item/value", "?value"}}},
		"ref":    {Find: []any{"?e"}, Where: []any{[]any{"?e", "item/link", RefTo("missing")}}},
	} {
		result, err := db.Query(ctx, query, nil)
		if err != nil || len(result.Rows) != 0 {
			t.Fatalf("unresolved %s constant = %#v, %v", name, result, err)
		}
	}
	wildcard := Q{Find: []any{"?e"}, Where: []any{[]any{"?e", "item/value", "_"}}}
	if _, err := db.Query(ctx, wildcard, nil); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("wildcard budget error = %v, want TooLarge", err)
	}
}

func TestQueryBudgetNormalizesSignedZeroBetweenOrBranches(t *testing.T) {
	ctx := context.Background()
	db, openErr := Open(":memory:", WithClock(func() int64 { return 1_767_225_600_000_000 }), WithQueryBudget(3))
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { closeTest(t, db) })
	if _, err := db.Transact(ctx, E{
		"id": "one", "number/negative": math.Copysign(0, -1), "number/positive": float64(0), "item/name": "One",
	}); err != nil {
		t.Fatal(err)
	}
	orClause := Object{Fields: []Field{{Name: "or", Value: []any{
		[]any{[]any{"?e", "number/negative", "?number"}},
		[]any{[]any{"?e", "number/positive", "?number"}},
	}}}}
	query := Q{Find: []any{"?e"}, Where: []any{orClause, []any{"?e", "item/name", "_"}}}
	result, err := db.Query(ctx, query, nil)
	if err != nil || !reflect.DeepEqual(result.Rows, [][]any{{map[string]any{"ref": "one"}}}) {
		t.Fatalf("signed-zero OR work budget = %#v, %v", result, err)
	}
}

func TestQueryBudgetBuildsRuleDependenciesTopologically(t *testing.T) {
	ctx := context.Background()
	db, openErr := Open(":memory:", WithClock(func() int64 { return 1_767_225_600_000_000 }), WithQueryBudget(5))
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { closeTest(t, db) })
	if _, err := db.Transact(ctx, E{"id": "one", "item/tag": "a"}); err != nil {
		t.Fatal(err)
	}
	invoke := func(name string) Object {
		return Object{Fields: []Field{{Name: "rule", Value: []any{name, "?entity"}}}}
	}
	query := Q{
		Find: []any{"?entity"}, Where: []any{invoke("derived")},
		Rules: []any{
			map[string]any{"head": []any{"derived", "?entity"}, "body": []any{invoke("base")}},
			map[string]any{"head": []any{"base", "?entity"}, "body": []any{[]any{"?entity", "item/tag", "a"}}},
		},
	}
	result, err := db.Query(ctx, query, nil)
	if err != nil || !reflect.DeepEqual(result.Rows, [][]any{{map[string]any{"ref": "one"}}}) {
		t.Fatalf("topological rule budget = %#v, %v", result, err)
	}
}

func TestQueryBudgetValidationAndCancellation(t *testing.T) {
	for _, budget := range []int{0, -1} {
		if _, err := Open(":memory:", WithQueryBudget(budget)); !errors.Is(err, ErrType) {
			t.Fatalf("budget %d error = %v", budget, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	evaluator := &queryEvaluator{
		ctx: ctx,
		db:  &DB{store: &store{names: map[string]int64{"item/value": FirstUserID}}},
		facts: []queryFact{{
			e: FirstUserID, a: "item/value", cell: cell{tag: TagInt, value: int64(1)},
		}},
		budget: DefaultQueryBudget,
	}
	if _, err := evaluator.evalPattern([]any{"?e", "item/value", "_"}, []binding{{}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled evaluation error = %v", err)
	}
}

func TestQueryJSONPreservesExactInt64Constants(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{"id": "boundary", "value/int": int64(math.MaxInt64)}); err != nil {
		t.Fatal(err)
	}
	query := map[string]any{
		"find": []any{"?value"},
		"where": []any{
			[]any{"boundary", "value/int", "?value"},
			[]any{"=", "?value", int64(math.MaxInt64)},
		},
		"in": []string{}, "order": []any{}, "rules": []any{}, "limit": float64(1), "offset": int64(0),
	}
	result, err := db.QueryJSON(ctx, query, nil)
	if err != nil || !reflect.DeepEqual(result.Rows, [][]any{{int64(math.MaxInt64)}}) {
		t.Fatalf("exact int64 query = %#v, %v", result, err)
	}
	parsed, err := ParseQuery(map[string]any{"find": []any{"?x"}, "where": []any{}, "limit": int64(0)})
	if err != nil || parsed.Limit == nil || *parsed.Limit != 0 {
		t.Fatalf("parsed explicit zero limit = %#v, %v", parsed, err)
	}
	for _, invalid := range []map[string]any{
		{"find": "?x", "where": []any{}},
		{"find": []any{"?x"}, "where": true},
		{"find": []any{"?x"}, "where": []any{}, "order": true},
		{"find": []any{"?x"}, "where": []any{}, "rules": true},
		{"find": []any{"?x"}, "where": []any{}, "in": "?x"},
		{"find": []any{"?x"}, "where": []any{}, "in": []any{int64(1)}},
		{"find": []any{"?x"}, "where": []any{}, "limit": "one"},
		{"find": []any{"?x"}, "where": []any{}, "limit": 1.5},
		{"find": []any{"?x"}, "where": []any{}, "offset": true},
	} {
		if _, err := ParseQuery(invalid); !errors.Is(err, ErrQuery) {
			t.Errorf("ParseQuery(%#v) error = %v", invalid, err)
		}
	}
}

func TestAttributesDiscoversEffectiveSchemaAndTypes(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "person/name", Type("text"), Unique(true), Doc("Display name")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "person/tags", Many(true)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{
		"id": "ada", "person/name": "Ada", "person/tags": []any{"compiler", int64(1)},
		"note/vector": Vector([]float32{1, 0}),
	}); err != nil {
		t.Fatal(err)
	}

	attributes, err := db.Attributes(ctx, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if names := []string{attributes[0].Name, attributes[1].Name, attributes[2].Name}; !reflect.DeepEqual(names, []string{"note/vector", "person/name", "person/tags"}) {
		t.Fatalf("attribute names = %v", names)
	}
	if !attributes[0].NoHistory || attributes[0].Facts != 1 || !reflect.DeepEqual(attributes[0].Types, []string{"vector"}) || attributes[0].Dims == nil || *attributes[0].Dims != 2 {
		t.Fatalf("vector attribute = %#v", attributes[0])
	}
	if !attributes[1].Unique || attributes[1].Doc == nil || *attributes[1].Doc != "Display name" {
		t.Fatalf("name attribute = %#v", attributes[1])
	}
	if !attributes[2].Many || !reflect.DeepEqual(attributes[2].Types, []string{"int", "text"}) {
		t.Fatalf("tags attribute = %#v", attributes[2])
	}
	prefixed, err := db.Attributes(ctx, "person/", false)
	if err != nil || len(prefixed) != 2 {
		t.Fatalf("prefixed attributes = %#v, %v", prefixed, err)
	}
	system, err := db.Attributes(ctx, "fgraph/", true)
	if err != nil || len(system) == 0 || system[0].Name != "fgraph/at" {
		t.Fatalf("system attributes = %#v, %v", system, err)
	}
}

func TestAttributesSurfacesSQLiteFailures(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected attributes failure")
	base := func() []scriptedQuery {
		return []scriptedQuery{
			{contains: "SELECT id,name FROM fgraph_ids ORDER BY name", columns: []string{"id", "name"}, rows: [][]driver.Value{{int64(65), "item/value"}}},
			{contains: "SELECT f.id,f.e,f.a", columns: []string{"id", "e", "a", "v", "t", "tx", "rx"}},
			{contains: "SELECT f.t,COUNT(*)", columns: []string{"t", "count"}},
			{contains: "SELECT f.v,f.t", columns: []string{"v", "t"}},
		}
	}
	run := func(t *testing.T, rule scriptedQuery) error {
		t.Helper()
		rules := append([]scriptedQuery{rule}, base()...)
		runner := openScriptedSQL(t, scriptedSQL{queries: rules})
		db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
		_, err := db.Attributes(ctx, "", false)
		return err
	}
	cases := []struct {
		name string
		rule scriptedQuery
	}{
		{name: "identity query", rule: scriptedQuery{contains: "SELECT id,name", err: failure}},
		{name: "identity scan", rule: scriptedQuery{contains: "SELECT id,name", columns: []string{"id", "name"}, rows: [][]driver.Value{{int64(65)}}}},
		{name: "identity iteration", rule: scriptedQuery{contains: "SELECT id,name", columns: []string{"id", "name"}, nextErr: failure}},
		{name: "identity close", rule: scriptedQuery{contains: "SELECT id,name", columns: []string{"id", "name"}, closeErr: failure}},
		{name: "schema", rule: scriptedQuery{contains: "SELECT f.id,f.e,f.a", err: failure}},
		{name: "observed query", rule: scriptedQuery{contains: "SELECT f.t,COUNT(*)", err: failure}},
		{name: "observed scan", rule: scriptedQuery{contains: "SELECT f.t,COUNT(*)", columns: []string{"t", "count"}, rows: [][]driver.Value{{int64(TagInt)}}}},
		{name: "observed iteration", rule: scriptedQuery{contains: "SELECT f.t,COUNT(*)", columns: []string{"t", "count"}, nextErr: failure}},
		{name: "observed close", rule: scriptedQuery{contains: "SELECT f.t,COUNT(*)", columns: []string{"t", "count"}, closeErr: failure}},
		{name: "unknown tag", rule: scriptedQuery{contains: "SELECT f.t,COUNT(*)", columns: []string{"t", "count"}, rows: [][]driver.Value{{int64(99), int64(1)}}}},
		{name: "documentation query", rule: scriptedQuery{contains: "SELECT f.v,f.t", err: failure}},
		{name: "documentation type", rule: scriptedQuery{contains: "SELECT f.v,f.t", columns: []string{"v", "t"}, rows: [][]driver.Value{{int64(1), int64(TagInt)}}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := run(t, test.rule); !errors.Is(err, ErrFormat) {
				t.Fatalf("Attributes error = %v", err)
			}
		})
	}
}

func TestDoctorCheckOnlyAndExplicitRepair(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, t.TempDir()+"/doctor.db")
	if _, err := db.Transact(ctx, E{"id": "note", "note/text": "searchable"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "INSERT INTO fgraph_blobs(hash,data) VALUES (?,?)", []byte("orphan"), []byte("unused")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "DELETE FROM fgraph_fts"); err != nil {
		t.Fatal(err)
	}
	before := physicalState(t, db)

	checked, err := db.Doctor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if checked.OK || !checked.RepairNeeded || checked.Repaired || checked.FTSRows != 0 || checked.ExpectedFTSRows == 0 || checked.OrphanedBlobs != 1 {
		t.Fatalf("doctor check = %#v", checked)
	}
	if after := physicalState(t, db); !reflect.DeepEqual(after, before) {
		t.Fatal("check-only doctor mutated the database")
	}
	repaired, err := db.Doctor(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if !repaired.OK || repaired.RepairNeeded || !repaired.Repaired || repaired.FTSRows != repaired.ExpectedFTSRows || repaired.OrphanedBlobs != 0 || repaired.OrphanedBlobsRemoved != 1 {
		t.Fatalf("doctor repair = %#v", repaired)
	}

	path := db.store.path
	closeTest(t, db)
	readOnly, err := Open(path, WithReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTest(t, readOnly) })
	if report, err := readOnly.Doctor(ctx); err != nil || !report.OK {
		t.Fatalf("read-only doctor = %#v, %v", report, err)
	}
	if _, err := readOnly.Doctor(ctx, true); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only repair error = %v", err)
	}
	if _, err := readOnly.Doctor(ctx, false, false); !errors.Is(err, ErrType) {
		t.Fatalf("multiple repair flags error = %v", err)
	}
}

func TestDoctorFTSRowFailures(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected FTS failure")
	for _, test := range []struct {
		read func(sqlRunner) error
		name string
		rule scriptedQuery
	}{
		{name: "actual scan", rule: scriptedQuery{contains: "SELECT rowid,text", columns: []string{"rowid", "text"}, rows: [][]driver.Value{{int64(1)}}}, read: func(runner sqlRunner) error {
			_, err := readFTSRows(ctx, runner)
			return err
		}},
		{name: "actual iteration", rule: scriptedQuery{contains: "SELECT rowid,text", columns: []string{"rowid", "text"}, nextErr: failure}, read: func(runner sqlRunner) error {
			_, err := readFTSRows(ctx, runner)
			return err
		}},
		{name: "actual close", rule: scriptedQuery{contains: "SELECT rowid,text", columns: []string{"rowid", "text"}, closeErr: failure}, read: func(runner sqlRunner) error {
			_, err := readFTSRows(ctx, runner)
			return err
		}},
		{name: "expected scan", rule: scriptedQuery{contains: "SELECT f.id,CASE", columns: []string{"id", "text"}, rows: [][]driver.Value{{int64(1)}}}, read: func(runner sqlRunner) error {
			_, err := (&DB{}).readExpectedFTSRows(ctx, runner)
			return err
		}},
		{name: "expected query", rule: scriptedQuery{contains: "SELECT f.id,CASE", err: failure}, read: func(runner sqlRunner) error {
			_, err := (&DB{}).readExpectedFTSRows(ctx, runner)
			return err
		}},
		{name: "expected type", rule: scriptedQuery{contains: "SELECT f.id,CASE", columns: []string{"id", "text"}, rows: [][]driver.Value{{int64(1), int64(2)}}}, read: func(runner sqlRunner) error {
			_, err := (&DB{}).readExpectedFTSRows(ctx, runner)
			return err
		}},
		{name: "expected iteration", rule: scriptedQuery{contains: "SELECT f.id,CASE", columns: []string{"id", "text"}, nextErr: failure}, read: func(runner sqlRunner) error {
			_, err := (&DB{}).readExpectedFTSRows(ctx, runner)
			return err
		}},
		{name: "expected close", rule: scriptedQuery{contains: "SELECT f.id,CASE", columns: []string{"id", "text"}, closeErr: failure}, read: func(runner sqlRunner) error {
			_, err := (&DB{}).readExpectedFTSRows(ctx, runner)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			if err := test.read(runner); !errors.Is(err, ErrFormat) {
				t.Fatalf("FTS row error = %v", err)
			}
		})
	}
	if equalFTSRows([]ftsRow{{id: 1, text: "left"}}, []ftsRow{{id: 1, text: "right"}}) {
		t.Fatal("different FTS text rows compare equal")
	}
}

func TestVersionIsOneZeroOne(t *testing.T) {
	if Version != "1.0.1" {
		t.Fatalf("version = %q", Version)
	}
}

func TestCLISchemaAndDoctorRepair(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	ctx := context.Background()
	path := t.TempDir() + "/cli-v1.db"
	if _, err := runCLIForTest(t, "", "init", "--db", path, "--json"); err != nil {
		t.Fatal(err)
	}
	if _, err := runCLIForTest(t, "", "add", "--db", path, "--json", `{"id":"note","note/text":"searchable"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := runCLIForTest(t, "", "add", "--db", path, "--json", `{"id":"note-2","note/text":"also searchable"}`); err != nil {
		t.Fatal(err)
	}
	db, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	if _, err := db.store.sql.ExecContext(ctx, "INSERT INTO fgraph_blobs(hash,data) VALUES (?,?)", []byte("orphan"), []byte("unused")); err != nil {
		closeTest(t, db)
		t.Fatal(err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "DELETE FROM fgraph_fts"); err != nil {
		closeTest(t, db)
		t.Fatal(err)
	}
	closeTest(t, db)

	schemaOutput, schemaErr := runCLIForTest(t, "", "schema", "--db", path, "--json", "note/")
	if schemaErr != nil {
		t.Fatal(schemaErr)
	}
	var snapshot SchemaSnapshot
	if decodeErr := json.Unmarshal([]byte(schemaOutput), &snapshot); decodeErr != nil || len(snapshot.Attributes) != 1 || snapshot.Attributes[0].Name != "note/text" {
		t.Fatalf("CLI schema = %q, %#v, %v", schemaOutput, snapshot, decodeErr)
	}
	checkedOutput, checkErr := runCLIForTest(t, "", "doctor", "--db", path, "--json")
	if checkErr != nil {
		t.Fatal(checkErr)
	}
	var checked DoctorReport
	if decodeErr := json.Unmarshal([]byte(checkedOutput), &checked); decodeErr != nil || checked.OK || !checked.RepairNeeded {
		t.Fatalf("CLI doctor check = %q, %#v, %v", checkedOutput, checked, decodeErr)
	}
	repairedOutput, repairErr := runCLIForTest(t, "", "doctor", "--db", path, "--json", "--repair")
	if repairErr != nil {
		t.Fatal(repairErr)
	}
	var repaired DoctorReport
	if decodeErr := json.Unmarshal([]byte(repairedOutput), &repaired); decodeErr != nil || !repaired.OK || !repaired.Repaired || repaired.OrphanedBlobsRemoved != 1 {
		t.Fatalf("CLI doctor repair = %q, %#v, %v", repairedOutput, repaired, decodeErr)
	}

	query := `{"find":["?e"],"where":[["?e","note/text","_"]]}`
	if _, err := runCLIForTest(t, "", "q", "--db", path, "--query-budget", "1", query); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("CLI query budget error = %v, want TooLarge", err)
	}
	t.Setenv("FGRAPH_QUERY_BUDGET", "1")
	if _, err := runCLIForTest(t, "", "q", "--db", path, query); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("CLI query budget environment error = %v, want TooLarge", err)
	}
	if _, err := runCLIForTest(t, "", "search", "--db", path, "--text", "searchable", "--k", "0"); !errors.Is(err, ErrType) {
		t.Fatalf("CLI explicit zero search limit error = %v, want TypeError", err)
	}
	if _, err := runCLIForTest(t, "", "history", "--db", path, "note", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CLI explicit empty history attribute error = %v, want NotFoundError", err)
	}
	if _, err := runCLIForTest(t, "", "why", "--db", path, "note", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CLI explicit empty why attribute error = %v, want NotFoundError", err)
	}
}
