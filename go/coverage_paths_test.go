package fgraph

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

func requireErrorKind(t *testing.T, err, kind error) {
	t.Helper()
	if !errors.Is(err, kind) {
		t.Fatalf("error = %v, want %v", err, kind)
	}
}

func TestTransactionOwnerAndResolverBranches(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	for _, declaration := range []struct {
		attr string
		opts []DeclareOption
	}{
		{attr: "person/email", opts: []DeclareOption{Type("text"), Unique()}},
		{attr: "person/handle", opts: []DeclareOption{Type("text"), Unique()}},
	} {
		if _, err := db.Declare(ctx, declaration.attr, declaration.opts...); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Transact(ctx, E{"id": "owner-a", "person/email": "a@example.test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "owner-b", "person/handle": "bee"}); err != nil {
		t.Fatal(err)
	}
	_, err := db.Transact(ctx, E{"person/email": "a@example.test", "person/handle": "bee"})
	requireErrorKind(t, err, ErrConflict)
	_, err = db.Transact(ctx, E{"id": "owner-b", "person/email": "a@example.test"})
	requireErrorKind(t, err, ErrConflict)

	// The second unpinned map resolves the owner from the first map's pending
	// unique assertion, rather than allocating a second anonymous entity.
	report, err := db.Transact(ctx, []any{
		E{"person/email": "new@example.test", "person/note": "first"},
		E{"person/email": "new@example.test", "person/age": 42},
	})
	if err != nil {
		t.Fatal(err)
	}
	var owner any
	for _, fact := range report.Asserted {
		if fact.A == "person/email" {
			owner = fact.E
		}
		if (fact.A == "person/note" || fact.A == "person/age") && owner != nil && !reflect.DeepEqual(fact.E, owner) {
			t.Fatalf("pending unique owner diverged: %+v", report.Asserted)
		}
	}

	err = db.withRead(ctx, func(runner sqlRunner) error {
		alloc, allocErr := newAllocator(ctx, runner, db.store)
		if allocErr != nil {
			return allocErr
		}
		plan := &transactionPlan{tempids: map[string]int64{}, allocator: alloc}

		anonymous, pinned, resolveErr := db.resolveWriteEntity(ctx, runner, plan, nil, false)
		if resolveErr != nil || pinned || anonymous <= GenesisTx {
			t.Errorf("anonymous write entity = %d,%t,%v", anonymous, pinned, resolveErr)
		}
		if _, _, resolveErr = db.resolvePinnedWriteID(ctx, runner, -1); !errors.Is(resolveErr, ErrType) {
			t.Errorf("negative pinned id error = %v", resolveErr)
		}
		if _, found, lookupErr := db.resolveLookup(ctx, runner, plan, []any{"missing/unique", "x"}, false); lookupErr != nil || found {
			t.Errorf("read lookup of missing attribute = %t,%v", found, lookupErr)
		}
		if _, expandErr := db.expandAttributeValue(ctx, runner, plan, "item/value", attributeSchema{many: true}, []any{1, nil}); !errors.Is(expandErr, ErrType) {
			t.Errorf("invalid expanded item error = %v", expandErr)
		}
		if _, dimsErr := vectorDimensions(storedValue{logical: "bad"}, "item/vector"); !errors.Is(dimsErr, ErrType) {
			t.Errorf("malformed vector dimensions error = %v", dimsErr)
		}

		for _, target := range []any{int(-1), int64(-1), true, Object{Fields: []Field{{Name: "tmp", Value: 1}}}} {
			if _, _, refErr := db.resolveReference(ctx, runner, plan, target, false); !errors.Is(refErr, ErrType) {
				t.Errorf("invalid reference %#v error = %v", target, refErr)
			}
		}
		if id, found, refErr := db.resolveReference(ctx, runner, plan, TempID("reference"), false); refErr != nil || !found || id <= GenesisTx {
			t.Errorf("temp reference = %d,%t,%v", id, found, refErr)
		}
		if id, found, refErr := db.resolveReference(ctx, runner, plan, Object{Fields: []Field{{Name: "tmp", Value: "object-reference"}}}, false); refErr != nil || !found || id <= GenesisTx {
			t.Errorf("object temp reference = %d,%t,%v", id, found, refErr)
		}

		for _, operation := range [][]any{
			{1, "owner-a", "item/value", 1},
			{"bogus", "owner-a"},
			{"assert", "owner-a"},
			{"retract", "owner-a", "item/value", 1, 2},
			{"retract", "owner-a", 1},
		} {
			if operationErr := db.planOperation(ctx, runner, plan, operation); !errors.Is(operationErr, ErrType) {
				t.Errorf("invalid operation %#v error = %v", operation, operationErr)
			}
		}
		for _, spec := range []any{float64(1.5), math.NaN(), true, Object{Fields: []Field{{Name: "tmp", Value: 1}}}} {
			if _, _, entityErr := db.resolveOperationEntity(ctx, runner, plan, spec, true); !errors.Is(entityErr, ErrType) {
				t.Errorf("invalid operation entity %#v error = %v", spec, entityErr)
			}
		}
		if _, found, entityErr := db.resolveOperationEntity(ctx, runner, plan, TempID("absent"), false); entityErr != nil || found {
			t.Errorf("missing temp retract entity = %t,%v", found, entityErr)
		}
		if id, found, entityErr := db.resolveOperationEntity(ctx, runner, plan, TempID("created"), true); entityErr != nil || !found || id <= GenesisTx {
			t.Errorf("created temp operation entity = %d,%t,%v", id, found, entityErr)
		}
		if id, found, entityErr := db.resolveOperationEntity(ctx, runner, plan, Object{Fields: []Field{{Name: "tmp", Value: "created"}}}, true); entityErr != nil || !found || id <= GenesisTx {
			t.Errorf("reused object temp operation entity = %d,%t,%v", id, found, entityErr)
		}
		if _, _, entityErr := db.resolveOperationNumeric(ctx, runner, -1, true); !errors.Is(entityErr, ErrType) {
			t.Errorf("negative operation entity error = %v", entityErr)
		}
		if schemaErr := applySchemaFact(&attributeSchema{}, 8, int64(1)); !errors.Is(schemaErr, ErrFormat) {
			t.Errorf("malformed stored schema error = %v", schemaErr)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueryEvaluationBranchCoverage(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, []any{
		E{"id": "q-a", "item/name": "A", "item/value": 1},
		E{"id": "q-b", "item/name": "B", "item/value": 2},
	}); err != nil {
		t.Fatal(err)
	}

	if err := validateClauseBindings([]any{[]any{"=", "?unbound", 1}}, nil); !errors.Is(err, ErrQuery) {
		t.Fatalf("static predicate binding error = %v", err)
	}
	badOr := Object{Fields: []Field{{Name: "or", Value: []any{
		true,
		[]any{[]any{"=", "?unbound", 1}},
	}}}}
	if err := validateClauseBindings([]any{badOr}, nil); !errors.Is(err, ErrQuery) {
		t.Fatalf("nested or binding error = %v", err)
	}

	err := db.withRead(ctx, func(runner sqlRunner) error {
		facts, factsErr := db.queryFacts(ctx, runner)
		if factsErr != nil {
			return factsErr
		}
		evaluator := &queryEvaluator{
			ctx: ctx, runner: runner, db: db, facts: facts,
			rules: map[string][]ruleDef{}, relations: map[string][][]cell{},
		}
		qA := db.store.names["q-a"]

		matched, matchErr := evaluator.matchTerm(int64(qA), cell{tag: TagRef, value: qA}, binding{}, true)
		if matchErr != nil || !matched {
			t.Errorf("numeric entity match = %t,%v", matched, matchErr)
		}
		if _, predicateErr := evaluator.evalPredicate([]any{1, 1, 1}, []binding{{}}); !errors.Is(predicateErr, ErrQuery) {
			t.Errorf("non-text predicate operator error = %v", predicateErr)
		}
		if _, predicateErr := evaluator.evalPredicate([]any{"=", "?x", 1}, []binding{{}}); !errors.Is(predicateErr, ErrQuery) {
			t.Errorf("runtime unbound predicate error = %v", predicateErr)
		}
		if _, notErr := evaluator.evalNot([]any{[]any{"?x", "item/name", "A"}}, []binding{{}}); !errors.Is(notErr, ErrQuery) {
			t.Errorf("runtime unsafe negation error = %v", notErr)
		}
		if _, orErr := evaluator.evalOr(nil, []binding{{}}); !errors.Is(orErr, ErrQuery) {
			t.Errorf("empty or error = %v", orErr)
		}
		if _, orErr := evaluator.evalOr([]any{true}, []binding{{}}); !errors.Is(orErr, ErrQuery) {
			t.Errorf("non-array or branch error = %v", orErr)
		}
		orBindings, orErr := evaluator.evalOr([]any{
			[]any{[]any{"?x", "item/name", "A"}},
			[]any{[]any{"?y", "item/name", "B"}},
		}, []binding{{}})
		if orErr != nil || len(orBindings) != 2 {
			t.Errorf("branch-local or bindings = %#v, %v", orBindings, orErr)
		}

		if _, ok := orderedCompare(int64(1), "1"); ok {
			t.Error("mixed numeric/text order unexpectedly comparable")
		}
		if _, ok := orderedCompare(false, int64(0)); ok {
			t.Error("mixed bool/int order unexpectedly comparable")
		}
		if _, ok := orderedCompare([]byte{1}, []byte{1}); ok {
			t.Error("byte order unexpectedly comparable")
		}
		if _, ok := numericRat(struct{}{}); ok {
			t.Error("struct unexpectedly numeric")
		}

		bindings := []binding{
			{"?name": {tag: TagText, value: "A"}, "?sort": {tag: TagText, value: "a"}},
			{"?name": {tag: TagText, value: "B"}, "?sort": {tag: TagBool, value: true}},
		}
		projected, projectErr := evaluator.project(Q{
			Find: []any{"?name"}, In: []string{"?sort"},
			Order: []any{[]any{"?sort", "asc"}},
		}, bindings)
		if projectErr != nil || !rowsEqual(projected.Rows, [][]any{{"B"}, {"A"}}) {
			t.Errorf("total rendered order = %#v, %v", projected.Rows, projectErr)
		}
		_, projectErr = evaluator.project(Q{Find: []any{[]any{"sum", "?value"}}}, []binding{
			{"?value": {tag: TagInt, value: "malformed"}},
		})
		if !errors.Is(projectErr, ErrQuery) {
			t.Errorf("aggregate projection error = %v", projectErr)
		}
		_, projectErr = evaluator.project(Q{Find: []any{"?missing"}}, []binding{{}})
		if !errors.Is(projectErr, ErrQuery) {
			t.Errorf("row projection error = %v", projectErr)
		}

		if findAggregate([]any{true, "?x"}) != "" {
			t.Error("non-text aggregate name accepted")
		}
		if _, _, aggregateErr := evaluator.aggregateRow([]any{"?missing"}, []binding{{}}); !errors.Is(aggregateErr, ErrQuery) {
			t.Errorf("non-aggregate group projection error = %v", aggregateErr)
		}
		if _, aggregateErr := aggregate("sum", []cell{
			{tag: TagFloat, value: float64(1)},
			{tag: TagInt, value: int64(2)},
		}); aggregateErr != nil {
			t.Errorf("mixed numeric aggregate = %v", aggregateErr)
		}

		evaluator.rules["empty"] = []ruleDef{{name: "empty", args: []string{"?x"}}}
		evaluator.relations["empty"] = nil
		for _, invocation := range [][]any{nil, {1, "?x"}, {"empty"}} {
			if _, ruleErr := evaluator.evalRule(invocation, []binding{{}}); !errors.Is(ruleErr, ErrQuery) {
				t.Errorf("invalid rule invocation %#v error = %v", invocation, ruleErr)
			}
		}

		evaluator.rules = map[string][]ruleDef{
			"base":  {{name: "base", args: []string{"?x"}, body: []any{[]any{"?e", "item/name", "?x"}}}},
			"left":  {{name: "left", args: []string{"?x"}, body: []any{Object{Fields: []Field{{Name: "rule", Value: []any{"base", "?x"}}}}}}},
			"right": {{name: "right", args: []string{"?x"}, body: []any{Object{Fields: []Field{{Name: "rule", Value: []any{"base", "?x"}}}}}}},
		}
		evaluator.relations = map[string][][]cell{}
		if relationErr := evaluator.buildRelations(); relationErr != nil {
			t.Errorf("shared rule dependency build = %v", relationErr)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestInitializationAndAllocatorFailureCoverage(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected SQLite failure")
	validClock := func() int64 { return 1_767_225_600_000_000 }

	for _, test := range []struct {
		kind  error
		name  string
		match string
	}{
		{name: "begin", match: "BEGIN IMMEDIATE", kind: ErrConflict},
		{name: "schema", match: "CREATE TABLE fgraph_meta", kind: ErrFormat},
		{name: "metadata", match: "INSERT INTO fgraph_meta", kind: ErrFormat},
		{name: "system name", match: "INSERT INTO fgraph_ids", kind: ErrFormat},
		{name: "genesis timestamp", match: "VALUES (64,1", kind: ErrFormat},
		{name: "system type", match: "VALUES (?,8", kind: ErrFormat},
		{name: "type search row", match: "INSERT INTO fgraph_fts", kind: ErrFormat},
		{name: "system documentation", match: "VALUES (?,10", kind: ErrFormat},
		{name: "application marker", match: "PRAGMA application_id", kind: ErrFormat},
		{name: "version marker", match: "PRAGMA user_version", kind: ErrFormat},
		{name: "commit", match: "COMMIT", kind: ErrFormat},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{execs: []scriptedExec{{contains: test.match, err: failure}}})
			root := &store{sql: runner, clock: validClock, names: map[string]int64{}}
			requireErrorKind(t, root.initialize(ctx), test.kind)
		})
	}

	t.Run("invalid genesis clock", func(t *testing.T) {
		runner := openScriptedSQL(t, scriptedSQL{})
		root := &store{sql: runner, clock: func() int64 { return maxInstantMicros + 1 }, names: map[string]int64{}}
		requireErrorKind(t, root.initialize(ctx), ErrType)
	})
	t.Run("rollback failure is retained", func(t *testing.T) {
		runner := openScriptedSQL(t, scriptedSQL{execs: []scriptedExec{
			{contains: "CREATE TABLE fgraph_meta", err: failure},
			{contains: "ROLLBACK", err: failure},
		}})
		root := &store{sql: runner, clock: validClock, names: map[string]int64{}}
		requireErrorKind(t, root.initialize(ctx), ErrFormat)
	})
	for _, test := range []struct {
		name  string
		match string
	}{
		{name: "system type fact id", match: "VALUES (?,8"},
		{name: "system documentation fact id", match: "VALUES (?,10"},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{execs: []scriptedExec{{contains: test.match, lastInsertIDErr: failure}}})
			root := &store{sql: runner, clock: validClock, names: map[string]int64{}}
			requireErrorKind(t, root.initialize(ctx), ErrFormat)
		})
	}
	t.Run("connection acquisition", func(t *testing.T) {
		runner := openScriptedSQL(t, scriptedSQL{})
		if err := runner.Close(); err != nil {
			t.Fatal(err)
		}
		root := &store{sql: runner, clock: validClock, names: map[string]int64{}}
		requireErrorKind(t, root.initialize(ctx), ErrFormat)
	})

	t.Run("configuration", func(t *testing.T) {
		runner := openScriptedSQL(t, scriptedSQL{execs: []scriptedExec{{contains: "PRAGMA busy_timeout", err: failure}}})
		root := &store{sql: runner, names: map[string]int64{}}
		requireErrorKind(t, root.configure(ctx, true), ErrFormat)
	})
	for _, test := range []struct {
		name    string
		queries []scriptedQuery
	}{
		{name: "application id", queries: []scriptedQuery{{contains: "PRAGMA application_id", err: failure}}},
		{name: "user version", queries: []scriptedQuery{
			{contains: "PRAGMA application_id", columns: []string{"application_id"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "PRAGMA user_version", err: failure},
		}},
		{name: "partial object inspection", queries: []scriptedQuery{
			{contains: "PRAGMA application_id", columns: []string{"application_id"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "PRAGMA user_version", columns: []string{"user_version"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "COUNT(*) FROM sqlite_master", err: failure},
		}},
	} {
		t.Run("format "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: test.queries})
			root := &store{sql: runner, clock: validClock, names: map[string]int64{}}
			requireErrorKind(t, root.checkOrInit(ctx), ErrFormat)
		})
	}

	t.Run("name cache query", func(t *testing.T) {
		runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{
			{contains: "PRAGMA data_version", columns: []string{"data_version"}, rows: [][]driver.Value{{int64(1)}}},
			{contains: "SELECT id,name", err: failure},
		}})
		root := &store{sql: runner, dataVersion: -1, names: map[string]int64{}}
		requireErrorKind(t, root.refreshNames(ctx, runner), ErrFormat)
	})

	newAllocatorRunner := func(t *testing.T, extraQueries []scriptedQuery, execs []scriptedExec) (*allocator, *sql.DB) {
		t.Helper()
		queries := append([]scriptedQuery{{
			contains: "SELECT value FROM fgraph_meta", columns: []string{"value"}, rows: [][]driver.Value{{int64(65)}},
		}}, extraQueries...)
		runner := openScriptedSQL(t, scriptedSQL{queries: queries, execs: execs})
		alloc, err := newAllocator(ctx, runner, &store{sql: runner, names: map[string]int64{}})
		if err != nil {
			t.Fatal(err)
		}
		return alloc, runner
	}
	t.Run("name lookup", func(t *testing.T) {
		alloc, _ := newAllocatorRunner(t, []scriptedQuery{{contains: "SELECT id FROM fgraph_ids", err: failure}}, nil)
		_, _, err := alloc.name(ctx, "entity", false, false)
		requireErrorKind(t, err, ErrFormat)
	})
	t.Run("name absent", func(t *testing.T) {
		alloc, _ := newAllocatorRunner(t, []scriptedQuery{{contains: "SELECT id FROM fgraph_ids", err: sql.ErrNoRows}}, nil)
		if id, found, err := alloc.name(ctx, "entity", false, false); err != nil || found || id != 0 {
			t.Fatalf("absent name = %d,%t,%v", id, found, err)
		}
	})
	t.Run("name allocation", func(t *testing.T) {
		alloc, _ := newAllocatorRunner(t,
			[]scriptedQuery{{contains: "SELECT id FROM fgraph_ids", err: sql.ErrNoRows}},
			[]scriptedExec{{contains: "INSERT INTO fgraph_ids", err: failure}},
		)
		if _, _, err := alloc.name(ctx, "entity", false, true); err != nil {
			t.Fatal(err)
		}
		tx, err := alloc.tx()
		if err != nil {
			t.Fatal(err)
		}
		requireErrorKind(t, alloc.finalize(ctx, tx, [16]byte{}), ErrFormat)
	})
	t.Run("allocator flush", func(t *testing.T) {
		alloc, _ := newAllocatorRunner(t, nil, []scriptedExec{{contains: "UPDATE fgraph_meta", err: failure}})
		if _, err := alloc.anonymous(); err != nil {
			t.Fatal(err)
		}
		requireErrorKind(t, alloc.flush(ctx), ErrFormat)
	})
}

func TestDoctorScriptedFailureCoverage(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected doctor failure")
	baseQueries := func() []scriptedQuery {
		reference, err := referenceSchemaObjects(ctx)
		if err != nil {
			t.Fatal(err)
		}
		names := make([]string, 0, len(reference))
		for name := range reference {
			names = append(names, name)
		}
		sort.Strings(names)
		schemaRows := make([][]driver.Value, 0, len(names))
		for _, name := range names {
			object := reference[name]
			schemaRows = append(schemaRows, []driver.Value{object.name, object.kind, object.sql})
		}
		systemRows := make([][]driver.Value, 0, len(systemNames)-1)
		for id := 1; id < len(systemNames); id++ {
			systemRows = append(systemRows, []driver.Value{int64(id), []byte(systemNames[id]), nil, int64(GenesisTx)})
		}
		genesisRows := make([][]driver.Value, 0, len(systemTypes)+len(systemDocs)-2)
		for id := 1; id < len(systemTypes); id++ {
			genesisRows = append(genesisRows, []driver.Value{
				int64(id + 1), int64(id), int64(8), []byte(systemTypes[id]), "text",
				int64(TagText), int64(GenesisTx), nil,
			})
		}
		for id := 1; id < len(systemDocs); id++ {
			genesisRows = append(genesisRows, []driver.Value{
				int64(id + len(systemTypes)), int64(id), int64(10), []byte(systemDocs[id]), "text",
				int64(TagText), int64(GenesisTx), nil,
			})
		}
		return []scriptedQuery{
			{contains: "FROM sqlite_schema", columns: []string{"name", "type", "sql"}, rows: schemaRows},
			{contains: "PRAGMA integrity_check", columns: []string{"integrity_check"}, rows: [][]driver.Value{{"ok"}}},
			{contains: "SELECT MAX(identifier)", columns: []string{"maximum"}, rows: [][]driver.Value{{int64(64)}}},
			{contains: "key='next_id'", columns: []string{"value"}, rows: [][]driver.Value{{int64(65)}}},
			{contains: "FROM fgraph_ids WHERE id<=0", columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "SELECT id,CAST(name AS BLOB)", columns: []string{"id", "name", "gid", "created_tx"}, rows: systemRows},
			{contains: "WHERE id<=0 OR e<=0", columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "i.name IS NOT NULL AND EXISTS", columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "event.tx=f.tx", columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "event.tx=f.rx", columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "key='created_at'", columns: []string{"value"}, rows: [][]driver.Value{{int64(1)}}},
			{contains: "SELECT v,t,tx,rx", columns: []string{"v", "t", "tx", "rx"}, rows: [][]driver.Value{{int64(1), int64(TagInstant), int64(GenesisTx), nil}}},
			{contains: "SELECT id,e,a,CAST(v AS BLOB)", columns: []string{"id", "e", "a", "v", "storage_class", "t", "tx", "rx"}, rows: genesisRows},
			{contains: "referenced WHERE NOT EXISTS", columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "i.created_tx", columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "LEFT JOIN fgraph_ids i ON i.id=event.tx", columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "LEFT JOIN fgraph_ids", columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "SELECT t,typeof(v)", columns: []string{"t", "storage_class", "scalar", "raw"}},
			{contains: "WHERE f.t IN (7,8,9)", columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "SELECT b.rowid,b.hash,b.data,f.t", columns: []string{"rowid", "hash", "data", "t"}},
			{contains: "rx IS NOT NULL AND rx<=tx", columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "SELECT ev.tx,ev.event_hash", columns: []string{"tx", "event_hash", "event_data", "operation_id", "gid"}},
			{contains: "SELECT DISTINCT a FROM fgraph_facts", columns: []string{"a"}},
			{contains: "SELECT DISTINCT e FROM fgraph_facts WHERE a=15", columns: []string{"e"}},
			{contains: "COUNT(*) FROM fgraph_blobs", columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "SELECT rowid,text", columns: []string{"rowid", "text"}},
			{contains: "COUNT(*) FROM fgraph_facts WHERE rx IS NULL AND t IN (4,8)", columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}},
			{contains: "SELECT f.id,CASE", columns: []string{"id", "text"}},
		}
	}
	runDoctor := func(t *testing.T, repair bool, first []scriptedQuery, execs []scriptedExec) error {
		t.Helper()
		queries := append(append([]scriptedQuery{}, first...), baseQueries()...)
		runner := openScriptedSQL(t, scriptedSQL{queries: queries, execs: execs})
		db := &DB{store: &store{
			sql: runner, path: ":memory:", clock: func() int64 { return 1_767_225_600_000_000 },
			names: map[string]int64{},
		}}
		_, err := db.Doctor(ctx, repair)
		return err
	}

	for _, test := range []struct {
		name  string
		query scriptedQuery
	}{
		{name: "integrity query", query: scriptedQuery{contains: "PRAGMA integrity_check", err: failure}},
		{name: "integrity scan", query: scriptedQuery{
			contains: "PRAGMA integrity_check", columns: []string{"message", "extra"}, rows: [][]driver.Value{{"ok", "extra"}},
		}},
		{name: "integrity iteration", query: scriptedQuery{
			contains: "PRAGMA integrity_check", columns: []string{"message"}, nextErr: failure,
		}},
		{name: "allocator maximum", query: scriptedQuery{contains: "SELECT MAX(identifier)", err: failure}},
		{name: "next id", query: scriptedQuery{contains: "key='next_id'", err: failure}},
		{name: "identity ids", query: scriptedQuery{contains: "FROM fgraph_ids WHERE id<=0", err: failure}},
		{name: "system identities", query: scriptedQuery{contains: "SELECT id,CAST(name AS BLOB)", err: failure}},
		{name: "fact ids", query: scriptedQuery{contains: "WHERE id<=0 OR e<=0", err: failure}},
		{name: "named transactions", query: scriptedQuery{contains: "i.name IS NOT NULL AND EXISTS", err: failure}},
		{name: "asserting transactions", query: scriptedQuery{contains: "event.tx=f.tx", err: failure}},
		{name: "retracting transactions", query: scriptedQuery{contains: "event.tx=f.rx", err: failure}},
		{name: "created at", query: scriptedQuery{contains: "key='created_at'", err: failure}},
		{name: "genesis", query: scriptedQuery{contains: "SELECT v,t,tx,rx", err: failure}},
		{name: "genesis facts", query: scriptedQuery{contains: "SELECT id,e,a,CAST(v AS BLOB)", err: failure}},
		{name: "orphan count", query: scriptedQuery{contains: "COUNT(*) FROM fgraph_blobs", err: failure}},
		{name: "missing blobs", query: scriptedQuery{contains: "WHERE f.t IN (7,8,9)", err: failure}},
		{name: "blob validation", query: scriptedQuery{contains: "SELECT b.rowid,b.hash,b.data,f.t", err: failure}},
		{name: "invalid interval", query: scriptedQuery{contains: "rx IS NOT NULL AND rx<=tx", err: failure}},
		{name: "dangling attribute", query: scriptedQuery{contains: "LEFT JOIN fgraph_ids", err: failure}},
		{name: "physical values", query: scriptedQuery{contains: "SELECT t,typeof(v)", err: failure}},
		{name: "FTS rows", query: scriptedQuery{contains: "SELECT rowid,text", err: failure}},
		{name: "expected FTS count", query: scriptedQuery{contains: "COUNT(*) FROM fgraph_facts WHERE rx IS NULL", err: failure}},
		{name: "expected FTS rows", query: scriptedQuery{contains: "SELECT f.id,CASE", err: failure}},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireErrorKind(t, runDoctor(t, false, []scriptedQuery{test.query}, nil), ErrFormat)
		})
	}
	t.Run("integrity violation", func(t *testing.T) {
		query := scriptedQuery{contains: "PRAGMA integrity_check", columns: []string{"message"}, rows: [][]driver.Value{{"malformed page"}}}
		queries := append([]scriptedQuery{query}, baseQueries()...)
		runner := openScriptedSQL(t, scriptedSQL{queries: queries})
		db := &DB{store: &store{
			sql: runner, path: ":memory:", clock: func() int64 { return 1_767_225_600_000_000 },
			names: map[string]int64{},
		}}
		report, err := db.Doctor(ctx)
		if err != nil || report.OK || len(report.Problems) == 0 {
			t.Fatalf("integrity violation report = %+v, %v", report, err)
		}
	})

	for _, test := range []struct {
		kind  error
		name  string
		match string
	}{
		{name: "begin", match: "BEGIN IMMEDIATE", kind: ErrConflict},
		{name: "orphan delete", match: "DELETE FROM fgraph_blobs", kind: ErrFormat},
		{name: "fts clear", match: "DELETE FROM fgraph_fts", kind: ErrFormat},
		{name: "fts rebuild", match: "INSERT INTO fgraph_fts", kind: ErrFormat},
		{name: "analyze", match: "ANALYZE", kind: ErrFormat},
		{name: "commit", match: "COMMIT", kind: ErrFormat},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireErrorKind(t, runDoctor(t, true, nil, []scriptedExec{{contains: test.match, err: failure}}), test.kind)
		})
	}
	t.Run("removed blob row count", func(t *testing.T) {
		exec := scriptedExec{contains: "DELETE FROM fgraph_blobs", rowsAffectedErr: failure}
		requireErrorKind(t, runDoctor(t, true, nil, []scriptedExec{exec}), ErrFormat)
	})
}

func TestCanonicalJSONBoundaryCoverage(t *testing.T) {
	if _, err := DecodeJSON(errorReader{}); !errors.Is(err, ErrType) {
		t.Fatalf("reader failure = %v", err)
	}
	for _, raw := range []string{`{"value":`, `[1,`, `1 trailing`} {
		if _, err := DecodeJSON(strings.NewReader(raw)); !errors.Is(err, ErrType) {
			t.Errorf("malformed JSON %q error = %v", raw, err)
		}
	}
	invalidText := string([]byte{0xff})
	for _, value := range []any{
		invalidText,
		map[string]any{invalidText: true},
		[]any{make(chan int)},
		map[string]any{"nested": make(chan int)},
	} {
		if err := validateJSON(value); !errors.Is(err, ErrType) {
			t.Errorf("validateJSON(%T) error = %v", value, err)
		}
	}
	for _, value := range []any{
		invalidText,
		map[string]any{invalidText: true},
		[]any{make(chan int)},
		map[string]any{"nested": make(chan int)},
		make(chan int),
	} {
		var output bytes.Buffer
		if err := writeCanonicalJSON(&output, value); !errors.Is(err, ErrType) {
			t.Errorf("writeCanonicalJSON(%T) error = %v", value, err)
		}
	}
	if stored, err := scalarValue([]float32{1, 2}); err != nil || stored.tag != TagVector {
		t.Fatalf("native float32 vector = %#v,%v", stored, err)
	}
	if got := normalizeJSONNumbers(json.Number("not-a-number")); got != "not-a-number" {
		t.Fatalf("invalid JSON number normalization = %#v", got)
	}
}

func TestExportScriptedFailureCoverage(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected export failure")
	emptyAssertions := scriptedQuery{
		contains: "WHERE tx=?", columns: []string{"id", "e", "a", "v", "t", "tx", "rx"},
	}
	emptyRetractions := scriptedQuery{
		contains: "WHERE rx=?", columns: []string{"id", "e", "a", "v", "t", "tx", "rx"},
	}
	run := func(t *testing.T, queries []scriptedQuery) error {
		t.Helper()
		queries = append([]scriptedQuery{
			{contains: "SELECT gid FROM fgraph_ids", columns: []string{"gid"}, rows: [][]driver.Value{{make([]byte, 16)}}},
			{contains: "SELECT name,gid FROM fgraph_ids", columns: []string{"name", "gid"}},
		}, queries...)
		runner := openScriptedSQL(t, scriptedSQL{queries: queries})
		db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
		_, err := db.exportTransaction(ctx, runner, 65, 1)
		return err
	}
	badScan := scriptedQuery{
		contains: "WHERE tx=?", columns: []string{"id", "e", "a", "v", "t", "tx", "rx"},
		rows: [][]driver.Value{{int64(1)}},
	}
	badLogical := scriptedQuery{
		contains: "WHERE tx=?", columns: []string{"id", "e", "a", "v", "t", "tx", "rx"},
		rows: [][]driver.Value{{int64(1), int64(66), int64(20), "not-bytes", int64(TagBytes), int64(65), nil}},
	}
	badTxAttribute := scriptedQuery{
		contains: "WHERE tx=?", columns: []string{"id", "e", "a", "v", "t", "tx", "rx"},
		rows: [][]driver.Value{{int64(1), int64(65), int64(999), int64(1), int64(TagInt), int64(65), nil}},
	}
	for _, test := range []struct {
		name    string
		queries []scriptedQuery
	}{
		{name: "assertion query", queries: []scriptedQuery{{contains: "WHERE tx=?", err: failure}}},
		{name: "assertion scan", queries: []scriptedQuery{badScan}},
		{name: "assertion logical value", queries: []scriptedQuery{badLogical}},
		{name: "transaction attribute", queries: []scriptedQuery{badTxAttribute}},
		{name: "retraction query", queries: []scriptedQuery{emptyAssertions, {contains: "WHERE rx=?", err: failure}}},
		{name: "retraction scan", queries: []scriptedQuery{emptyAssertions, {
			contains: "WHERE rx=?", columns: []string{"id", "e", "a", "v", "t", "tx", "rx"}, rows: [][]driver.Value{{int64(1)}},
		}}},
		{name: "retraction logical value", queries: []scriptedQuery{emptyAssertions, {
			contains: "WHERE rx=?", columns: []string{"id", "e", "a", "v", "t", "tx", "rx"},
			rows: [][]driver.Value{{int64(1), int64(66), int64(20), "not-bytes", int64(TagBytes), int64(64), int64(65)}},
		}}},
		{name: "retraction attribute", queries: []scriptedQuery{emptyAssertions, {
			contains: "WHERE rx=?", columns: []string{"id", "e", "a", "v", "t", "tx", "rx"},
			rows: [][]driver.Value{{int64(1), int64(66), int64(999), int64(1), int64(TagInt), int64(64), int64(65)}},
		}}},
		{name: "baseline scripted receipt", queries: []scriptedQuery{emptyAssertions, emptyRetractions}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := run(t, test.queries)
			if test.name == "baseline scripted receipt" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			requireErrorKind(t, err, ErrFormat)
		})
	}
}

func TestRemainingQueryAndTransactionCoverage(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Query(ctx, Q{Find: []any{"?input"}, In: []string{"?input"}}, nil); !errors.Is(err, ErrQuery) {
		t.Fatalf("missing query input error = %v", err)
	}

	evaluator := &queryEvaluator{db: db, ctx: ctx, rules: map[string][]ruleDef{}, relations: map[string][][]cell{}}
	if _, err := evaluator.project(Q{Find: []any{"?value"}}, []binding{
		{"?value": {tag: TagJSON, value: make(chan int)}},
	}); !errors.Is(err, ErrQuery) {
		t.Fatalf("non-canonical projected row error = %v", err)
	}
	equalBindings := []binding{
		{"?name": {tag: TagText, value: "A"}, "?sort": {tag: TagInt, value: int64(1)}},
		{"?name": {tag: TagText, value: "B"}, "?sort": {tag: TagInt, value: int64(1)}},
	}
	result, err := evaluator.project(Q{
		Find: []any{"?name"}, In: []string{"?sort"}, Limit: queryLimit(1),
		Order: []any{[]any{"?sort", "asc"}},
	}, equalBindings)
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("equal ordering with limit = %#v,%v", result, err)
	}

	failure := errors.New("injected transaction failure")
	runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{
		{contains: "SELECT value FROM fgraph_meta", columns: []string{"value"}, rows: [][]driver.Value{{int64(65)}}},
		{contains: "SELECT id FROM fgraph_ids", columns: []string{"id"}, rows: [][]driver.Value{{int64(70)}}},
	}})
	alloc, err := newAllocator(ctx, runner, &store{sql: runner, names: map[string]int64{}})
	if err != nil {
		t.Fatal(err)
	}
	if id, found, nameErr := alloc.name(ctx, "existing", false, false); nameErr != nil || !found || id != 70 {
		t.Fatalf("database-resolved allocator name = %d,%t,%v", id, found, nameErr)
	}

	err = db.withRead(ctx, func(runner sqlRunner) error {
		_, schemaErr := db.schemaFor(ctx, runner, 999, []plannedFact{{
			e: 999, a: 8, value: storedValue{storage: int64(1)},
		}})
		return schemaErr
	})
	requireErrorKind(t, err, ErrFormat)

	lastIDRunner := openScriptedSQL(t, scriptedSQL{execs: []scriptedExec{{
		contains: "INSERT INTO fgraph_facts", lastInsertIDErr: failure,
	}}})
	_, err = db.insertFact(ctx, lastIDRunner, plannedFact{
		e: 65, a: 66, attr: "item/value", value: storedValue{logical: int64(1), storage: int64(1), tag: TagInt},
	}, 67)
	requireErrorKind(t, err, ErrFormat)
	textRunner := openScriptedSQL(t, scriptedSQL{})
	_, err = db.insertFact(ctx, textRunner, plannedFact{
		e: 65, a: 66, attr: "item/text", value: storedValue{logical: int64(1), storage: "text", tag: TagText},
	}, 67)
	requireErrorKind(t, err, ErrFormat)
	invalidAt := maxInstantMicros + 1
	if _, err := db.nextTimestamp(ctx, textRunner, &invalidAt); !errors.Is(err, ErrType) {
		t.Fatalf("invalid timestamp override error = %v", err)
	}

	trueValue, falseValue, textType := true, false, "text"
	declarationDB := &DB{store: &store{names: map[string]int64{"item/value": 65}}}
	for _, test := range []struct {
		config declareOptions
		name   string
	}{
		{name: "unique schema", config: declareOptions{unique: &trueValue}},
		{name: "unique duplicates", config: declareOptions{unique: &trueValue, typeName: &textType}},
		{name: "cardinality", config: declareOptions{many: &falseValue}},
		{name: "type", config: declareOptions{typeName: &textType}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{})
			requireErrorKind(t, declarationDB.validateDeclarationOn(ctx, runner, "item/value", test.config), ErrFormat)
		})
	}
}

func TestTailWriterFailureCancelsFollower(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tail-cancel.db")
	db := fixedDB(t, path)
	if _, err := db.Transact(ctx, E{"id": "tail", "item/value": true}); err != nil {
		t.Fatal(err)
	}
	closeTest(t, db)

	baseline := countFollowGoroutines()
	err := RunCLI(ctx, []string{
		"fgraph", "--db", path, "tail", "--since", "64", "--follow",
	}, strings.NewReader(""), errorWriter{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("tail with a failed writer unexpectedly succeeded")
	}
	deadline := time.Now().Add(time.Second)
	for countFollowGoroutines() > baseline && time.Now().Before(deadline) {
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
	if got := countFollowGoroutines(); got > baseline {
		t.Fatalf("tail left %d follower goroutines, baseline %d", got, baseline)
	}
}

func countFollowGoroutines() int {
	buffer := make([]byte, 1<<20)
	used := runtime.Stack(buffer, true)
	return bytes.Count(buffer[:used], []byte("github.com/fmind/fgraph/go.(*DB).Follow.func1"))
}
