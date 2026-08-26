package fgraph

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestReadSelectorAndAttributeFaultBoundaries(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	for _, selector := range []any{
		map[string]any{"eid": 1},
		map[string]any{"eid": "not-a-uuid"},
		map[string]any{"eid": "11111111-1111-4111-8111-111111111111"},
		map[string]any{"other": "value"},
	} {
		if _, err := db.Entity(ctx, selector); !errors.Is(err, ErrType) && !errors.Is(err, ErrNotFound) {
			t.Errorf("Entity(%#v) error = %v", selector, err)
		}
	}
	if _, err := db.Entity(ctx, int64(0)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("zero numeric entity error = %v", err)
	}

	failure := errors.New("read fault")
	runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{{contains: "WHERE gid", err: failure}}})
	faultDB := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
	if _, _, err := faultDB.resolveReadEntity(ctx, runner, map[string]any{"eid": "11111111-1111-4111-8111-111111111111"}); !errors.Is(err, ErrFormat) {
		t.Fatalf("eid lookup fault = %v", err)
	}

	for _, test := range []struct {
		name string
		rule scriptedQuery
	}{
		{name: "query", rule: scriptedQuery{contains: "SELECT f.t,COUNT", err: failure}},
		{name: "scan", rule: scriptedQuery{contains: "SELECT f.t,COUNT", columns: []string{"tag", "count"}, rows: [][]driver.Value{{int64(1)}}}},
		{name: "unknown tag", rule: scriptedQuery{contains: "SELECT f.t,COUNT", columns: []string{"tag", "count"}, rows: [][]driver.Value{{int64(99), int64(1)}}}},
		{name: "iteration", rule: scriptedQuery{contains: "SELECT f.t,COUNT", columns: []string{"tag", "count"}, nextErr: failure}},
		{name: "close", rule: scriptedQuery{contains: "SELECT f.t,COUNT", columns: []string{"tag", "count"}, closeErr: failure}},
	} {
		t.Run("observed "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
			if _, _, err := db.observedAttributeTypes(ctx, runner, 65, "item/value"); !errors.Is(err, ErrFormat) {
				t.Fatalf("observed types fault = %v", err)
			}
		})
	}

	for _, test := range []struct {
		name string
		rule scriptedQuery
	}{
		{name: "query", rule: scriptedQuery{contains: "SELECT f.v,f.t", err: failure}},
		{name: "logical", rule: scriptedQuery{contains: "SELECT f.v,f.t", columns: []string{"value", "tag"}, rows: [][]driver.Value{{"bad", int64(TagInt)}}}},
		{name: "non-text", rule: scriptedQuery{contains: "SELECT f.v,f.t", columns: []string{"value", "tag"}, rows: [][]driver.Value{{int64(1), int64(TagInt)}}}},
	} {
		t.Run("doc "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
			if _, _, err := db.attributeDoc(ctx, runner, 65); !errors.Is(err, ErrFormat) {
				t.Fatalf("attribute doc fault = %v", err)
			}
		})
	}
}

func TestAttributeAndStatisticsRowFaults(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("row fault")
	basis := scriptedQuery{contains: "SELECT COALESCE(MAX(tx)", columns: []string{"basis"}, rows: [][]driver.Value{{int64(GenesisTx)}}}
	for _, test := range []struct {
		name string
		rule scriptedQuery
	}{
		{name: "query", rule: scriptedQuery{contains: "SELECT id,name FROM", err: failure}},
		{name: "scan", rule: scriptedQuery{contains: "SELECT id,name FROM", columns: []string{"id", "name"}, rows: [][]driver.Value{{int64(65)}}}},
		{name: "iteration", rule: scriptedQuery{contains: "SELECT id,name FROM", columns: []string{"id", "name"}, nextErr: failure}},
		{name: "close", rule: scriptedQuery{contains: "SELECT id,name FROM", columns: []string{"id", "name"}, closeErr: failure}},
	} {
		t.Run("attributes "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{basis, test.rule}})
			db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
			if _, err := db.Attributes(ctx, "", false); !errors.Is(err, ErrFormat) {
				t.Fatalf("Attributes row fault = %v", err)
			}
		})
	}

	for _, rule := range []scriptedQuery{
		{contains: "SELECT name", columns: []string{"name"}, rows: [][]driver.Value{{}}},
		{contains: "SELECT name", columns: []string{"name"}, nextErr: failure},
		{contains: "SELECT name", columns: []string{"name"}, closeErr: failure},
	} {
		runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{rule}})
		rows, err := runner.QueryContext(ctx, "SELECT name")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := countAttributeRows(rows); !errors.Is(err, ErrFormat) {
			t.Errorf("countAttributeRows fault = %v", err)
		}
	}
}

func TestSearchCandidateAndCompactProjectionFaults(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("search row fault")
	filter := []resolvedSearchFilter{{a: 65, value: storedValue{storage: "x", tag: TagText}}}
	for _, test := range []struct {
		work *searchWork
		name string
		rule scriptedQuery
	}{
		{name: "query", rule: scriptedQuery{contains: "SELECT e FROM", err: failure}, work: &searchWork{limit: 10}},
		{name: "budget", rule: scriptedQuery{contains: "SELECT e FROM", columns: []string{"e"}, rows: [][]driver.Value{{int64(65)}}}, work: &searchWork{}},
		{name: "scan", rule: scriptedQuery{contains: "SELECT e FROM", columns: []string{"e"}, rows: [][]driver.Value{{}}}, work: &searchWork{limit: 10}},
		{name: "iteration", rule: scriptedQuery{contains: "SELECT e FROM", columns: []string{"e"}, nextErr: failure}, work: &searchWork{limit: 10}},
		{name: "close", rule: scriptedQuery{contains: "SELECT e FROM", columns: []string{"e"}, closeErr: failure}, work: &searchWork{limit: 10}},
	} {
		t.Run("eligible "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			db := &DB{store: &store{sql: runner, names: map[string]int64{}, queryBudget: 10}, exec: runner}
			if _, err := db.eligibleSearchEntities(ctx, runner, filter, test.work); err == nil {
				t.Fatal("eligible search fault unexpectedly succeeded")
			}
		})
	}

	attribute := scriptedQuery{
		contains: "SELECT f.a,i.name", columns: []string{"a", "name"},
		rows: [][]driver.Value{{int64(66), "item/value"}},
	}
	schema := scriptedQuery{contains: "SELECT f.id,f.e,f.a,f.v,f.t,f.tx,f.rx", columns: make([]string, 7)}
	for _, test := range []struct {
		name    string
		queries []scriptedQuery
	}{
		{name: "attribute query", queries: []scriptedQuery{{contains: "SELECT f.a,i.name", err: failure}}},
		{name: "attribute scan", queries: []scriptedQuery{{contains: "SELECT f.a,i.name", columns: []string{"a", "name"}, rows: [][]driver.Value{{int64(66)}}}}},
		{name: "attribute iteration", queries: []scriptedQuery{{contains: "SELECT f.a,i.name", columns: []string{"a", "name"}, nextErr: failure}}},
		{name: "attribute close", queries: []scriptedQuery{{contains: "SELECT f.a,i.name", columns: []string{"a", "name"}, closeErr: failure}}},
		{name: "schema", queries: []scriptedQuery{attribute, {contains: "SELECT f.id,f.e,f.a,f.v,f.t,f.tx,f.rx", err: failure}}},
		{name: "value query", queries: []scriptedQuery{attribute, schema, {contains: "SELECT f.v,f.t", err: failure}}},
		{name: "value scan", queries: []scriptedQuery{attribute, schema, {contains: "SELECT f.v,f.t", columns: []string{"v", "t"}, rows: [][]driver.Value{{int64(1)}}}}},
		{name: "value iteration", queries: []scriptedQuery{attribute, schema, {contains: "SELECT f.v,f.t", columns: []string{"v", "t"}, nextErr: failure}}},
		{name: "value close", queries: []scriptedQuery{attribute, schema, {contains: "SELECT f.v,f.t", columns: []string{"v", "t"}, closeErr: failure}}},
		{name: "logical", queries: []scriptedQuery{attribute, schema, {contains: "SELECT f.v,f.t", columns: []string{"v", "t"}, rows: [][]driver.Value{{"bad", int64(TagInt)}}}}},
	} {
		t.Run("compact "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: test.queries})
			db := &DB{store: &store{sql: runner, names: map[string]int64{}, queryBudget: 10}, exec: runner}
			if _, err := db.pullEntityCompact(ctx, runner, 65); !errors.Is(err, ErrFormat) {
				t.Fatalf("compact projection fault = %v", err)
			}
		})
	}

	storedVector, err := vectorStored([]float32{1})
	if err != nil {
		t.Fatal(err)
	}
	vectorColumns := make([]string, 8)
	validVector := []driver.Value{
		int64(1), int64(65), int64(66), storedVector.storage,
		int64(TagVector), int64(64), nil, storedVector.blob,
	}
	for _, test := range []struct {
		name      string
		attribute string
		work      *searchWork
		want      error
		queries   []scriptedQuery
	}{
		{
			name: "vector schema", attribute: "item/vector", work: &searchWork{limit: 10}, want: ErrFormat,
			queries: []scriptedQuery{{contains: "SELECT f.id,f.e,f.a,f.v,f.t,f.tx,f.rx", err: failure}},
		},
		{
			name: "vector scan", work: &searchWork{limit: 10}, want: ErrFormat,
			queries: []scriptedQuery{{contains: "SELECT f.id,f.e", columns: vectorColumns, rows: [][]driver.Value{{int64(1)}}}},
		},
		{
			name: "vector budget", work: &searchWork{}, want: ErrTooLarge,
			queries: []scriptedQuery{{contains: "SELECT f.id,f.e", columns: vectorColumns, rows: [][]driver.Value{validVector}}},
		},
		{
			name: "vector logical", work: &searchWork{limit: 10}, want: ErrFormat,
			queries: []scriptedQuery{{contains: "SELECT f.id,f.e", columns: vectorColumns, rows: [][]driver.Value{{int64(1), int64(65), int64(66), []byte{1}, int64(TagVector), int64(64), nil, []byte{1}}}}},
		},
		{
			name: "vector iteration", work: &searchWork{limit: 10}, want: ErrFormat,
			queries: []scriptedQuery{{contains: "SELECT f.id,f.e", columns: vectorColumns, nextErr: failure}},
		},
		{
			name: "vector render", work: &searchWork{limit: 10}, want: ErrFormat,
			queries: []scriptedQuery{{contains: "SELECT f.id,f.e", columns: vectorColumns, rows: [][]driver.Value{validVector}}},
		},
		{
			name: "vector provenance", work: &searchWork{limit: 10}, want: ErrFormat,
			queries: []scriptedQuery{
				{contains: "SELECT f.id,f.e", columns: vectorColumns, rows: [][]driver.Value{validVector}},
				{contains: "SELECT name FROM fgraph_ids", columns: []string{"name"}, rows: [][]driver.Value{{"item/vector"}}},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: test.queries})
			db := &DB{store: &store{sql: runner, names: map[string]int64{"item/vector": 66}, queryBudget: 10}, exec: runner}
			_, candidateErr := db.vectorCandidatesBounded(
				ctx, runner, []float32{1}, test.attribute, nil, searchCandidateLimit, test.work,
			)
			if !errors.Is(candidateErr, test.want) {
				t.Fatalf("vector candidate fault = %v, want %v", candidateErr, test.want)
			}
		})
	}

	edge := scriptedQuery{
		contains: "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts",
		columns:  make([]string, 7),
		rows: [][]driver.Value{{
			int64(1), int64(65), int64(70), int64(66), int64(TagRef), int64(64), nil,
		}},
	}
	for _, test := range []struct {
		work *searchWork
		want error
		name string
	}{
		{name: "expansion budget", work: &searchWork{}, want: ErrTooLarge},
		{name: "expansion render", work: &searchWork{limit: 10}, want: ErrFormat},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{edge}})
			db := &DB{store: &store{sql: runner, names: map[string]int64{}, queryBudget: 10}, exec: runner}
			_, _, expansionErr := db.expandSearch(ctx, runner, []int64{65}, 1, test.work)
			if !errors.Is(expansionErr, test.want) {
				t.Fatalf("expansion fault = %v, want %v", expansionErr, test.want)
			}
		})
	}
}

func TestMCPChangeFeedBoundsAndSQLFaults(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	fields := E{"id": "feed/large"}
	for index := 0; index < 25; index++ {
		fields["feed/value-"+string(rune('a'+index))] = int64(index)
	}
	large, transactErr := db.Transact(ctx, fields)
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	if _, err := db.Transact(ctx, []any{"retract", "feed/large"}); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < mcpResourcePage; index++ {
		if _, err := db.Transact(ctx, E{"id": "feed/identity-" + strings.Repeat("x", index+1)}); err != nil {
			t.Fatal(err)
		}
	}
	basis, err := db.latestTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	page, err := db.mcpChanges(ctx, basis, GenesisTx, "64", "fgraph://changes?since=64")
	if err != nil {
		t.Fatal(err)
	}
	events, ok := page["events"].([]map[string]any)
	if !ok || len(events) != mcpResourcePage {
		t.Fatalf("bounded change feed events = %#v", page)
	}
	asserted, assertedOK := events[0]["asserted"].([]any)
	if page["next_uri"] == nil || events[0]["event"] != large.EventID || events[0]["fgraph"] != "event/1" || !assertedOK || len(asserted) != 25 {
		t.Fatalf("bounded change feed = %#v", page)
	}

	failure := errors.New("change feed fault")
	for _, test := range []struct {
		name string
		rule scriptedQuery
	}{
		{name: "query", rule: scriptedQuery{contains: "SELECT tx", err: failure}},
		{name: "scan", rule: scriptedQuery{contains: "SELECT tx", columns: []string{"tx"}, rows: [][]driver.Value{{}}}},
		{name: "iteration", rule: scriptedQuery{contains: "SELECT tx", columns: []string{"tx"}, nextErr: failure}},
		{name: "close", rule: scriptedQuery{contains: "SELECT tx", columns: []string{"tx"}, closeErr: failure}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
			if _, err := db.mcpChanges(ctx, GenesisTx, GenesisTx, "64", "fgraph://changes"); !errors.Is(err, ErrFormat) {
				t.Fatalf("MCP change feed fault = %v", err)
			}
		})
	}
}
