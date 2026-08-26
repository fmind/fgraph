package fgraph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
)

func maintenanceCoverageDB(runner *sql.DB) *DB {
	return &DB{
		store: &store{
			sql:         runner,
			names:       map[string]int64{"shape/test": 65},
			gids:        map[int64]string{},
			dataVersion: -1,
		},
		exec: runner,
	}
}

func maintenanceCoverageEvent(t *testing.T, record map[string]any) (string, []byte) {
	t.Helper()
	encoded, err := canonicalJSON(record)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	return string(encoded), digest[:]
}

func maintenanceCoverageUUID(t *testing.T, value string) []byte {
	t.Helper()
	uuid, err := parseUUID(value)
	if err != nil {
		t.Fatal(err)
	}
	return uuid[:]
}

func maintenanceCoverageProblem(t *testing.T, problems []string, want string) {
	t.Helper()
	for _, problem := range problems {
		if strings.Contains(problem, want) {
			return
		}
	}
	t.Fatalf("problems %q do not contain %q", problems, want)
}

func TestMaintenanceCoverageEventInspectionFailures(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected event inspection failure")

	for _, test := range []struct {
		name string
		rule scriptedQuery
	}{
		{name: "query", rule: scriptedQuery{contains: "SELECT ev.tx,ev.event_hash", err: failure}},
		{name: "scan", rule: scriptedQuery{
			contains: "SELECT ev.tx,ev.event_hash", columns: []string{"tx", "hash", "data", "operation", "gid"},
			rows: [][]driver.Value{{"not-a-transaction", make([]byte, sha256.Size), nil, nil, nil}},
		}},
		{name: "iteration", rule: scriptedQuery{
			contains: "SELECT ev.tx,ev.event_hash", columns: []string{"tx", "hash", "data", "operation", "gid"}, nextErr: failure,
		}},
		{name: "close", rule: scriptedQuery{
			contains: "SELECT ev.tx,ev.event_hash", columns: []string{"tx", "hash", "data", "operation", "gid"}, closeErr: failure,
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			_, _, err := maintenanceCoverageDB(runner).inspectEventHashes(ctx, runner)
			requireErrorKind(t, err, ErrFormat)
		})
	}

	const at = int64(1_767_225_600_000_000)
	base := func(event string, timestamp int64) map[string]any {
		return map[string]any{
			"fgraph": "event/1", "event": event, "at": timestamp,
			"created": []any{}, "asserted": []any{}, "retracted": []any{},
		}
	}
	type semanticCase struct {
		name   string
		want   string
		record map[string]any
		row    []driver.Value
		extra  []scriptedQuery
	}
	validGID := maintenanceCoverageUUID(t, genesisEventID)
	otherEvent := "00000000-0000-4000-8000-000000000099"
	semantic := make([]semanticCase, 0, 9)
	semantic = append(semantic, []semanticCase{
		{
			name: "malformed identity", want: "no 16-byte UUID identity",
			row: []driver.Value{int64(GenesisTx), make([]byte, sha256.Size), nil, nil, []byte{1}},
		},
		{
			name: "invalid operation", want: "invalid operation id",
			row: []driver.Value{int64(GenesisTx), make([]byte, sha256.Size), nil, "", validGID},
		},
		{
			name: "invalid payload", want: "payload is invalid",
			row: []driver.Value{int64(GenesisTx), make([]byte, sha256.Size), "{}", nil, validGID},
		},
		{
			name: "payload identity", want: "names another event identity", record: base(otherEvent, at),
		},
		{
			name: "missing timestamp", want: "missing timestamp", record: base(genesisEventID, at),
			extra: []scriptedQuery{{contains: "a=1 AND tx=e", err: sql.ErrNoRows}},
		},
		{
			name: "origin read", want: "cannot read original timestamp", record: base(genesisEventID, at),
			extra: []scriptedQuery{
				{contains: "a=1 AND tx=e", columns: []string{"v"}, rows: [][]driver.Value{{at}}},
				{contains: "a=? AND tx=e", err: failure},
			},
		},
		{
			name: "timestamp mismatch", want: "timestamp differs", record: base(genesisEventID, at+1),
			extra: []scriptedQuery{
				{contains: "a=1 AND tx=e", columns: []string{"v"}, rows: [][]driver.Value{{at}}},
				{contains: "a=? AND tx=e", err: sql.ErrNoRows},
			},
		},
	}...)
	malformedRedaction := base(genesisEventID, at)
	malformedRedaction["redacted"] = true
	semantic = append(semantic, semanticCase{
		name: "malformed redaction", want: "malformed redacted excision", record: malformedRedaction,
		extra: []scriptedQuery{
			{contains: "a=1 AND tx=e", columns: []string{"v"}, rows: [][]driver.Value{{at}}},
			{contains: "a=? AND tx=e", err: sql.ErrNoRows},
		},
	})
	validRedaction := base(genesisEventID, at)
	validRedaction["redacted"] = true
	validRedaction["redacts"] = []any{}
	semantic = append(semantic, semanticCase{
		name: "missing redaction marker", want: "has no live fgraph/excised audit marker", record: validRedaction,
		extra: []scriptedQuery{
			{contains: "a=1 AND tx=e", columns: []string{"v"}, rows: [][]driver.Value{{at}}},
			{contains: "a=? AND tx=e", err: sql.ErrNoRows},
			{contains: "SELECT EXISTS", columns: []string{"exists"}, rows: [][]driver.Value{{int64(0)}}},
		},
	})

	for _, test := range semantic {
		t.Run(test.name, func(t *testing.T) {
			row := test.row
			if test.record != nil {
				encoded, digest := maintenanceCoverageEvent(t, test.record)
				row = []driver.Value{int64(GenesisTx), digest, encoded, nil, validGID}
			}
			queries := []scriptedQuery{{
				contains: "SELECT ev.tx,ev.event_hash", columns: []string{"tx", "hash", "data", "operation", "gid"}, rows: [][]driver.Value{row},
			}}
			queries = append(queries, test.extra...)
			runner := openScriptedSQL(t, scriptedSQL{queries: queries})
			_, problems, err := maintenanceCoverageDB(runner).inspectEventHashes(ctx, runner)
			if err != nil {
				t.Fatal(err)
			}
			maintenanceCoverageProblem(t, problems, test.want)
		})
	}
}

func TestMaintenanceCoverageEventPayloadFailures(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected retained payload failure")
	if _, err := maintenanceCoverageDB(nil).eventPayloadTransactionsForSelector(ctx, nil, make(chan int), 100); !errors.Is(err, ErrFormat) {
		t.Fatalf("uncanonicalizable selector error = %v", err)
	}

	malformed, malformedHash := maintenanceCoverageEvent(t, map[string]any{
		"fgraph": "event/1", "created": true,
	})
	for _, test := range []struct {
		name string
		rule scriptedQuery
	}{
		{name: "query", rule: scriptedQuery{contains: "SELECT tx,event_hash,event_data", err: failure}},
		{name: "scan", rule: scriptedQuery{
			contains: "SELECT tx,event_hash,event_data", columns: []string{"tx", "hash", "data"}, rows: [][]driver.Value{{int64(65), make([]byte, sha256.Size)}},
		}},
		{name: "invalid payload", rule: scriptedQuery{
			contains: "SELECT tx,event_hash,event_data", columns: []string{"tx", "hash", "data"}, rows: [][]driver.Value{{int64(65), make([]byte, sha256.Size), "{}"}},
		}},
		{name: "invalid event shape", rule: scriptedQuery{
			contains: "SELECT tx,event_hash,event_data", columns: []string{"tx", "hash", "data"}, rows: [][]driver.Value{{int64(65), malformedHash, malformed}},
		}},
		{name: "iteration", rule: scriptedQuery{
			contains: "SELECT tx,event_hash,event_data", columns: []string{"tx", "hash", "data"}, nextErr: failure,
		}},
		{name: "close", rule: scriptedQuery{
			contains: "SELECT tx,event_hash,event_data", columns: []string{"tx", "hash", "data"}, closeErr: failure,
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			_, err := maintenanceCoverageDB(runner).eventPayloadTransactionsForSelector(ctx, runner, "item/entity", 100)
			requireErrorKind(t, err, ErrFormat)
		})
	}
}

func TestSchemaCoverageScriptedReadFailures(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected schema read failure")
	threeColumns := []string{"a", "v", "t"}
	twoColumns := []string{"a", "count"}

	tests := []struct {
		run    func(*DB, *sql.DB) error
		name   string
		script scriptedSQL
	}{
		{name: "declaration query", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.a,f.v,f.t", err: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.declaredAttribute(ctx, runner, 65, "schema/value")
			return err
		}},
		{name: "declaration scan", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.a,f.v,f.t", columns: threeColumns, rows: [][]driver.Value{{int64(8), "text"}}}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.declaredAttribute(ctx, runner, 65, "schema/value")
			return err
		}},
		{name: "declaration logical value", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.a,f.v,f.t", columns: threeColumns, rows: [][]driver.Value{{int64(8), "not-bytes", int64(TagBytes)}}}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.declaredAttribute(ctx, runner, 65, "schema/value")
			return err
		}},
		{name: "declaration semantic type", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.a,f.v,f.t", columns: threeColumns, rows: [][]driver.Value{{int64(5), int64(1), int64(TagInt)}}}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.declaredAttribute(ctx, runner, 65, "schema/value")
			return err
		}},
		{name: "declaration iteration", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.a,f.v,f.t", columns: threeColumns, nextErr: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.declaredAttribute(ctx, runner, 65, "schema/value")
			return err
		}},
		{name: "declaration close", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.a,f.v,f.t", columns: threeColumns, closeErr: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.declaredAttribute(ctx, runner, 65, "schema/value")
			return err
		}},
		{name: "observation query", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.t,COUNT(*)", err: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.schemaObservation(ctx, runner, 65, "schema/value")
			return err
		}},
		{name: "observation scan", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.t,COUNT(*)", columns: twoColumns, rows: [][]driver.Value{{int64(TagText)}}}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.schemaObservation(ctx, runner, 65, "schema/value")
			return err
		}},
		{name: "observation iteration", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.t,COUNT(*)", columns: twoColumns, nextErr: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.schemaObservation(ctx, runner, 65, "schema/value")
			return err
		}},
		{name: "observation close", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.t,COUNT(*)", columns: twoColumns, closeErr: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.schemaObservation(ctx, runner, 65, "schema/value")
			return err
		}},
		{name: "observation count", script: scriptedSQL{queries: []scriptedQuery{
			{contains: "SELECT f.t,COUNT(*)", columns: twoColumns}, {contains: "SELECT COUNT(DISTINCT", err: failure},
		}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.schemaObservation(ctx, runner, 65, "schema/value")
			return err
		}},
		{name: "identities query", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT id,name FROM fgraph_ids", err: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.schemaIdentities(ctx, runner, 65, "", false)
			return err
		}},
		{name: "identities scan", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT id,name FROM fgraph_ids", columns: []string{"id", "name"}, rows: [][]driver.Value{{int64(65)}}}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.schemaIdentities(ctx, runner, 65, "", false)
			return err
		}},
		{name: "identities iteration", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT id,name FROM fgraph_ids", columns: []string{"id", "name"}, nextErr: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.schemaIdentities(ctx, runner, 65, "", false)
			return err
		}},
		{name: "identities close", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT id,name FROM fgraph_ids", columns: []string{"id", "name"}, closeErr: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.schemaIdentities(ctx, runner, 65, "", false)
			return err
		}},
		{name: "shape list query", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT DISTINCT f.e", err: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.readShapes(ctx, runner)
			return err
		}},
		{name: "shape list scan", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT DISTINCT f.e", columns: []string{"e"}, rows: [][]driver.Value{{}}}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.readShapes(ctx, runner)
			return err
		}},
		{name: "shape list iteration", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT DISTINCT f.e", columns: []string{"e"}, nextErr: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.readShapes(ctx, runner)
			return err
		}},
		{name: "shape list close", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT DISTINCT f.e", columns: []string{"e"}, closeErr: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.readShapes(ctx, runner)
			return err
		}},
		{name: "shape query", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.a,f.v,f.t", err: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.readShape(ctx, runner, 65)
			return err
		}},
		{name: "shape scan", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.a,f.v,f.t", columns: threeColumns, rows: [][]driver.Value{{int64(16), int64(66)}}}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.readShape(ctx, runner, 65)
			return err
		}},
		{name: "shape iteration", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.a,f.v,f.t", columns: threeColumns, nextErr: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.readShape(ctx, runner, 65)
			return err
		}},
		{name: "shape close", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.a,f.v,f.t", columns: threeColumns, closeErr: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.readShape(ctx, runner, 65)
			return err
		}},
		{name: "shape unnamed reference", script: scriptedSQL{queries: []scriptedQuery{
			{contains: "SELECT f.a,f.v,f.t", columns: threeColumns, rows: [][]driver.Value{{int64(16), int64(66), int64(TagRef)}}},
			{contains: "SELECT name FROM fgraph_ids", columns: []string{"name"}, rows: [][]driver.Value{{nil}}},
		}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.readShape(ctx, runner, 65)
			return err
		}},
		{name: "membership query", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.e,f.v", err: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.shapeIssues(ctx, runner, nil)
			return err
		}},
		{name: "membership scan", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.e,f.v", columns: []string{"e", "v"}, rows: [][]driver.Value{{int64(65)}}}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.shapeIssues(ctx, runner, nil)
			return err
		}},
		{name: "membership iteration", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.e,f.v", columns: []string{"e", "v"}, nextErr: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.shapeIssues(ctx, runner, nil)
			return err
		}},
		{name: "membership close", script: scriptedSQL{queries: []scriptedQuery{{contains: "SELECT f.e,f.v", columns: []string{"e", "v"}, closeErr: failure}}}, run: func(db *DB, runner *sql.DB) error {
			_, err := db.shapeIssues(ctx, runner, nil)
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, test.script)
			err := test.run(maintenanceCoverageDB(runner), runner)
			requireErrorKind(t, err, ErrFormat)
		})
	}

	for _, test := range []struct {
		name string
		rule scriptedQuery
	}{
		{name: "query", rule: scriptedQuery{contains: "SELECT e FROM fgraph_facts WHERE a=15", err: failure}},
		{name: "scan", rule: scriptedQuery{contains: "SELECT e FROM fgraph_facts WHERE a=15", columns: []string{"e"}, rows: [][]driver.Value{{}}}},
		{name: "iteration", rule: scriptedQuery{contains: "SELECT e FROM fgraph_facts WHERE a=15", columns: []string{"e"}, nextErr: failure}},
		{name: "close", rule: scriptedQuery{contains: "SELECT e FROM fgraph_facts WHERE a=15", columns: []string{"e"}, closeErr: failure}},
	} {
		t.Run("changed shape members "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			plan := &transactionPlan{assertions: []plannedFact{{e: 65, a: 16}}}
			err := maintenanceCoverageDB(runner).validateTouchedShapes(ctx, runner, plan)
			requireErrorKind(t, err, ErrFormat)
		})
	}
}

func TestSnapshotCoverageReceiptAndRegistryFailures(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected snapshot failure")
	columns := []string{"tx", "event_hash", "event_data", "operation_id", "request_hash", "gid"}
	validRow := []driver.Value{
		int64(65), make([]byte, sha256.Size), nil, nil, nil,
		maintenanceCoverageUUID(t, "00000000-0000-4000-8000-000000000065"),
	}
	validQueries := func(main scriptedQuery) []scriptedQuery {
		return []scriptedQuery{
			main,
			{contains: "a=1 AND tx=?", columns: []string{"v"}, rows: [][]driver.Value{{int64(1_767_225_600_000_000)}}},
			{contains: "a=? AND tx=?", err: sql.ErrNoRows},
			{contains: "SELECT id FROM fgraph_ids WHERE created_tx", columns: []string{"id"}},
		}
	}
	tests := []struct {
		write   func(any) error
		name    string
		queries []scriptedQuery
	}{
		{name: "query", queries: []scriptedQuery{{contains: "SELECT ev.tx,ev.event_hash", err: failure}}},
		{name: "scan", queries: []scriptedQuery{{contains: "SELECT ev.tx,ev.event_hash", columns: columns, rows: [][]driver.Value{{int64(65)}}}}},
		{name: "malformed receipt", queries: []scriptedQuery{{contains: "SELECT ev.tx,ev.event_hash", columns: columns, rows: [][]driver.Value{{int64(65), []byte{1}, nil, nil, nil, []byte{1}}}}}},
		{name: "timestamp", queries: []scriptedQuery{
			{contains: "SELECT ev.tx,ev.event_hash", columns: columns, rows: [][]driver.Value{validRow}},
			{contains: "a=1 AND tx=?", err: failure},
		}},
		{name: "origin", queries: []scriptedQuery{
			{contains: "SELECT ev.tx,ev.event_hash", columns: columns, rows: [][]driver.Value{validRow}},
			{contains: "a=1 AND tx=?", columns: []string{"v"}, rows: [][]driver.Value{{int64(1_767_225_600_000_000)}}},
			{contains: "a=? AND tx=?", err: failure},
		}},
		{name: "created identities", queries: []scriptedQuery{
			{contains: "SELECT ev.tx,ev.event_hash", columns: columns, rows: [][]driver.Value{validRow}},
			{contains: "a=1 AND tx=?", columns: []string{"v"}, rows: [][]driver.Value{{int64(1_767_225_600_000_000)}}},
			{contains: "a=? AND tx=?", err: sql.ErrNoRows},
			{contains: "SELECT id FROM fgraph_ids WHERE created_tx", err: failure},
		}},
		{name: "event data", queries: validQueries(scriptedQuery{
			contains: "SELECT ev.tx,ev.event_hash", columns: columns,
			rows: [][]driver.Value{{int64(65), make([]byte, sha256.Size), "{}", nil, nil, validRow[5]}},
		})},
		{name: "writer", queries: validQueries(scriptedQuery{contains: "SELECT ev.tx,ev.event_hash", columns: columns, rows: [][]driver.Value{validRow}}), write: func(any) error { return failure }},
		{name: "iteration", queries: []scriptedQuery{{contains: "SELECT ev.tx,ev.event_hash", columns: columns, nextErr: failure}}},
		{name: "close", queries: []scriptedQuery{{contains: "SELECT ev.tx,ev.event_hash", columns: columns, closeErr: failure}}},
	}
	for _, test := range tests {
		t.Run("receipt "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: test.queries})
			write := test.write
			if write == nil {
				write = func(any) error { return nil }
			}
			_, err := maintenanceCoverageDB(runner).snapshotReceipts(ctx, runner, 65, write)
			if test.name == "writer" {
				if !errors.Is(err, failure) {
					t.Fatalf("writer error = %v", err)
				}
				return
			}
			requireErrorKind(t, err, ErrFormat)
		})
	}

	for _, test := range []struct {
		name string
		rule scriptedQuery
	}{
		{name: "query", rule: scriptedQuery{contains: "SELECT id,name,gid", err: failure}},
		{name: "scan", rule: scriptedQuery{contains: "SELECT id,name,gid", columns: []string{"id", "name", "gid"}, rows: [][]driver.Value{{"not-an-id", "name", nil}}}},
		{name: "iteration", rule: scriptedQuery{contains: "SELECT id,name,gid", columns: []string{"id", "name", "gid"}, nextErr: failure}},
	} {
		t.Run("registry "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			_, err := maintenanceCoverageDB(runner).newSnapshotRestoreState(ctx, runner)
			requireErrorKind(t, err, ErrFormat)
		})
	}

	state := &snapshotRestoreState{receipts: map[int64]snapshotReceipt{
		65: {event: "00000000-0000-4000-8000-000000000065", at: 10, originAt: 9},
	}}
	for _, test := range []struct {
		kind    error
		name    string
		queries []scriptedQuery
	}{
		{name: "timestamp", kind: ErrConflict, queries: []scriptedQuery{{contains: "a=1 AND tx=?", err: sql.ErrNoRows}}},
		{name: "origin query", kind: ErrFormat, queries: []scriptedQuery{
			{contains: "a=1 AND tx=?", columns: []string{"v"}, rows: [][]driver.Value{{int64(10)}}},
			{contains: "a=? AND tx=?", err: failure},
		}},
		{name: "origin mismatch", kind: ErrConflict, queries: []scriptedQuery{
			{contains: "a=1 AND tx=?", columns: []string{"v"}, rows: [][]driver.Value{{int64(10)}}},
			{contains: "a=? AND tx=?", columns: []string{"v"}, rows: [][]driver.Value{{int64(8)}}},
		}},
	} {
		t.Run("receipt time "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: test.queries})
			err := maintenanceCoverageDB(runner).validateRestoredReceiptTimes(ctx, runner, state)
			requireErrorKind(t, err, test.kind)
		})
	}
}

func TestSnapshotCoverageAnonymousHistoryAndSemanticRollback(t *testing.T) {
	ctx := context.Background()
	source := fixedDB(t, ":memory:")
	historicalText := strings.Repeat("historical-portable-token ", 400)
	created, createErr := source.Transact(ctx, E{
		"portable/history": historicalText,
		"portable/live":    "current-portable-token",
	})
	if createErr != nil {
		t.Fatal(createErr)
	}
	historicalFacts := factsForAttribute(created.Asserted, "portable/history")
	if len(historicalFacts) != 1 {
		t.Fatalf("historical assertion = %#v", created.Asserted)
	}
	entity := historicalFacts[0].E
	selector, selectorErr := source.identitySelector(ctx, source.store.sql, asInt64(entity))
	if selectorErr != nil {
		t.Fatal(selectorErr)
	}
	if _, err := source.Retract(ctx, entity, "portable/history"); err != nil {
		t.Fatal(err)
	}

	var snapshot bytes.Buffer
	if err := source.Snapshot(ctx, &snapshot); err != nil {
		t.Fatal(err)
	}
	target := fixedDB(t, ":memory:")
	if err := target.Restore(ctx, bytes.NewReader(snapshot.Bytes())); err != nil {
		t.Fatal(err)
	}
	entityValue, entityErr := target.Entity(ctx, selector)
	if entityErr != nil || entityValue["portable/live"] != "current-portable-token" {
		t.Fatalf("restored anonymous entity = %#v, %v", entityValue, entityErr)
	}
	if _, exists := entityValue["portable/history"]; exists {
		t.Fatalf("retracted history became live: %#v", entityValue)
	}
	var historicalFTS, liveFTS int64
	if err := target.store.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM fgraph_fts x
		JOIN fgraph_facts f ON f.id=x.rowid WHERE f.rx IS NOT NULL`).Scan(&historicalFTS); err != nil {
		t.Fatal(err)
	}
	if err := target.store.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM fgraph_fts x
		JOIN fgraph_facts f ON f.id=x.rowid WHERE f.rx IS NULL`).Scan(&liveFTS); err != nil {
		t.Fatal(err)
	}
	if historicalFTS != 0 || liveFTS == 0 {
		t.Fatalf("restored FTS rows: historical=%d live=%d", historicalFTS, liveFTS)
	}
	var roundTrip bytes.Buffer
	if err := target.Snapshot(ctx, &roundTrip); err != nil || !bytes.Equal(snapshot.Bytes(), roundTrip.Bytes()) {
		t.Fatalf("anonymous historical snapshot round trip equal=%t err=%v", bytes.Equal(snapshot.Bytes(), roundTrip.Bytes()), err)
	}

	rollback := fixedDB(t, ":memory:")
	var beforeCreatedAt int64
	var beforeGenesisData string
	if err := rollback.store.sql.QueryRowContext(ctx, "SELECT value FROM fgraph_meta WHERE key='created_at'").Scan(&beforeCreatedAt); err != nil {
		t.Fatal(err)
	}
	if err := rollback.store.sql.QueryRowContext(ctx, "SELECT event_data FROM fgraph_events WHERE tx=?", GenesisTx).Scan(&beforeGenesisData); err != nil {
		t.Fatal(err)
	}
	header, headerErr := canonicalJSON(map[string]any{
		"fgraph": "snapshot/1", "format": int64(FormatVersion),
		"created_at": beforeCreatedAt + 1, "basis": "00000000-0000-4000-8000-000000000099",
	})
	if headerErr != nil {
		t.Fatal(headerErr)
	}
	digest := sha256.Sum256(append(append([]byte(nil), header...), '\n'))
	footer, footerErr := canonicalJSON(map[string]any{
		"fgraph": "end", "sha256": hex.EncodeToString(digest[:]), "receipts": int64(0), "facts": int64(0),
	})
	if footerErr != nil {
		t.Fatal(footerErr)
	}
	stream := append(append(append([]byte(nil), header...), '\n'), footer...)
	stream = append(stream, '\n')
	if err := rollback.Restore(ctx, bytes.NewReader(stream)); !errors.Is(err, ErrConflict) {
		t.Fatalf("inconsistent basis restore error = %v", err)
	}
	var afterCreatedAt int64
	var afterGenesisData string
	if err := rollback.store.sql.QueryRowContext(ctx, "SELECT value FROM fgraph_meta WHERE key='created_at'").Scan(&afterCreatedAt); err != nil {
		t.Fatal(err)
	}
	if err := rollback.store.sql.QueryRowContext(ctx, "SELECT event_data FROM fgraph_events WHERE tx=?", GenesisTx).Scan(&afterGenesisData); err != nil {
		t.Fatal(err)
	}
	if afterCreatedAt != beforeCreatedAt || afterGenesisData != beforeGenesisData {
		t.Fatalf("failed restore mutated genesis: created_at %d -> %d", beforeCreatedAt, afterCreatedAt)
	}
}

func TestSnapshotCoverageHeaderValidationAndExecutionFailure(t *testing.T) {
	ctx := context.Background()
	valid := func() map[string]any {
		return map[string]any{
			"fgraph": "snapshot/1", "format": int64(FormatVersion),
			"created_at": int64(1_767_225_600_000_000), "basis": genesisEventID,
		}
	}
	for name, mutate := range map[string]func(map[string]any){
		"keys":          func(header map[string]any) { header["extra"] = true },
		"kind":          func(header map[string]any) { header["fgraph"] = "event/1" },
		"format":        func(header map[string]any) { header["format"] = int64(FormatVersion - 1) },
		"created type":  func(header map[string]any) { header["created_at"] = "now" },
		"created range": func(header map[string]any) { header["created_at"] = maxInstantMicros + 1 },
		"basis type":    func(header map[string]any) { header["basis"] = true },
		"basis UUID":    func(header map[string]any) { header["basis"] = "bad" },
	} {
		t.Run(name, func(t *testing.T) {
			header := valid()
			mutate(header)
			db := fixedDB(t, ":memory:")
			if _, err := db.restoreSnapshotHeader(ctx, db.store.sql, header); err == nil {
				t.Fatalf("invalid header unexpectedly restored: %#v", header)
			}
		})
	}

	failure := errors.New("injected genesis update failure")
	runner := openScriptedSQL(t, scriptedSQL{execs: []scriptedExec{{contains: "UPDATE fgraph_facts SET v", err: failure}}})
	if _, err := maintenanceCoverageDB(runner).restoreSnapshotHeader(ctx, runner, valid()); !errors.Is(err, ErrFormat) {
		t.Fatalf("genesis update error = %v", err)
	}
}

func TestMaintenanceCoverageInvariantCounters(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected invariant counter failure")
	for _, test := range []struct {
		run  func(*DB, *sql.DB) error
		name string
		rule scriptedQuery
	}{
		{name: "schema query", rule: scriptedQuery{contains: "SELECT DISTINCT a", err: failure}, run: func(db *DB, runner *sql.DB) error { _, err := db.countGlobalSchemaProblems(ctx, runner); return err }},
		{name: "schema scan", rule: scriptedQuery{contains: "SELECT DISTINCT a", columns: []string{"a"}, rows: [][]driver.Value{{}}}, run: func(db *DB, runner *sql.DB) error { _, err := db.countGlobalSchemaProblems(ctx, runner); return err }},
		{name: "schema iteration", rule: scriptedQuery{contains: "SELECT DISTINCT a", columns: []string{"a"}, nextErr: failure}, run: func(db *DB, runner *sql.DB) error { _, err := db.countGlobalSchemaProblems(ctx, runner); return err }},
		{name: "schema close", rule: scriptedQuery{contains: "SELECT DISTINCT a", columns: []string{"a"}, closeErr: failure}, run: func(db *DB, runner *sql.DB) error { _, err := db.countGlobalSchemaProblems(ctx, runner); return err }},
		{name: "shape query", rule: scriptedQuery{contains: "SELECT DISTINCT e", err: failure}, run: func(db *DB, runner *sql.DB) error { _, err := db.countGlobalShapeViolations(ctx, runner); return err }},
		{name: "shape scan", rule: scriptedQuery{contains: "SELECT DISTINCT e", columns: []string{"e"}, rows: [][]driver.Value{{}}}, run: func(db *DB, runner *sql.DB) error { _, err := db.countGlobalShapeViolations(ctx, runner); return err }},
		{name: "shape iteration", rule: scriptedQuery{contains: "SELECT DISTINCT e", columns: []string{"e"}, nextErr: failure}, run: func(db *DB, runner *sql.DB) error { _, err := db.countGlobalShapeViolations(ctx, runner); return err }},
		{name: "shape close", rule: scriptedQuery{contains: "SELECT DISTINCT e", columns: []string{"e"}, closeErr: failure}, run: func(db *DB, runner *sql.DB) error { _, err := db.countGlobalShapeViolations(ctx, runner); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			err := test.run(maintenanceCoverageDB(runner), runner)
			requireErrorKind(t, err, ErrFormat)
		})
	}

	t.Run("schema semantics", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		if _, err := db.Declare(ctx, "broken/value", Type("int"), Unique()); err != nil {
			t.Fatal(err)
		}
		first, transactErr := db.Transact(ctx, E{"id": "broken/one", "broken/value": 1})
		if transactErr != nil {
			t.Fatal(transactErr)
		}
		if _, err := db.Transact(ctx, E{"id": "broken/two", "broken/value": 2}); err != nil {
			t.Fatal(err)
		}
		var attribute, one, two int64
		for name, destination := range map[string]*int64{
			"broken/value": &attribute, "broken/one": &one, "broken/two": &two,
		} {
			if err := db.store.sql.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE name=?", name).Scan(destination); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_facts SET v=1 WHERE e=? AND a=?", two, attribute); err != nil {
			t.Fatal(err)
		}
		if _, err := db.store.sql.ExecContext(ctx, `INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES
			(?,?,?,?,?,NULL),(?,?,?,?,?,NULL),(?,?,?,?,?,NULL)`,
			one, attribute, int64(3), TagInt, first.Tx,
			attribute, int64(9), int64(2), TagInt, first.Tx,
			attribute, int64(14), "embedding/model", TagText, first.Tx,
		); err != nil {
			t.Fatal(err)
		}
		problems, countErr := db.countGlobalSchemaProblems(ctx, db.store.sql)
		if countErr != nil || problems < 4 {
			t.Fatalf("global schema problems = %d, %v", problems, countErr)
		}
	})

	t.Run("malformed shape", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		report, transactErr := db.Transact(ctx, []any{E{"id": "shape/counter-bad"}, E{"id": "shape/counter-member"}})
		if transactErr != nil {
			t.Fatal(transactErr)
		}
		var shapeID, memberID int64
		if err := db.store.sql.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE name='shape/counter-bad'").Scan(&shapeID); err != nil {
			t.Fatal(err)
		}
		if err := db.store.sql.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE name='shape/counter-member'").Scan(&memberID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.store.sql.ExecContext(ctx, `INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES
			(?,16,'not-a-ref',3,?,NULL),(?,15,?,0,?,NULL)`, shapeID, report.Tx, memberID, shapeID, report.Tx); err != nil {
			t.Fatal(err)
		}
		violations, countErr := db.countGlobalShapeViolations(ctx, db.store.sql)
		if countErr != nil || violations != 1 {
			t.Fatalf("global shape violations = %d, %v", violations, countErr)
		}
	})
}

func TestSnapshotCoverageFactExportFailures(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected snapshot fact failure")
	columns := []string{"id", "e", "a", "v", "t", "tx", "rx"}
	validFact := []driver.Value{int64(1), int64(65), int64(65), int64(7), int64(TagInt), int64(66), nil}
	validRules := func(main scriptedQuery) []scriptedQuery {
		return []scriptedQuery{
			main,
			{contains: "SELECT name FROM fgraph_ids WHERE id=?", columns: []string{"name"}, rows: [][]driver.Value{{"item/value"}}},
			{contains: "SELECT name,gid FROM fgraph_ids WHERE id=?", columns: []string{"name", "gid"}, rows: [][]driver.Value{{"item/entity", nil}}},
			{contains: "SELECT gid FROM fgraph_ids WHERE id=?", columns: []string{"gid"}, rows: [][]driver.Value{{maintenanceCoverageUUID(t, "00000000-0000-4000-8000-000000000066")}}},
		}
	}
	for _, test := range []struct {
		kind    error
		write   func(any) error
		name    string
		queries []scriptedQuery
	}{
		{name: "query", kind: ErrFormat, queries: []scriptedQuery{{contains: "SELECT id,e,a,v,t,tx,rx", err: failure}}},
		{name: "scan", kind: ErrFormat, queries: []scriptedQuery{{contains: "SELECT id,e,a,v,t,tx,rx", columns: columns, rows: [][]driver.Value{{"not-an-id", int64(65), int64(65), int64(7), int64(TagInt), int64(66), nil}}}}},
		{name: "iteration", kind: ErrFormat, queries: []scriptedQuery{{contains: "SELECT id,e,a,v,t,tx,rx", columns: columns, nextErr: failure}}},
		{name: "logical value", kind: ErrFormat, queries: []scriptedQuery{{contains: "SELECT id,e,a,v,t,tx,rx", columns: columns, rows: [][]driver.Value{{int64(1), int64(65), int64(65), "not-an-int", int64(TagInt), int64(66), nil}}}}},
		{name: "attribute selector", kind: ErrFormat, queries: []scriptedQuery{
			{contains: "SELECT id,e,a,v,t,tx,rx", columns: columns, rows: [][]driver.Value{validFact}},
			{contains: "SELECT name FROM fgraph_ids WHERE id=?", err: failure},
		}},
		{name: "entity selector", kind: ErrFormat, queries: []scriptedQuery{
			{contains: "SELECT id,e,a,v,t,tx,rx", columns: columns, rows: [][]driver.Value{validFact}},
			{contains: "SELECT name FROM fgraph_ids WHERE id=?", columns: []string{"name"}, rows: [][]driver.Value{{"item/value"}}},
			{contains: "SELECT name,gid FROM fgraph_ids WHERE id=?", err: failure},
		}},
		{name: "assertion event", kind: ErrFormat, queries: []scriptedQuery{
			{contains: "SELECT id,e,a,v,t,tx,rx", columns: columns, rows: [][]driver.Value{validFact}},
			{contains: "SELECT name FROM fgraph_ids WHERE id=?", columns: []string{"name"}, rows: [][]driver.Value{{"item/value"}}},
			{contains: "SELECT name,gid FROM fgraph_ids WHERE id=?", columns: []string{"name", "gid"}, rows: [][]driver.Value{{"item/entity", nil}}},
			{contains: "SELECT gid FROM fgraph_ids WHERE id=?", err: failure},
		}},
		{name: "writer", kind: failure, queries: validRules(scriptedQuery{contains: "SELECT id,e,a,v,t,tx,rx", columns: columns, rows: [][]driver.Value{validFact}}), write: func(any) error { return failure }},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: test.queries})
			write := test.write
			if write == nil {
				write = func(any) error { return nil }
			}
			_, err := maintenanceCoverageDB(runner).snapshotFacts(ctx, runner, 66, write)
			if errors.Is(test.kind, failure) {
				if !errors.Is(err, failure) {
					t.Fatalf("writer error = %v", err)
				}
				return
			}
			requireErrorKind(t, err, test.kind)
		})
	}

	t.Run("retraction event identity", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		assertion, assertionErr := db.Transact(ctx, E{"id": "snapshot/retracted", "snapshot/value": 1})
		if assertionErr != nil {
			t.Fatal(assertionErr)
		}
		retraction, retractionErr := db.Retract(ctx, "snapshot/retracted", "snapshot/value")
		if retractionErr != nil {
			t.Fatal(retractionErr)
		}
		if _, err := db.store.sql.ExecContext(ctx, "PRAGMA ignore_check_constraints=ON"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_ids SET gid=? WHERE id=?", []byte{1}, retraction.Tx); err != nil {
			t.Fatal(err)
		}
		_, snapshotErr := db.snapshotFacts(ctx, db.store.sql, retraction.Tx, func(any) error { return nil })
		if !errors.Is(snapshotErr, ErrFormat) {
			t.Fatalf("malformed retraction event error = %v (assertion tx %d)", snapshotErr, assertion.Tx)
		}
	})

	for _, test := range []struct {
		name    string
		queries []scriptedQuery
	}{
		{name: "query", queries: []scriptedQuery{{contains: "SELECT id FROM fgraph_ids WHERE created_tx", err: failure}}},
		{name: "scan", queries: []scriptedQuery{{
			contains: "SELECT id FROM fgraph_ids WHERE created_tx", columns: []string{"id"}, rows: [][]driver.Value{{"not-an-id"}},
		}}},
		{name: "selector", queries: []scriptedQuery{
			{contains: "SELECT id FROM fgraph_ids WHERE created_tx", columns: []string{"id"}, rows: [][]driver.Value{{int64(65)}}},
			{contains: "SELECT name,gid FROM fgraph_ids WHERE id=?", err: failure},
		}},
		{name: "iteration", queries: []scriptedQuery{{contains: "SELECT id FROM fgraph_ids WHERE created_tx", columns: []string{"id"}, nextErr: failure}}},
	} {
		t.Run("created selectors "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: test.queries})
			_, err := maintenanceCoverageDB(runner).createdSelectors(ctx, runner, 66)
			requireErrorKind(t, err, ErrFormat)
		})
	}
}

func TestSnapshotCoverageHighLevelPropagation(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected snapshot API failure")
	basis := scriptedQuery{contains: "SELECT COALESCE(MAX(tx),0)", columns: []string{"basis"}, rows: [][]driver.Value{{int64(GenesisTx)}}}
	meta := scriptedQuery{contains: "key='created_at'", columns: []string{"created_at"}, rows: [][]driver.Value{{int64(1_767_225_600_000_000)}}}
	gid := scriptedQuery{contains: "SELECT gid FROM fgraph_ids", columns: []string{"gid"}, rows: [][]driver.Value{{maintenanceCoverageUUID(t, genesisEventID)}}}
	receipts := scriptedQuery{contains: "SELECT ev.tx,ev.event_hash", columns: []string{"tx", "hash", "data", "operation", "request", "gid"}}
	for _, test := range []struct {
		name    string
		queries []scriptedQuery
	}{
		{name: "basis", queries: []scriptedQuery{{contains: "SELECT COALESCE(MAX(tx),0)", err: failure}}},
		{name: "metadata", queries: []scriptedQuery{basis, {contains: "key='created_at'", err: failure}}},
		{name: "basis identity", queries: []scriptedQuery{basis, meta, {contains: "SELECT gid FROM fgraph_ids", err: failure}}},
		{name: "receipts", queries: []scriptedQuery{basis, meta, gid, {contains: "SELECT ev.tx,ev.event_hash", err: failure}}},
		{name: "facts", queries: []scriptedQuery{basis, meta, gid, receipts, {contains: "SELECT id,e,a,v,t,tx,rx", err: failure}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: test.queries})
			if err := maintenanceCoverageDB(runner).Snapshot(ctx, &bytes.Buffer{}); !errors.Is(err, ErrFormat) {
				t.Fatalf("Snapshot error = %v", err)
			}
		})
	}

	source := fixedDB(t, ":memory:")
	first, err := source.Transact(ctx, E{"id": "snapshot/first", "snapshot/value": 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := source.Transact(ctx, E{"id": "snapshot/second", "snapshot/value": 2})
	if err != nil {
		t.Fatal(err)
	}
	view := source.atTx(first.Tx)
	var historical bytes.Buffer
	if err := view.Snapshot(ctx, &historical); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(historical.String(), first.EventID) || strings.Contains(historical.String(), second.EventID) {
		t.Fatalf("historical snapshot crossed its basis: %s", historical.String())
	}
	if err := source.Snapshot(ctx, &coverageWriter{failAt: 2}); !errors.Is(err, ErrFormat) {
		t.Fatalf("receipt write failure = %v", err)
	}
	if err := source.Snapshot(ctx, &coverageWriter{failAt: 4}); !errors.Is(err, ErrFormat) {
		t.Fatalf("fact write failure = %v", err)
	}
}

func TestSnapshotCoverageRestoreOperationFailures(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected restore operation failure")
	baseState := func() *snapshotRestoreState {
		return &snapshotRestoreState{
			identities: map[string]int64{"name:item/entity": 65, "name:item/value": 66},
			events: map[string]int64{
				genesisEventID:                         GenesisTx,
				"00000000-0000-4000-8000-000000000067": 67,
				"00000000-0000-4000-8000-000000000068": 68,
			},
			receipts: map[int64]snapshotReceipt{}, next: 69,
		}
	}
	fact := func(value any, tag string, retract any) map[string]any {
		return map[string]any{"fact": []any{
			"item/entity", "item/value", value, tag,
			"00000000-0000-4000-8000-000000000067", retract,
		}}
	}
	for _, test := range []struct {
		kind    error
		wrapper map[string]any
		name    string
		execs   []scriptedExec
	}{
		{name: "fact insert", wrapper: fact(int64(1), "int", nil), execs: []scriptedExec{{contains: "INSERT INTO fgraph_facts", err: failure}}, kind: ErrConflict},
		{name: "retraction type", wrapper: fact(int64(1), "int", true), kind: ErrType},
		{name: "retraction order", wrapper: fact(int64(1), "int", "00000000-0000-4000-8000-000000000067"), kind: ErrConflict},
		{name: "retraction update", wrapper: fact(int64(1), "int", "00000000-0000-4000-8000-000000000068"), execs: []scriptedExec{{contains: "UPDATE fgraph_facts SET rx", err: failure}}, kind: ErrFormat},
		{name: "historical FTS delete", wrapper: fact("history", "text", "00000000-0000-4000-8000-000000000068"), execs: []scriptedExec{{contains: "DELETE FROM fgraph_fts", err: failure}}, kind: ErrFormat},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{execs: test.execs})
			err := maintenanceCoverageDB(runner).restoreSnapshotFact(ctx, runner, baseState(), test.wrapper)
			requireErrorKind(t, err, test.kind)
		})
	}
	if _, err := baseState().resolveSelector(true); err == nil {
		t.Fatal("invalid restore selector unexpectedly resolved")
	}

	receipt := func(created []any) map[string]any {
		wrapper := coverageSnapshotReceipt(7000)
		coverageReceiptFields(t, wrapper)["created"] = created
		return wrapper
	}
	for _, test := range []struct {
		exec    scriptedExec
		name    string
		created []any
	}{
		{name: "named identity", created: []any{"new/name"}, exec: scriptedExec{contains: "VALUES (?,?,NULL,?)", err: failure}},
		{name: "anonymous identity", created: []any{map[string]any{"eid": "00000000-0000-4000-8000-000000000070"}}, exec: scriptedExec{contains: "VALUES (?,NULL,?,?)", err: failure}},
		{name: "event identity", exec: scriptedExec{contains: "VALUES (?,NULL,?,?)", err: failure}},
		{name: "event receipt", exec: scriptedExec{contains: "INSERT INTO fgraph_events", err: failure}},
	} {
		t.Run("receipt "+test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{execs: []scriptedExec{test.exec}})
			state := &snapshotRestoreState{
				identities: map[string]int64{}, events: map[string]int64{genesisEventID: GenesisTx},
				receipts: map[int64]snapshotReceipt{}, next: 65,
			}
			err := maintenanceCoverageDB(runner).restoreSnapshotReceipt(ctx, runner, state, receipt(test.created))
			requireErrorKind(t, err, ErrConflict)
		})
	}
	invalidSelector := receipt([]any{true})
	state := &snapshotRestoreState{
		identities: map[string]int64{}, events: map[string]int64{genesisEventID: GenesisTx},
		receipts: map[int64]snapshotReceipt{}, next: 65,
	}
	runner := openScriptedSQL(t, scriptedSQL{})
	if err := maintenanceCoverageDB(runner).restoreSnapshotReceipt(ctx, runner, state, invalidSelector); err == nil {
		t.Fatal("invalid created selector unexpectedly restored")
	}
}

type maintenanceCoverageFailReader struct{ err error }

func (reader maintenanceCoverageFailReader) Read([]byte) (int, error) { return 0, reader.err }

func TestSnapshotCoverageRestoreStreamRollback(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if err := db.Restore(ctx, maintenanceCoverageFailReader{err: io.ErrUnexpectedEOF}); !errors.Is(err, ErrFormat) {
		t.Fatalf("reader failure = %v", err)
	}

	source := fixedDB(t, ":memory:")
	if _, err := source.Transact(ctx, E{"id": "rollback/source", "rollback/value": 1}); err != nil {
		t.Fatal(err)
	}
	var valid bytes.Buffer
	if err := source.Snapshot(ctx, &valid); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(valid.String()), "\n")
	if len(lines) < 4 {
		t.Fatalf("snapshot lines = %d", len(lines))
	}
	for name, stream := range map[string]string{
		"fact failure":       strings.Join([]string{lines[0], lines[1], `{"fact":[]}`}, "\n") + "\n",
		"receipt after fact": strings.Join([]string{lines[0], lines[1], lines[2], lines[1]}, "\n") + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			target := fixedDB(t, ":memory:")
			if err := target.Restore(ctx, strings.NewReader(stream)); err == nil {
				t.Fatal("malformed stream unexpectedly restored")
			}
			stats, err := target.Stats(ctx)
			if err != nil || stats.Transactions != 1 || stats.Facts != GenesisFactCount {
				t.Fatalf("failed restore was not atomic: %#v, %v", stats, err)
			}
		})
	}
}

func TestSchemaCoveragePublicPropagation(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected schema API failure")
	basis := scriptedQuery{contains: "SELECT COALESCE(MAX(tx),0)", columns: []string{"basis"}, rows: [][]driver.Value{{int64(GenesisTx)}}}
	identities := scriptedQuery{contains: "SELECT id,name FROM fgraph_ids", columns: []string{"id", "name"}, rows: [][]driver.Value{{int64(65), "schema/value"}}}
	schema := scriptedQuery{contains: "SELECT f.id,f.e,f.a,f.v,f.t,f.tx,f.rx", columns: []string{"id", "e", "a", "v", "t", "tx", "rx"}}
	declarations := scriptedQuery{contains: "SELECT f.a,f.v,f.t", columns: []string{"a", "v", "t"}}
	for _, test := range []struct {
		name    string
		queries []scriptedQuery
	}{
		{name: "basis", queries: []scriptedQuery{{contains: "SELECT COALESCE(MAX(tx),0)", err: failure}}},
		{name: "identities", queries: []scriptedQuery{basis, {contains: "SELECT id,name FROM fgraph_ids", err: failure}}},
		{name: "effective schema", queries: []scriptedQuery{basis, identities, {contains: "SELECT f.id,f.e,f.a,f.v,f.t,f.tx,f.rx", err: failure}}},
		{name: "declarations", queries: []scriptedQuery{basis, identities, schema, {contains: "SELECT f.a,f.v,f.t", err: failure}}},
		{name: "observations", queries: []scriptedQuery{basis, identities, schema, declarations, {contains: "SELECT f.t,COUNT(*)", err: failure}}},
		{name: "shapes", queries: []scriptedQuery{
			basis,
			{contains: "SELECT id,name FROM fgraph_ids", columns: []string{"id", "name"}},
			{contains: "SELECT DISTINCT f.e", err: failure},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: test.queries})
			if _, err := maintenanceCoverageDB(runner).Schema(ctx, "", false); !errors.Is(err, ErrFormat) {
				t.Fatalf("Schema error = %v", err)
			}
		})
	}
	db := fixedDB(t, ":memory:")
	if _, err := db.DeclareShape(ctx, "shape/invalid-allowed", ShapeDefinition{Allowed: []string{"bad"}}); !errors.Is(err, ErrSchema) {
		t.Fatalf("invalid allowed attribute error = %v", err)
	}
	runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{{contains: "SELECT COALESCE(MAX(tx),0)", err: failure}}})
	if _, err := maintenanceCoverageDB(runner).Validate(ctx); !errors.Is(err, ErrFormat) {
		t.Fatalf("Validate basis error = %v", err)
	}
}

func TestMaintenanceCoverageExciseInputAndEventIdentityFailures(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Excise(ctx, "missing", nil); !errors.Is(err, ErrType) {
		t.Fatalf("nil excision option error = %v", err)
	}
	if _, err := db.Excise(ctx, "missing", WithBy("agent")); !errors.Is(err, ErrType) {
		t.Fatalf("unsupported excision option error = %v", err)
	}
	if _, err := db.Excise(ctx, "missing", WithOperationID("")); !errors.Is(err, ErrType) {
		t.Fatalf("invalid excision operation error = %v", err)
	}
	if _, err := db.Excise(ctx, make(chan int)); !errors.Is(err, ErrType) {
		t.Fatalf("uncanonicalizable excision request error = %v", err)
	}

	for _, test := range []struct {
		kind    error
		factory EventIDFactory
		name    string
	}{
		{name: "factory error", factory: func() (string, error) { return "", errors.New("event factory failed") }},
		{name: "malformed UUID", factory: func() (string, error) { return "bad", nil }, kind: ErrType},
		{name: "non-v4 UUID", factory: func() (string, error) { return "00000000-0000-1000-8000-000000000001", nil }, kind: ErrType},
	} {
		t.Run(test.name, func(t *testing.T) {
			testDB := fixedDB(t, ":memory:")
			if _, err := testDB.Transact(ctx, E{"id": "excise/event-id", "item/value": 1}); err != nil {
				t.Fatal(err)
			}
			testDB.store.eventIDs = test.factory
			_, err := testDB.Excise(ctx, "excise/event-id")
			if test.kind == nil {
				if err == nil || !strings.Contains(err.Error(), "event factory failed") {
					t.Fatalf("event factory error = %v", err)
				}
				return
			}
			requireErrorKind(t, err, test.kind)
		})
	}
}

func TestMaintenanceCoverageSemanticRollbackAndHistoryFailures(t *testing.T) {
	ctx := context.Background()

	t.Run("unreconstructable retained event", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		report, transactErr := db.Transact(ctx, E{"id": "event/corrupt", "event/value": 1})
		if transactErr != nil {
			t.Fatal(transactErr)
		}
		factID := coveragePublicFactID(t, report, "event/value")
		if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_facts SET v='not-an-int' WHERE id=?", factID); err != nil {
			t.Fatal(err)
		}
		_, problems, inspectErr := db.inspectEventHashes(ctx, db.store.sql)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		maintenanceCoverageProblem(t, problems, "cannot be reconstructed")
	})

	t.Run("history policy query", func(t *testing.T) {
		failure := errors.New("history policy query failed")
		runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{
			{contains: "SELECT id FROM fgraph_ids", columns: []string{"id"}, rows: [][]driver.Value{{int64(65)}}},
			{contains: "SELECT EXISTS", err: failure},
		}})
		record := map[string]any{
			"asserted": []any{[]any{"item/entity", "item/value", int64(1), "int"}},
		}
		if _, err := maintenanceCoverageDB(runner).eventMayLoseHistory(ctx, runner, record); !errors.Is(err, ErrFormat) {
			t.Fatalf("history policy error = %v", err)
		}
	})

	for _, test := range []struct {
		prepare func(*testing.T, *DB, TxReport)
		name    string
	}{
		{name: "invalid retained payload", prepare: func(t *testing.T, db *DB, report TxReport) {
			if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_events SET event_data='{}' WHERE tx=?", report.Tx); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing prior event identity", prepare: func(t *testing.T, db *DB, report TxReport) {
			if _, err := db.store.sql.ExecContext(ctx, "PRAGMA ignore_check_constraints=ON"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_ids SET gid=? WHERE id=?", []byte{1}, report.Tx); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "blocked payload redaction", prepare: func(t *testing.T, db *DB, _ TxReport) {
			if _, err := db.store.sql.ExecContext(ctx, `CREATE TRIGGER block_event_redaction BEFORE UPDATE OF event_data ON fgraph_events
				WHEN NEW.event_data IS NULL BEGIN SELECT RAISE(ABORT,'blocked redaction'); END`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "blocked excision receipt", prepare: func(t *testing.T, db *DB, _ TxReport) {
			if _, err := db.store.sql.ExecContext(ctx, `CREATE TRIGGER block_excision_event BEFORE INSERT ON fgraph_events
				BEGIN SELECT RAISE(ABORT,'blocked excision event'); END`); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			report, err := db.Transact(ctx, E{"id": "excise/rollback", "item/value": "retained"})
			if err != nil {
				t.Fatal(err)
			}
			test.prepare(t, db, report)
			if _, err := db.Excise(ctx, "excise/rollback"); err == nil {
				t.Fatal("faulted excision unexpectedly committed")
			}
			var liveFacts int64
			if err := db.store.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM fgraph_facts
				WHERE e=(SELECT id FROM fgraph_ids WHERE name='excise/rollback') AND rx IS NULL`).Scan(&liveFacts); err != nil {
				t.Fatal(err)
			}
			if liveFacts == 0 {
				t.Fatal("failed excision removed the target's live facts")
			}
		})
	}
}

func TestSnapshotCoverageRejectsInvalidRetainedStateAndRollsBack(t *testing.T) {
	ctx := context.Background()
	source := fixedDB(t, ":memory:")
	report, transactErr := source.Transact(ctx, E{"id": "snapshot/invalid", "snapshot/cardinality": 1})
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	var entity, attribute int64
	if err := source.store.sql.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE name='snapshot/invalid'").Scan(&entity); err != nil {
		t.Fatal(err)
	}
	if err := source.store.sql.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE name='snapshot/cardinality'").Scan(&attribute); err != nil {
		t.Fatal(err)
	}
	if _, err := source.store.sql.ExecContext(ctx, `INSERT INTO fgraph_facts(e,a,v,t,tx,rx)
		VALUES (?,?,2,2,?,NULL)`, entity, attribute, report.Tx); err != nil {
		t.Fatal(err)
	}
	var invalid bytes.Buffer
	if err := source.Snapshot(ctx, &invalid); err != nil {
		t.Fatal(err)
	}
	target := fixedDB(t, ":memory:")
	if err := target.Restore(ctx, bytes.NewReader(invalid.Bytes())); !errors.Is(err, ErrFormat) {
		t.Fatalf("invalid retained state restore error = %v", err)
	}
	stats, statsErr := target.Stats(ctx)
	if statsErr != nil || stats.Transactions != 1 || stats.Facts != GenesisFactCount {
		t.Fatalf("rejected retained state was not atomic: %#v, %v", stats, statsErr)
	}

	clean := fixedDB(t, ":memory:")
	if _, err := clean.Transact(ctx, E{"id": "snapshot/clean", "snapshot/value": 1}); err != nil {
		t.Fatal(err)
	}
	var valid bytes.Buffer
	if err := clean.Snapshot(ctx, &valid); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(valid.String()), "\n")
	footer, decodeErr := DecodeJSON(strings.NewReader(lines[len(lines)-1]))
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	footerObject, ok := objectMap(footer)
	if !ok {
		t.Fatalf("footer = %#v", footer)
	}
	footerObject["sha256"] = strings.Repeat("0", sha256.Size*2)
	badFooter, canonicalErr := canonicalJSON(footerObject)
	if canonicalErr != nil {
		t.Fatal(canonicalErr)
	}
	lines[len(lines)-1] = string(badFooter)
	t.Run("digest", func(t *testing.T) {
		digestTarget := fixedDB(t, ":memory:")
		if err := digestTarget.Restore(ctx, strings.NewReader(strings.Join(lines, "\n")+"\n")); !errors.Is(err, ErrConflict) {
			t.Fatalf("modified digest restore error = %v", err)
		}
	})

	t.Run("allocator finalization", func(t *testing.T) {
		allocatorTarget := fixedDB(t, ":memory:")
		if _, err := allocatorTarget.store.sql.ExecContext(ctx, `CREATE TRIGGER block_restored_allocator BEFORE UPDATE ON fgraph_meta
			WHEN OLD.key='next_id' BEGIN SELECT RAISE(ABORT,'blocked restored allocator'); END`); err != nil {
			t.Fatal(err)
		}
		if err := allocatorTarget.Restore(ctx, bytes.NewReader(valid.Bytes())); !errors.Is(err, ErrFormat) {
			t.Fatalf("allocator restore error = %v", err)
		}
		stats, err := allocatorTarget.Stats(ctx)
		if err != nil || stats.Transactions != 1 || stats.Facts != GenesisFactCount {
			t.Fatalf("allocator failure was not atomic: %#v, %v", stats, err)
		}
	})
}
