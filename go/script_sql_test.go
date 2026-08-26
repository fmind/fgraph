package fgraph

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

type scriptedQuery struct {
	err      error
	nextErr  error
	closeErr error
	contains string
	columns  []string
	rows     [][]driver.Value
}

type scriptedExec struct {
	err             error
	lastInsertIDErr error
	rowsAffectedErr error
	contains        string
}

type scriptedSQL struct {
	queries []scriptedQuery
	execs   []scriptedExec
}

type scriptedDriver struct{ script *scriptedSQL }

func (d scriptedDriver) Open(string) (driver.Conn, error) {
	return &scriptedConn{script: d.script}, nil
}

type scriptedConn struct{ script *scriptedSQL }

func (c *scriptedConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("scripted prepare is unsupported")
}
func (c *scriptedConn) Close() error              { return nil }
func (c *scriptedConn) Begin() (driver.Tx, error) { return scriptedTx{}, nil }
func (c *scriptedConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	for _, rule := range c.script.queries {
		if strings.Contains(query, rule.contains) {
			if rule.err != nil {
				return nil, rule.err
			}
			return &scriptedRows{columns: rule.columns, rows: rule.rows, nextErr: rule.nextErr, closeErr: rule.closeErr}, nil
		}
	}
	return nil, errors.New("unexpected scripted query: " + query)
}

func (c *scriptedConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	for _, rule := range c.script.execs {
		if strings.Contains(query, rule.contains) {
			return scriptedResult{lastInsertID: 1, lastInsertIDErr: rule.lastInsertIDErr, rowsAffectedErr: rule.rowsAffectedErr}, rule.err
		}
	}
	return scriptedResult{lastInsertID: 1}, nil
}

type scriptedResult struct {
	lastInsertIDErr error
	rowsAffectedErr error
	lastInsertID    int64
}

func (r scriptedResult) LastInsertId() (int64, error) { return r.lastInsertID, r.lastInsertIDErr }
func (r scriptedResult) RowsAffected() (int64, error) { return 1, r.rowsAffectedErr }

type scriptedTx struct{}

func (scriptedTx) Commit() error   { return nil }
func (scriptedTx) Rollback() error { return nil }

type scriptedRows struct {
	nextErr  error
	closeErr error
	columns  []string
	rows     [][]driver.Value
	index    int
	errSent  bool
}

func (r *scriptedRows) Columns() []string { return r.columns }
func (r *scriptedRows) Close() error      { return r.closeErr }
func (r *scriptedRows) Next(values []driver.Value) error {
	if r.index < len(r.rows) {
		copy(values, r.rows[r.index])
		r.index++
		return nil
	}
	if r.nextErr != nil && !r.errSent {
		r.errSent = true
		return r.nextErr
	}
	return io.EOF
}

var scriptedDriverID atomic.Uint64

func openScriptedSQL(t *testing.T, script scriptedSQL) *sql.DB {
	t.Helper()
	name := "fgraph-scripted-" + strconv.FormatUint(scriptedDriverID.Add(1), 10)
	sql.Register(name, scriptedDriver{script: &script})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTest(t, db) })
	return db
}

func TestPreparedRunnerPreservesQueryRowPreparationErrors(t *testing.T) {
	ctx := context.Background()
	db := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{{
		contains: "SELECT value", columns: []string{"value"}, rows: [][]driver.Value{{int64(7)}},
	}}})
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, conn)
	runner := newPreparedRunner(conn)
	defer closeTest(t, runner)

	var value int64
	if err := runner.QueryRowContext(ctx, "SELECT value").Scan(&value); err != nil || value != 7 {
		t.Fatalf("fallback scalar query = %d, %v", value, err)
	}
}

func TestRowDecodeAndIterationFailureMatrix(t *testing.T) {
	ctx := context.Background()
	rowErr := errors.New("row iteration failed")
	badScan := func(columns int) scriptedQuery {
		names := make([]string, columns)
		values := make([]driver.Value, columns-1)
		return scriptedQuery{columns: names, rows: [][]driver.Value{values}}
	}
	db := fixedDB(t, ":memory:")

	for _, test := range []struct {
		run  func(*sql.DB) error
		name string
		rule scriptedQuery
	}{
		{name: "scanRawFacts scan", run: func(r *sql.DB) error {
			rows, queryErr := r.QueryContext(ctx, "SELECT facts")
			if queryErr != nil {
				return queryErr
			}
			_, err := scanRawFacts(rows)
			return err
		}, rule: badScan(7)},
		{name: "scanRawFacts iteration", run: func(r *sql.DB) error {
			rows, queryErr := r.QueryContext(ctx, "SELECT facts")
			if queryErr != nil {
				return queryErr
			}
			_, err := scanRawFacts(rows)
			return err
		}, rule: scriptedQuery{columns: make([]string, 7), nextErr: rowErr}},
		{name: "queryFacts scan", run: func(r *sql.DB) error { _, err := db.queryFacts(ctx, r); return err }, rule: badScan(4)},
		{name: "queryFacts logical", run: func(r *sql.DB) error { _, err := db.queryFacts(ctx, r); return err }, rule: scriptedQuery{columns: make([]string, 4), rows: [][]driver.Value{{"bad", int64(TagBytes), int64(65), "a/b"}}}},
		{name: "queryFacts iteration", run: func(r *sql.DB) error { _, err := db.queryFacts(ctx, r); return err }, rule: scriptedQuery{columns: make([]string, 4), nextErr: rowErr}},
		{name: "schema scan", run: func(r *sql.DB) error { _, err := db.schemaFor(ctx, r, 65, nil); return err }, rule: badScan(3)},
		{name: "schema iteration", run: func(r *sql.DB) error { _, err := db.schemaFor(ctx, r, 65, nil); return err }, rule: scriptedQuery{columns: make([]string, 3), nextErr: rowErr}},
		{name: "transaction info scan", run: func(r *sql.DB) error { _, err := db.transactionInfo(ctx, r, 65); return err }, rule: badScan(3)},
		{name: "transaction info logical", run: func(r *sql.DB) error { _, err := db.transactionInfo(ctx, r, 65); return err }, rule: scriptedQuery{columns: make([]string, 3), rows: [][]driver.Value{{"bad", int64(TagBytes), "a/b"}}}},
		{name: "transaction info iteration", run: func(r *sql.DB) error { _, err := db.transactionInfo(ctx, r, 65); return err }, rule: scriptedQuery{columns: make([]string, 3), nextErr: rowErr}},
		{name: "keyword scan", run: func(r *sql.DB) error {
			_, err := db.keywordCandidatesBounded(ctx, r, "word", nil, nil, searchCandidateLimit, &searchWork{limit: 10})
			return err
		}, rule: badScan(9)},
		{name: "keyword iteration", run: func(r *sql.DB) error {
			_, err := db.keywordCandidatesBounded(ctx, r, "word", nil, nil, searchCandidateLimit, &searchWork{limit: 10})
			return err
		}, rule: scriptedQuery{columns: make([]string, 9), nextErr: rowErr}},
		{name: "expansion scan", run: func(r *sql.DB) error { _, _, err := db.expandSearch(ctx, r, []int64{65}, 1); return err }, rule: badScan(7)},
		{name: "expansion iteration", run: func(r *sql.DB) error { _, _, err := db.expandSearch(ctx, r, []int64{65}, 1); return err }, rule: scriptedQuery{columns: make([]string, 7), nextErr: rowErr}},
		{name: "raw facts scan", run: func(r *sql.DB) error {
			testDB := &DB{store: &store{sql: r, names: map[string]int64{}}, exec: r}
			_, err := testDB.RawFacts(ctx, true)
			return err
		}, rule: badScan(7)},
	} {
		t.Run(test.name, func(t *testing.T) {
			rule := test.rule
			rule.contains = "SELECT"
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{rule}})
			if err := test.run(runner); !errors.Is(err, ErrFormat) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestCacheAndExportRowFailures(t *testing.T) {
	ctx := context.Background()
	rowErr := errors.New("iteration failed")
	for _, test := range []struct {
		name   string
		idRows scriptedQuery
	}{
		{name: "cache scan", idRows: scriptedQuery{contains: "SELECT id,name", columns: []string{"id", "name"}, rows: [][]driver.Value{{int64(1)}}}},
		{name: "cache iteration", idRows: scriptedQuery{contains: "SELECT id,name", columns: []string{"id", "name"}, nextErr: rowErr}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{
				{contains: "PRAGMA data_version", columns: []string{"v"}, rows: [][]driver.Value{{int64(1)}}}, test.idRows,
			}})
			root := &store{names: map[string]int64{}, dataVersion: -1}
			if err := root.refreshNames(ctx, runner); !errors.Is(err, ErrFormat) {
				t.Fatalf("refresh error = %v", err)
			}
		})
	}
	for _, rule := range []scriptedQuery{
		{contains: "SELECT e,v", columns: []string{"e", "v"}, rows: [][]driver.Value{{int64(65)}}},
		{contains: "SELECT e,v", columns: []string{"e", "v"}, closeErr: rowErr},
	} {
		runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{rule}})
		testDB := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
		if err := testDB.Tail(ctx, &strings.Builder{}, GenesisTx); !errors.Is(err, ErrFormat) {
			t.Errorf("export row failure = %v", err)
		}
	}
}
