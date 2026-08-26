package fgraph

import (
	"context"
	"database/sql"
)

// preparedRunner reuses parsed SQLite statements for one coherent read or
// write operation. Keeping the cache operation-scoped avoids cross-transaction
// lifetime and schema-invalidation complexity while accelerating fact loops.
type preparedRunner struct {
	conn       *sql.Conn
	statements map[string]*cachedStatement
	ordered    []*cachedStatement
}

type cachedStatement struct{ statement *sql.Stmt }

func newPreparedRunner(conn *sql.Conn) *preparedRunner {
	return &preparedRunner{conn: conn, statements: map[string]*cachedStatement{}}
}

func (runner *preparedRunner) statement(ctx context.Context, query string) (*cachedStatement, error) {
	if statement, found := runner.statements[query]; found {
		return statement, nil
	}
	prepared, err := runner.conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	statement := &cachedStatement{statement: prepared}
	runner.statements[query] = statement
	runner.ordered = append(runner.ordered, statement)
	return statement, nil
}

func (runner *preparedRunner) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	statement, err := runner.statement(ctx, query)
	if err != nil {
		return nil, err
	}
	return statement.statement.ExecContext(ctx, args...)
}

func (runner *preparedRunner) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	// A pull can recursively issue the same SELECT while its outer cursor is
	// still open. Preparing that SQL once would make both cursors share one
	// modernc SQLite statement, which is not re-entrant. Cursor queries therefore
	// keep database/sql's independent statement lifetime; repeated writes and
	// scalar lookups still use the operation cache below.
	return runner.conn.QueryContext(ctx, query, args...)
}

func (runner *preparedRunner) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	statement, err := runner.statement(ctx, query)
	if err != nil {
		// database/sql exposes preparation failures through Row.Scan. Running the
		// original query preserves that error contract for sqlRunner callers.
		return runner.conn.QueryRowContext(ctx, query, args...)
	}
	return statement.statement.QueryRowContext(ctx, args...)
}

func (runner *preparedRunner) Close() (resultErr error) {
	for _, statement := range runner.ordered {
		resultErr = joinErrors(resultErr, statement.statement.Close())
	}
	return resultErr
}
