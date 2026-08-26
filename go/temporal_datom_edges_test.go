package fgraph

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestDatomsHistoryRetractionAndCorruptValueBoundaries(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{"id": "datom/entity", "datom/value": "old"}); err != nil {
		t.Fatal(err)
	}
	retracted, err := db.Transact(ctx, []any{"retract", "datom/entity", "datom/value", "old"})
	if err != nil {
		t.Fatal(err)
	}
	page, err := db.Datoms(ctx, DatomOptions{
		Index: "eavt", Source: "history", Limit: 10,
		Components: []any{"datom/entity", "datom/value", "old", retracted.Tx, false},
	})
	if err != nil || len(page.Items) != 1 || page.Items[0].Added || page.Items[0].Tx != retracted.Tx {
		t.Fatalf("retraction datom page = %#v, %v", page, err)
	}
	asserted, err := db.Datoms(ctx, DatomOptions{
		Index: "eavt", Source: "history", Limit: 10,
		Components: []any{"datom/entity", "datom/value", "old", page.Items[0].FactID, true},
	})
	if err != nil {
		// A fact id is deliberately not a transaction selector.
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("invalid assertion transaction selector error = %v", err)
		}
	} else if len(asserted.Items) != 0 {
		t.Fatalf("fact id unexpectedly selected assertion transaction: %#v", asserted)
	}
	history, err := db.History(ctx, "datom/entity", "datom/value")
	if err != nil || len(history) != 1 {
		t.Fatalf("datom source history = %#v, %v", history, err)
	}
	asserted, err = db.Datoms(ctx, DatomOptions{
		Index: "eavt", Source: "history", Limit: 10,
		Components: []any{"datom/entity", "datom/value", "old", history[0].Tx, true},
	})
	if err != nil || len(asserted.Items) != 1 || !asserted.Items[0].Added {
		t.Fatalf("assertion datom page = %#v, %v", asserted, err)
	}
	if _, err := db.Datoms(ctx, DatomOptions{Cursor: "not-base64", Limit: 1}); !errors.Is(err, ErrType) {
		t.Fatalf("malformed public datom cursor error = %v", err)
	}

	corrupt := fixedDB(t, ":memory:")
	if _, err := corrupt.Declare(ctx, "datom/count", Type("int")); err != nil {
		t.Fatal(err)
	}
	if _, err := corrupt.Transact(ctx, E{"id": "datom/corrupt", "datom/count": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := corrupt.store.sql.ExecContext(ctx, `UPDATE fgraph_facts SET v='bad' WHERE a=(SELECT id FROM fgraph_ids WHERE name='datom/count')`); err != nil {
		t.Fatal(err)
	}
	if _, err := corrupt.Datoms(ctx, DatomOptions{Index: "avet", Components: []any{"datom/count"}, Limit: 10}); !errors.Is(err, ErrFormat) {
		t.Fatalf("corrupt datom value error = %v, want FormatError", err)
	}
}

func TestDatomsPropagatesBasisFailure(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("basis failure")
	runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{{contains: "SELECT COALESCE(MAX(tx)", err: failure}}})
	db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
	if _, err := db.Datoms(ctx, DatomOptions{Limit: 1}); !errors.Is(err, ErrFormat) {
		t.Fatalf("datom basis error = %v, want FormatError", err)
	}
}

func TestDatomPagePropagatesSQLiteRowFailures(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("datom row failure")
	for _, test := range []struct {
		name string
		rule scriptedQuery
	}{
		{name: "query", rule: scriptedQuery{err: failure}},
		{name: "scan", rule: scriptedQuery{columns: make([]string, 8), rows: [][]driver.Value{{int64(1)}}}},
		{name: "iteration", rule: scriptedQuery{columns: make([]string, 8), nextErr: failure}},
		{name: "close", rule: scriptedQuery{columns: make([]string, 8), closeErr: failure}},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.rule.contains = "SELECT d.id"
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
			_, err := db.readDatomPage(ctx, runner, DatomOptions{Index: "eavt", Source: "current", Limit: 1}, GenesisTx, GenesisTx, nil, nil, nil)
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("readDatomPage error = %v, want FormatError", err)
			}
		})
	}
}

func TestExplainPlansBarriersScansAndHistoricalBasis(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	first, transactErr := db.Transact(ctx, E{"id": "explain/entity", "explain/value": int64(1)})
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	if _, err := db.Transact(ctx, E{"id": "explain/later", "explain/value": int64(2)}); err != nil {
		t.Fatal(err)
	}
	barrier := Object{Fields: []Field{{Name: "rule", Value: []any{"candidate", "?seed"}}}}
	plan, planErr := db.atTx(first.Tx).Explain(ctx, Q{
		Find: []any{"?entity"},
		Where: []any{
			barrier,
			[]any{"?entity", "?attribute", "?value"},
		},
	}, nil)
	if planErr != nil {
		t.Fatal(planErr)
	}
	if plan.BasisTx != first.Tx || len(plan.Clauses) != 2 || plan.Clauses[0].Kind != "barrier" || len(plan.Warnings) != 1 || !strings.Contains(plan.Warnings[0], "scan") {
		t.Fatalf("historical barrier plan = %#v", plan)
	}
	if _, err := db.Explain(ctx, Q{Where: []any{[]any{"=", "?missing", int64(1)}}}, nil); !errors.Is(err, ErrQuery) {
		t.Fatalf("invalid explain clause error = %v", err)
	}
	if _, err := db.ExplainJSON(ctx, []any{}, nil); !errors.Is(err, ErrQuery) {
		t.Fatalf("invalid explain JSON error = %v", err)
	}

	failure := errors.New("basis failure")
	runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{{contains: "SELECT COALESCE(MAX(tx)", err: failure}}})
	faultDB := &DB{store: &store{sql: runner, names: map[string]int64{}, queryBudget: DefaultQueryBudget}, exec: runner}
	if _, err := faultDB.Explain(ctx, Q{}, nil); !errors.Is(err, ErrFormat) {
		t.Fatalf("explain basis error = %v, want FormatError", err)
	}
}

func TestTemporalAndReceiptCorruptionRemainsVisible(t *testing.T) {
	ctx := context.Background()
	if _, err := fixedDB(t, ":memory:").Receipt(ctx, GenesisTx-1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown receipt error = %v", err)
	}

	corruptReceipt := fixedDB(t, ":memory:")
	report, receiptTxErr := corruptReceipt.Transact(ctx, E{"id": "receipt/entity", "receipt/value": true}, WithTxFacts(E{"audit/count": int64(1)}))
	if receiptTxErr != nil {
		t.Fatal(receiptTxErr)
	}
	if _, err := corruptReceipt.store.sql.ExecContext(ctx, `UPDATE fgraph_facts SET v='bad' WHERE e=? AND a=(SELECT id FROM fgraph_ids WHERE name='audit/count')`, report.Tx); err != nil {
		t.Fatal(err)
	}
	if _, err := corruptReceipt.Receipt(ctx, report.Tx); !errors.Is(err, ErrFormat) {
		t.Fatalf("corrupt custom receipt fact error = %v", err)
	}

	corruptTemporal := fixedDB(t, ":memory:")
	changed, temporalTxErr := corruptTemporal.Transact(ctx, E{"id": "temporal/entity", "temporal/value": int64(1)})
	if temporalTxErr != nil {
		t.Fatal(temporalTxErr)
	}
	coveragePublicCorruptTag(t, corruptTemporal, coveragePublicFactID(t, changed, "temporal/value"))
	for name, read := range map[string]func() error{
		"history": func() error { _, err := corruptTemporal.History(ctx, "temporal/entity"); return err },
		"why":     func() error { _, err := corruptTemporal.Why(ctx, "temporal/entity"); return err },
		"diff":    func() error { _, err := corruptTemporal.Diff(ctx, GenesisTx, changed.Tx); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := read(); !errors.Is(err, ErrFormat) {
				t.Fatalf("corrupt %s error = %v, want FormatError", name, err)
			}
		})
	}

	metadataOnly := fixedDB(t, ":memory:")
	metadata, metadataErr := metadataOnly.Transact(ctx, E{"id": "undo/noop"}, WithBy("audit"))
	if metadataErr != nil {
		t.Fatal(metadataErr)
	}
	basis, basisErr := metadataOnly.latestTx(ctx)
	if basisErr != nil {
		t.Fatal(basisErr)
	}
	undone, undoErr := metadataOnly.Undo(ctx, metadata.Tx, WithOperationID("undo-noop-receipt"), IfBasis(basis))
	if undoErr != nil || undone.Status != "applied" {
		t.Fatalf("receipt-bearing no-op undo = %#v, %v", undone, undoErr)
	}
	receipt, receiptErr := metadataOnly.Receipt(ctx, undone.Tx)
	if receiptErr != nil || receipt.OperationID == nil || *receipt.OperationID != "undo-noop-receipt" {
		t.Fatalf("no-op undo receipt = %#v, %v", receipt, receiptErr)
	}
}

func TestReceiptSQLiteFailuresAndImportedOrigin(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("receipt row failure")
	basis := scriptedQuery{contains: "SELECT COALESCE(MAX(tx)", columns: []string{"basis"}, rows: [][]driver.Value{{int64(GenesisTx)}}}
	validReceipt := scriptedQuery{
		contains: "SELECT ev.event_hash", columns: []string{"hash", "operation", "request", "gid", "basis"},
		rows: [][]driver.Value{{make([]byte, 32), nil, nil, make([]byte, 16), int64(GenesisTx)}},
	}
	for _, test := range []struct {
		name    string
		queries []scriptedQuery
	}{
		{name: "receipt query", queries: []scriptedQuery{basis, {contains: "SELECT ev.event_hash", err: failure}}},
		{name: "metadata query", queries: []scriptedQuery{basis, validReceipt, {contains: "SELECT id,e,a,v", err: failure}}},
		{name: "metadata scan", queries: []scriptedQuery{basis, validReceipt, {
			contains: "SELECT id,e,a,v", columns: make([]string, 7), rows: [][]driver.Value{{int64(1)}},
		}}},
		{name: "metadata iteration", queries: []scriptedQuery{basis, validReceipt, {
			contains: "SELECT id,e,a,v", columns: make([]string, 7), nextErr: failure,
		}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: test.queries})
			db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
			if _, err := db.Receipt(ctx, GenesisTx); !errors.Is(err, ErrFormat) {
				t.Fatalf("Receipt error = %v, want FormatError", err)
			}
		})
	}

	source := fixedDB(t, ":memory:")
	created, transactErr := source.Transact(ctx, E{"id": "portable/origin", "portable/value": "origin"})
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	var stream bytes.Buffer
	if err := source.Tail(ctx, &stream, GenesisTx); err != nil {
		t.Fatal(err)
	}
	target := fixedDB(t, ":memory:")
	reports, applyErr := target.Apply(ctx, bytes.NewReader(stream.Bytes()))
	if applyErr != nil || len(reports) != 1 {
		t.Fatalf("portable apply reports = %#v, %v", reports, applyErr)
	}
	imported, receiptErr := target.Receipt(ctx, reports[0].Tx)
	if receiptErr != nil || imported.ImportedAt == nil || *imported.ImportedAt != created.At {
		t.Fatalf("imported receipt = %#v, %v; origin at=%d", imported, receiptErr, created.At)
	}
}

func TestTemporalReadersPropagateSQLiteQueryAndScanFailures(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("temporal row failure")
	resolve := scriptedQuery{contains: "SELECT id FROM fgraph_ids", columns: []string{"id"}, rows: [][]driver.Value{{int64(65)}}}
	for _, test := range []struct {
		name    string
		read    func(*DB) error
		queries []scriptedQuery
	}{
		{name: "history query", read: func(db *DB) error { _, err := db.History(ctx, "known"); return err }, queries: []scriptedQuery{
			resolve, {contains: "SELECT id,e,a,v", err: failure},
		}},
		{name: "history scan", read: func(db *DB) error { _, err := db.History(ctx, "known"); return err }, queries: []scriptedQuery{
			resolve, {contains: "SELECT id,e,a,v", columns: make([]string, 7), rows: [][]driver.Value{{int64(1)}}},
		}},
		{name: "why query", read: func(db *DB) error { _, err := db.Why(ctx, "known"); return err }, queries: []scriptedQuery{
			resolve, {contains: "SELECT f.id,f.e", err: failure},
		}},
		{name: "why scan", read: func(db *DB) error { _, err := db.Why(ctx, "known"); return err }, queries: []scriptedQuery{
			resolve, {contains: "SELECT f.id,f.e", columns: make([]string, 7), rows: [][]driver.Value{{int64(1)}}},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: test.queries})
			db := &DB{store: &store{sql: runner, names: map[string]int64{"known": 65}}, exec: runner}
			if err := test.read(db); !errors.Is(err, ErrFormat) {
				t.Fatalf("temporal reader error = %v, want FormatError", err)
			}
		})
	}
}
