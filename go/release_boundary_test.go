package fgraph

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type duplicateMCPObject struct{}

func (duplicateMCPObject) MarshalJSON() ([]byte, error) {
	return []byte(`{"duplicate":1,"duplicate":2}`), nil
}

func TestTxReportDefaultWireStates(t *testing.T) {
	for name, test := range map[string]struct {
		wantTx     any
		wantAt     any
		wantEvent  any
		wantStatus string
		report     TxReport
	}{
		"noop": {report: TxReport{BasisTx: GenesisTx}, wantStatus: "noop"},
		"applied": {
			report:     TxReport{Tx: FirstUserID, At: 1_767_225_600_000_000, EventID: "11111111-1111-4111-8111-111111111111", BasisTx: GenesisTx},
			wantStatus: "applied", wantTx: float64(FirstUserID), wantAt: float64(1_767_225_600_000_000), wantEvent: "11111111-1111-4111-8111-111111111111",
		},
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := json.Marshal(test.report)
			if err != nil {
				t.Fatal(err)
			}
			var wire map[string]any
			if err := json.Unmarshal(encoded, &wire); err != nil {
				t.Fatal(err)
			}
			if wire["status"] != test.wantStatus || wire["tx"] != test.wantTx || wire["at"] != test.wantAt || wire["event"] != test.wantEvent {
				t.Fatalf("TxReport wire = %#v", wire)
			}
		})
	}
}

func TestCriticalBoundaryFailuresRemainTyped(t *testing.T) {
	if _, err := canonicalMCPBytes(duplicateMCPObject{}); !errors.Is(err, ErrFormat) {
		t.Fatalf("duplicate-key MCP output error = %v, want FormatError", err)
	}

	runner := openScriptedSQL(t, scriptedSQL{})
	conn, err := runner.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rollbackSQLite(conn, "closed test transaction"); !errors.Is(err, ErrFormat) {
		t.Fatalf("rollback on closed connection error = %v, want FormatError", err)
	}
}

func TestStorageInspectionFailuresRemainTyped(t *testing.T) {
	ctx := context.Background()
	rowFailure := errors.New("storage iteration failed")
	for name, rule := range map[string]scriptedQuery{
		"query": {
			contains: "SELECT name,type", err: rowFailure,
		},
		"decode": {
			contains: "SELECT name,type", columns: []string{"name", "type", "sql"}, rows: [][]driver.Value{{"object", "table"}},
		},
		"iteration": {
			contains: "SELECT name,type", columns: []string{"name", "type", "sql"}, nextErr: rowFailure,
		},
	} {
		t.Run("schema "+name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{rule}})
			if _, err := readSchemaObjects(ctx, runner); !errors.Is(err, ErrFormat) {
				t.Fatalf("schema inspection error = %v, want FormatError", err)
			}
		})
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := referenceSchemaObjects(canceled); !errors.Is(err, ErrFormat) {
		t.Fatalf("canceled reference schema error = %v, want FormatError", err)
	}

	closedDB, _ := closedRunnerDB(t)
	if _, err := closedDB.Receipt(ctx, FirstUserID); !errors.Is(err, ErrFormat) {
		t.Fatalf("receipt on unavailable store error = %v, want FormatError", err)
	}

	queryFailure := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{{
		contains: "SELECT id FROM fgraph_ids", err: rowFailure,
	}}})
	failedApply := &DB{store: &store{sql: queryFailure}}
	event := `{"fgraph":"event/1","event":"11111111-1111-4111-8111-111111111111","at":1,"created":[],"asserted":[],"retracted":[]}`
	if _, err := failedApply.applyEventLine(ctx, queryFailure, []byte(event), 1); !errors.Is(err, ErrFormat) {
		t.Fatalf("apply identity inspection error = %v, want FormatError", err)
	}

	historical := fixedDB(t, ":memory:")
	view, err := historical.ViewAt(ctx, GenesisTx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := view.eventRecordForTx(ctx, FirstUserID); !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "historical horizon") {
		t.Fatalf("historical event boundary error = %v, want NotFound horizon", err)
	}
}
