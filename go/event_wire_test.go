package fgraph

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func TestEventStreamApplyAllLogicalTagsAndFailures(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	longText := strings.Repeat("text", 80)
	longBytes := bytes.Repeat([]byte{0xab}, 300)
	if _, err := db.Transact(ctx, E{
		"id": "all", "tag/bool": true, "tag/int": int64(9), "tag/float": 1.25,
		"tag/text": longText, "tag/instant": Instant(1_700_000_000_000_000), "tag/bytes": Bytes(longBytes),
		"tag/vector": Vector([]float32{1, 2, 3}), "tag/json": JSON(E{"nested": []any{int64(1), "é"}}),
		"tag/ref": RefTo("target"),
	}, WithBy("agent"), WithSource("test"), WithMeta(E{"ok": true}),
		WithTxFacts(E{"audit/ref": RefTo("target"), "audit/bytes": Bytes(longBytes)})); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := db.Tail(ctx, &out, GenesisTx); err != nil {
		t.Fatal(err)
	}
	for _, tag := range []string{"ref", "bool", "int", "float", "text", "instant", "bytes", "vector", "json"} {
		if !bytes.Contains(out.Bytes(), []byte(`"`+tag+`"`)) {
			t.Errorf("event stream lacks logical tag %q: %s", tag, out.String())
		}
	}
	if bytes.Contains(out.Bytes(), []byte("text_ref")) || bytes.Contains(out.Bytes(), []byte("bytes_ref")) {
		t.Fatalf("physical indirect tags leaked: %s", out.String())
	}
	target := fixedDB(t, ":memory:")
	if _, err := target.Apply(ctx, bytes.NewReader(out.Bytes())); err != nil {
		t.Fatal(err)
	}
	got, err := target.Entity(ctx, "all")
	if err != nil {
		t.Fatal(err)
	}
	if got["tag/text"] != longText || !reflect.DeepEqual(got["tag/bytes"], map[string]any{"bytes": base64.StdEncoding.EncodeToString(longBytes)}) {
		t.Fatalf("indirect values changed: %#v", got)
	}
	var targetOut bytes.Buffer
	if err := target.Tail(ctx, &targetOut, GenesisTx); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(targetOut.Bytes(), []byte(`"audit/ref"`)) || !bytes.Contains(targetOut.Bytes(), []byte(`"by":"agent"`)) {
		t.Fatalf("transaction provenance lost: %s", targetOut.String())
	}

	if err := db.Tail(ctx, errorWriter{}, GenesisTx); !errors.Is(err, ErrFormat) {
		t.Fatalf("failed event stream writer error = %v", err)
	}
	if _, err := db.eventRecordForTx(ctx, 999_999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown event transaction error = %v", err)
	}
	if _, err := db.Apply(ctx, nil); !errors.Is(err, ErrType) {
		t.Fatalf("nil apply reader error = %v", err)
	}
	if _, err := db.Apply(ctx, errorReader{}); !errors.Is(err, ErrFormat) {
		t.Fatalf("failed apply reader error = %v", err)
	}
}

func TestTaggedInputAndLogicalTagMatrix(t *testing.T) {
	cases := []struct {
		value any
		tag   string
	}{
		{RefTo("named"), "ref"},
		{Instant(1), "instant"},
		{Bytes([]byte{1}), "bytes"},
		{Vector([]float32{1}), "vector"},
		{JSON(E{"a": 1}), "json"},
		{Object{Fields: []Field{{Name: "ref", Value: int64(70)}}}, "ref"},
		{true, "bool"},
		{int64(3), "int"},
		{float64(3.5), "float"},
		{int64(3), "float"},
		{"text", "text"},
		{"1970-01-01T00:00:00.000001Z", "instant"},
		{int64(1), "instant"},
		{"AQI=", "bytes"},
		{[]any{float64(1), float64(2)}, "vector"},
		{nil, "json"},
	}
	for _, test := range cases {
		if _, err := taggedInput(test.value, test.tag); err != nil {
			t.Errorf("taggedInput(%#v, %q) = %v", test.value, test.tag, err)
		}
	}
	invalid := []struct {
		value any
		tag   string
	}{
		{true, "bogus"},
		{"yes", "bool"},
		{float64(1), "int"},
		{true, "float"},
		{int64(1), "text"},
		{true, "instant"},
		{int64(1), "bytes"},
		{"bad", "vector"},
		{RefTo("x"), "text"},
	}
	for _, test := range invalid {
		if _, err := taggedInput(test.value, test.tag); !errors.Is(err, ErrType) {
			t.Errorf("invalid taggedInput(%#v, %q) error = %v", test.value, test.tag, err)
		}
	}
	if _, err := taggedInput(true, "ref"); err != nil {
		t.Fatalf("ref input is normalized by the caller: %v", err)
	}
	for tag := Tag(-1); tag <= TagJSON+1; tag++ {
		got := logicalTag(tag)
		if tag < TagRef || tag > TagJSON {
			if got != "unknown" {
				t.Errorf("logicalTag(%d) = %q", tag, got)
			}
		} else if got == "unknown" || got == "text_ref" || got == "bytes_ref" {
			t.Errorf("logicalTag(%d) = %q", tag, got)
		}
	}
}

func TestErrorKindTaxonomy(t *testing.T) {
	for _, kind := range []error{ErrNotFound, ErrConflict, ErrSchema, ErrType, ErrQuery, ErrFormat, ErrReadOnly, ErrTooLarge, ErrUnsupported} {
		err := wrap(kind, errors.New("cause"), "context")
		if got := ErrorKind(err); !errors.Is(got, kind) {
			t.Errorf("ErrorKind(%v) = %v, want %v", err, got, kind)
		}
	}
	if got := ErrorKind(errors.New("plain")); !errors.Is(got, ErrFormat) {
		t.Fatalf("plain ErrorKind = %v", got)
	}
}

func TestPublicEventRecordsAndTailRespectHistoricalBasis(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	first, err := db.Transact(ctx, E{"id": "timeline/item", "timeline/value": "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.Transact(ctx, E{"id": "timeline/item", "timeline/value": "second"})
	if err != nil {
		t.Fatal(err)
	}

	records, err := db.EventRecords(ctx, GenesisTx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0]["event"] != first.EventID || records[1]["event"] != second.EventID {
		t.Fatalf("event records = %#v", records)
	}
	throughFirst, err := db.EventRecords(ctx, GenesisTx, first.Tx)
	if err != nil {
		t.Fatal(err)
	}
	if len(throughFirst) != 1 || throughFirst[0]["event"] != first.EventID {
		t.Fatalf("event records through first = %#v", throughFirst)
	}
	afterFirst, err := db.EventRecords(ctx, first.Tx)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterFirst) != 1 || afterFirst[0]["event"] != second.EventID {
		t.Fatalf("event records after first = %#v", afterFirst)
	}

	historical, err := db.At(ctx, first.Tx)
	if err != nil {
		t.Fatal(err)
	}
	historicalRecords, err := historical.EventRecords(ctx, GenesisTx, second.Tx)
	if err != nil {
		t.Fatal(err)
	}
	if len(historicalRecords) != 1 || historicalRecords[0]["event"] != first.EventID {
		t.Fatalf("historical event records = %#v", historicalRecords)
	}
	var tail bytes.Buffer
	if err := historical.Tail(ctx, &tail, GenesisTx); err != nil {
		t.Fatal(err)
	}
	if strings.Count(tail.String(), "\n") != 1 || !strings.Contains(tail.String(), first.EventID) || strings.Contains(tail.String(), second.EventID) {
		t.Fatalf("historical tail = %q", tail.String())
	}
}

func TestPublicEventRecordsAndTailValidation(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")

	if _, err := db.EventRecords(ctx, GenesisTx-1); !errors.Is(err, ErrType) {
		t.Fatalf("invalid since error = %v", err)
	}
	if _, err := db.EventRecords(ctx, GenesisTx, GenesisTx-1); !errors.Is(err, ErrType) {
		t.Fatalf("invalid through error = %v", err)
	}
	if _, err := db.EventRecords(ctx, GenesisTx, GenesisTx, GenesisTx); !errors.Is(err, ErrType) {
		t.Fatalf("multiple through boundaries error = %v", err)
	}
	if records, err := db.EventRecords(ctx, GenesisTx+1); err != nil || len(records) != 0 {
		t.Fatalf("empty event range = %#v, %v", records, err)
	}
	if err := db.Tail(ctx, nil, GenesisTx); !errors.Is(err, ErrType) {
		t.Fatalf("nil tail writer error = %v", err)
	}

	closed, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := closed.EventRecords(ctx, GenesisTx); !errors.Is(err, ErrFormat) {
		t.Fatalf("closed event records error = %v", err)
	}
}

func TestEventRecordReceiptIntegrityBoundaries(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		kind   error
		mutate func(*testing.T, *DB, TxReport)
		name   string
	}{
		{name: "missing receipt", kind: ErrFormat, mutate: func(t *testing.T, db *DB, report TxReport) {
			if _, err := db.store.sql.ExecContext(ctx, "DELETE FROM fgraph_events WHERE tx=?", report.Tx); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "malformed hash", kind: ErrFormat, mutate: func(t *testing.T, db *DB, report TxReport) {
			if _, err := db.store.sql.ExecContext(ctx, "PRAGMA ignore_check_constraints=ON"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_events SET event_hash=? WHERE tx=?", []byte{1}, report.Tx); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "malformed event data", kind: ErrFormat, mutate: func(t *testing.T, db *DB, report TxReport) {
			if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_events SET event_data=? WHERE tx=?", "not-json", report.Tx); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "mismatched identity", kind: ErrFormat, mutate: func(t *testing.T, db *DB, report TxReport) {
			records, err := db.EventRecords(ctx, GenesisTx)
			if err != nil {
				t.Fatal(err)
			}
			records[0]["event"] = "11111111-1111-4111-8111-111111111111"
			data, digest, err := canonicalEventData(records[0])
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_events SET event_data=?,event_hash=? WHERE tx=?", data, digest[:], report.Tx); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			report, err := db.Transact(ctx, E{"id": "receipt/item", "receipt/value": "value"})
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, db, report)
			if _, err := db.eventRecordForTx(ctx, report.Tx); !errors.Is(err, test.kind) {
				t.Fatalf("event records error = %v", err)
			}
		})
	}

	canonical, digest, err := canonicalEventData(map[string]any{
		"fgraph": "event/1", "event": "11111111-1111-4111-8111-111111111111", "at": int64(1),
		"created": []any{}, "asserted": []any{}, "retracted": []any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		kind error
		data string
		hash []byte
	}{
		{data: strings.Repeat("x", maxPortableLineBytes+1), hash: digest[:], kind: ErrTooLarge},
		{data: "not-json", hash: digest[:], kind: ErrType},
		{data: "{}", hash: digest[:], kind: ErrFormat},
		{data: strings.Replace(canonical, `"fgraph":`, `"fgraph" :`, 1), hash: digest[:], kind: ErrFormat},
		{data: canonical, hash: make([]byte, 32), kind: ErrFormat},
	} {
		if _, err := decodeStoredEventData(test.data, test.hash); !errors.Is(err, test.kind) {
			t.Errorf("decodeStoredEventData(%q) error = %v", test.data[:min(len(test.data), 20)], err)
		}
	}
}

func TestEventRecordsAndWireSQLFaults(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("scripted event fault")
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
			if _, err := db.EventRecords(ctx, GenesisTx, GenesisTx); !errors.Is(err, ErrFormat) {
				t.Fatalf("EventRecords SQL fault = %v", err)
			}
		})
	}

	runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{
		{contains: "SELECT tx", columns: []string{"tx"}, rows: [][]driver.Value{{int64(65)}}},
		{contains: "SELECT v FROM", err: failure},
	}})
	db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
	if _, err := db.EventRecords(ctx, GenesisTx, int64(65)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("event materialization fault = %v", err)
	}

	for _, test := range []struct {
		name    string
		queries []scriptedQuery
	}{
		{name: "missing event uuid", queries: []scriptedQuery{{contains: "SELECT gid", err: failure}}},
		{name: "created query", queries: []scriptedQuery{
			{contains: "SELECT gid", columns: []string{"gid"}, rows: [][]driver.Value{{bytes.Repeat([]byte{1}, 16)}}},
			{contains: "SELECT name,gid", err: failure},
		}},
		{name: "created scan", queries: []scriptedQuery{
			{contains: "SELECT gid", columns: []string{"gid"}, rows: [][]driver.Value{{bytes.Repeat([]byte{1}, 16)}}},
			{contains: "SELECT name,gid", columns: []string{"name", "gid"}, rows: [][]driver.Value{{"name-only"}}},
		}},
		{name: "created iteration", queries: []scriptedQuery{
			{contains: "SELECT gid", columns: []string{"gid"}, rows: [][]driver.Value{{bytes.Repeat([]byte{1}, 16)}}},
			{contains: "SELECT name,gid", columns: []string{"name", "gid"}, nextErr: failure},
		}},
		{name: "created close", queries: []scriptedQuery{
			{contains: "SELECT gid", columns: []string{"gid"}, rows: [][]driver.Value{{bytes.Repeat([]byte{1}, 16)}}},
			{contains: "SELECT name,gid", columns: []string{"name", "gid"}, closeErr: failure},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: test.queries})
			db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
			if _, err := db.exportTransaction(ctx, runner, 65, 1); !errors.Is(err, ErrFormat) {
				t.Fatalf("exportTransaction SQL fault = %v", err)
			}
		})
	}
}

func TestEventIdentitySelectorIntegrity(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("identity lookup failed")
	for _, test := range []struct {
		kind error
		want any
		name string
		rule scriptedQuery
	}{
		{name: "query", kind: ErrFormat, rule: scriptedQuery{contains: "SELECT name,gid", err: failure}},
		{name: "malformed uuid", kind: ErrFormat, rule: scriptedQuery{contains: "SELECT name,gid", columns: []string{"name", "gid"}, rows: [][]driver.Value{{nil, []byte{1}}}}},
		{name: "named", want: "identity/name", rule: scriptedQuery{contains: "SELECT name,gid", columns: []string{"name", "gid"}, rows: [][]driver.Value{{"identity/name", nil}}}},
		{name: "anonymous", want: map[string]any{"eid": "01010101-0101-0101-0101-010101010101"}, rule: scriptedQuery{contains: "SELECT name,gid", columns: []string{"name", "gid"}, rows: [][]driver.Value{{nil, bytes.Repeat([]byte{1}, 16)}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			db := &DB{store: &store{sql: runner, names: map[string]int64{}}, exec: runner}
			got, err := db.identitySelector(ctx, runner, 65)
			if test.kind != nil {
				if !errors.Is(err, test.kind) {
					t.Fatalf("identitySelector error = %v", err)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, test.want) {
				t.Fatalf("identitySelector = %#v, %v", got, err)
			}
		})
	}
}
