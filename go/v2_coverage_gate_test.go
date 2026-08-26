package fgraph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type coverageWriter struct {
	failAt int
	calls  int
	short  bool
}

func (writer *coverageWriter) Write(value []byte) (int, error) {
	writer.calls++
	if writer.calls == writer.failAt {
		return 0, errors.New("coverage writer failure")
	}
	if writer.short && len(value) > 0 {
		return len(value) - 1, nil
	}
	return len(value), nil
}

func coverageEventRecord(suffix int) map[string]any {
	return map[string]any{
		"fgraph":    "event/1",
		"event":     fmt.Sprintf("00000000-0000-4000-8000-%012x", suffix),
		"at":        int64(1_767_225_601_000_000),
		"created":   []any{},
		"asserted":  []any{},
		"retracted": []any{},
	}
}

func coverageEventLine(t *testing.T, record map[string]any) string {
	t.Helper()
	encoded, err := canonicalJSON(record)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded) + "\n"
}

func coverageInt64(t *testing.T, value any) int64 {
	t.Helper()
	result, ok := value.(int64)
	if !ok {
		t.Fatalf("coverage fixture value has type %T, want int64", value)
	}
	return result
}

func coverageFactTuple(t *testing.T, wrapper map[string]any) []any {
	t.Helper()
	result, ok := wrapper["fact"].([]any)
	if !ok {
		t.Fatalf("coverage fixture fact has type %T, want []any", wrapper["fact"])
	}
	return result
}

func TestCoverageV2ApplyValidationAndSelectors(t *testing.T) {
	ctx := context.Background()
	if _, err := fixedDB(t, ":memory:").Apply(ctx, nil); !errors.Is(err, ErrType) {
		t.Fatalf("nil apply reader error = %v", err)
	}
	closed := fixedDB(t, ":memory:")
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := closed.Apply(ctx, strings.NewReader("")); err == nil {
		t.Fatal("closed database accepted apply")
	}
	if _, err := fixedDB(t, ":memory:").Apply(ctx, strings.NewReader(strings.Repeat("x", maxPortableLineBytes+1))); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized portable line error = %v", err)
	}

	tests := map[string]func(map[string]any) any{
		"non object":    func(map[string]any) any { return []any{} },
		"wrong kind":    func(record map[string]any) any { record["fgraph"] = "snapshot/1"; return record },
		"unknown field": func(record map[string]any) any { record["unknown"] = true; return record },
		"missing event": func(record map[string]any) any { delete(record, "event"); return record },
		"event type":    func(record map[string]any) any { record["event"] = int64(1); return record },
		"event UUID":    func(record map[string]any) any { record["event"] = "not-a-uuid"; return record },
		"event version": func(record map[string]any) any {
			record["event"] = "00000000-0000-0000-8000-000000000001"
			return record
		},
		"at type":        func(record map[string]any) any { record["at"] = "now"; return record },
		"at range":       func(record map[string]any) any { record["at"] = maxInstantMicros + 1; return record },
		"created type":   func(record map[string]any) any { record["created"] = true; return record },
		"asserted type":  func(record map[string]any) any { record["asserted"] = true; return record },
		"retracted type": func(record map[string]any) any { record["retracted"] = true; return record },
		"tuple type":     func(record map[string]any) any { record["asserted"] = []any{true}; return record },
		"tuple length":   func(record map[string]any) any { record["asserted"] = []any{[]any{"e"}}; return record },
		"attribute type": func(record map[string]any) any {
			record["asserted"] = []any{[]any{"e", true, "v", "text"}}
			return record
		},
		"tag type": func(record map[string]any) any {
			record["asserted"] = []any{[]any{"e", "a/b", "v", true}}
			return record
		},
		"selector type": func(record map[string]any) any {
			record["asserted"] = []any{[]any{true, "a/b", "v", "text"}}
			return record
		},
		"selector map": func(record map[string]any) any {
			record["asserted"] = []any{[]any{map[string]any{"bad": "x"}, "a/b", "v", "text"}}
			return record
		},
		"wire tag": func(record map[string]any) any {
			record["asserted"] = []any{[]any{"e", "a/b", "v", "bogus"}}
			return record
		},
		"ref wrapper": func(record map[string]any) any {
			record["asserted"] = []any{[]any{"e", "a/b", "target", "ref"}}
			return record
		},
		"ref selector": func(record map[string]any) any {
			record["asserted"] = []any{[]any{"e", "a/b", map[string]any{"ref": true}, "ref"}}
			return record
		},
		"tx facts type":     func(record map[string]any) any { record["tx_facts"] = true; return record },
		"tx fact type":      func(record map[string]any) any { record["tx_facts"] = []any{true}; return record },
		"tx fact length":    func(record map[string]any) any { record["tx_facts"] = []any{[]any{"a/b"}}; return record },
		"tx fact attribute": func(record map[string]any) any { record["tx_facts"] = []any{[]any{true, "v", "text"}}; return record },
		"tx fact tag":       func(record map[string]any) any { record["tx_facts"] = []any{[]any{"a/b", "v", true}}; return record },
		"tx fact value":     func(record map[string]any) any { record["tx_facts"] = []any{[]any{"a/b", "v", "bogus"}}; return record },
		"by type":           func(record map[string]any) any { record["by"] = true; return record },
		"source type":       func(record map[string]any) any { record["source"] = true; return record },
	}
	index := 1000
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			record := coverageEventRecord(index)
			index++
			value := mutate(record)
			encoded, err := canonicalJSON(value)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Apply(ctx, bytes.NewReader(append(encoded, '\n'))); err == nil {
				t.Fatalf("invalid event unexpectedly applied: %s", encoded)
			}
			basis, basisErr := db.latestTx(ctx)
			if basisErr != nil || basis != GenesisTx {
				t.Fatalf("failed apply was not atomic: basis=%d, %v", basis, basisErr)
			}
		})
	}

	source := fixedDB(t, ":memory:")
	sourceReport, transactErr := source.Transact(ctx, []any{
		E{"id": "import/name", "import/text": "value"},
		E{"id": Tmp("anonymous"), "import/ref": RefTo("import/name")},
	}, WithTxFacts(E{"audit/import": true}), WithBy("coverage"), WithSource("portable"), WithMeta(E{"test": true}))
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	var stream bytes.Buffer
	if err := source.Tail(ctx, &stream, GenesisTx); err != nil {
		t.Fatal(err)
	}
	db := fixedDB(t, ":memory:")
	reports, applyErr := db.Apply(ctx, strings.NewReader("\n"+stream.String()+"\n"))
	if applyErr != nil || len(reports) != 1 || reports[0].EventID != sourceReport.EventID {
		t.Fatalf("valid event apply = %#v, %v", reports, applyErr)
	}
	retry, retryErr := db.Apply(ctx, strings.NewReader(stream.String()))
	if retryErr != nil || len(retry) != 1 || retry[0].Status != "already_applied" {
		t.Fatalf("event retry = %#v, %v", retry, retryErr)
	}
	decoded, decodeErr := DecodeJSON(strings.NewReader(stream.String()))
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	record, ok := objectMap(decoded)
	if !ok {
		t.Fatalf("exported event is not an object: %#v", decoded)
	}
	record["meta"] = map[string]any{"test": false}
	if _, err := db.Apply(ctx, strings.NewReader(coverageEventLine(t, record))); !errors.Is(err, ErrConflict) {
		t.Fatalf("event collision error = %v", err)
	}

	createdID := reports[0].IDs["anonymous"]
	for name, value := range map[string]any{
		"not object": true,
		"wrong key":  map[string]any{"id": createdID},
		"eid type":   map[string]any{"eid": true},
		"eid UUID":   map[string]any{"eid": "bad"},
	} {
		t.Run("selector "+name, func(t *testing.T) {
			if _, err := selectorEID(value); err == nil {
				t.Fatalf("selector %#v unexpectedly accepted", value)
			}
		})
	}
	for name, value := range map[string]any{
		"not text": true,
		"short":    "00",
		"upper":    strings.Repeat("A", 64),
		"invalid":  strings.Repeat("z", 64),
	} {
		t.Run("hex "+name, func(t *testing.T) {
			if _, err := decodeLowerHex(value, 32, "digest"); err == nil {
				t.Fatalf("hex %#v unexpectedly accepted", value)
			}
		})
	}
}

func coverageSnapshotReceipt(suffix int) map[string]any {
	return map[string]any{"receipt": map[string]any{
		"event": fmt.Sprintf("00000000-0000-4000-8000-%012x", suffix),
		"at":    int64(1_767_225_601_000_000), "origin_at": int64(1_767_225_601_000_000),
		"event_hash": strings.Repeat("0", 64), "event_data": nil,
		"operation_id": nil, "request_hash": nil, "created": []any{},
	}}
}

func coverageReceiptFields(t *testing.T, wrapper map[string]any) map[string]any {
	t.Helper()
	receipt, ok := wrapper["receipt"].(map[string]any)
	if !ok {
		t.Fatalf("receipt is not an object: %#v", wrapper)
	}
	return receipt
}

func TestCoverageV2SnapshotBoundaries(t *testing.T) {
	ctx := context.Background()
	if err := fixedDB(t, ":memory:").Snapshot(ctx, nil); !errors.Is(err, ErrType) {
		t.Fatalf("nil snapshot writer error = %v", err)
	}
	if err := fixedDB(t, ":memory:").Snapshot(ctx, &coverageWriter{short: true}); !errors.Is(err, ErrFormat) {
		t.Fatalf("short snapshot write error = %v", err)
	}
	if err := fixedDB(t, ":memory:").Snapshot(ctx, &coverageWriter{failAt: 2}); !errors.Is(err, ErrFormat) {
		t.Fatalf("footer snapshot write error = %v", err)
	}
	if err := fixedDB(t, ":memory:").Restore(ctx, nil); !errors.Is(err, ErrType) {
		t.Fatalf("nil restore reader error = %v", err)
	}

	source := fixedDB(t, ":memory:")
	var valid bytes.Buffer
	if err := source.Snapshot(ctx, &valid); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(valid.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("pristine snapshot lines = %d", len(lines))
	}
	for name, stream := range map[string]string{
		"empty":          "",
		"invalid JSON":   "{\n",
		"non object":     "[]\n",
		"footer first":   lines[1] + "\n",
		"after footer":   valid.String() + "{}\n",
		"unknown record": lines[0] + "\n{}\n" + lines[1] + "\n",
		"header twice":   lines[0] + "\n" + lines[0] + "\n" + lines[1] + "\n",
		"no footer":      lines[0] + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			if err := db.Restore(ctx, strings.NewReader(stream)); err == nil {
				t.Fatalf("invalid snapshot unexpectedly restored: %q", stream)
			}
		})
	}

	nonPristine := fixedDB(t, ":memory:")
	if _, err := nonPristine.Transact(ctx, E{"id": "occupied", "item/value": 1}); err != nil {
		t.Fatal(err)
	}
	if err := nonPristine.Restore(ctx, strings.NewReader(valid.String())); !errors.Is(err, ErrConflict) {
		t.Fatalf("non-pristine restore error = %v", err)
	}
	speculative := fixedDB(t, ":memory:")
	if err := speculative.Speculate(ctx, func(view *DB) error {
		return view.Restore(ctx, strings.NewReader(valid.String()))
	}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("speculative restore error = %v", err)
	}

	db := fixedDB(t, ":memory:")
	if _, err := db.eventIDForTx(ctx, db.store.sql, 999_999); !errors.Is(err, ErrFormat) {
		t.Fatalf("unknown event identity error = %v", err)
	}
	if got := (&snapshotRestoreState{receiptN: 2, factN: 3, next: 70}).String(); got != "receipts=2 facts=3 next=70" {
		t.Fatalf("snapshot state string = %q", got)
	}
	for name, selector := range map[string]any{
		"invalid name": "",
		"invalid eid":  map[string]any{"eid": "bad"},
		"invalid form": true,
	} {
		t.Run("selector "+name, func(t *testing.T) {
			if _, err := snapshotSelectorKey(selector); err == nil {
				t.Fatalf("snapshot selector %#v unexpectedly accepted", selector)
			}
		})
	}
	if !exactKeys(map[string]any{"a": true, "b": true}, "a", "b") || exactKeys(map[string]any{"a": true}, "a", "b") || exactKeys(map[string]any{"a": true, "b": true}, "a", "c") {
		t.Fatal("exact snapshot key matcher is inconsistent")
	}
}

func TestCoverageV2SnapshotReceiptValidation(t *testing.T) {
	ctx := context.Background()
	tests := map[string]func(map[string]any){
		"wrapper extra":   func(wrapper map[string]any) { wrapper["extra"] = true },
		"receipt type":    func(wrapper map[string]any) { wrapper["receipt"] = true },
		"receipt extra":   func(wrapper map[string]any) { coverageReceiptFields(t, wrapper)["extra"] = true },
		"event type":      func(wrapper map[string]any) { coverageReceiptFields(t, wrapper)["event"] = true },
		"event UUID":      func(wrapper map[string]any) { coverageReceiptFields(t, wrapper)["event"] = "bad" },
		"duplicate event": func(wrapper map[string]any) { coverageReceiptFields(t, wrapper)["event"] = genesisEventID },
		"at type":         func(wrapper map[string]any) { coverageReceiptFields(t, wrapper)["at"] = "now" },
		"origin type":     func(wrapper map[string]any) { coverageReceiptFields(t, wrapper)["origin_at"] = "now" },
		"at range":        func(wrapper map[string]any) { coverageReceiptFields(t, wrapper)["at"] = maxInstantMicros + 1 },
		"origin range":    func(wrapper map[string]any) { coverageReceiptFields(t, wrapper)["origin_at"] = maxInstantMicros + 1 },
		"event hash":      func(wrapper map[string]any) { coverageReceiptFields(t, wrapper)["event_hash"] = "00" },
		"event data type": func(wrapper map[string]any) { coverageReceiptFields(t, wrapper)["event_data"] = true },
		"operation type": func(wrapper map[string]any) {
			receipt := coverageReceiptFields(t, wrapper)
			receipt["operation_id"], receipt["request_hash"] = true, strings.Repeat("0", 64)
		},
		"operation empty": func(wrapper map[string]any) {
			receipt := coverageReceiptFields(t, wrapper)
			receipt["operation_id"], receipt["request_hash"] = "", strings.Repeat("0", 64)
		},
		"request hash": func(wrapper map[string]any) {
			receipt := coverageReceiptFields(t, wrapper)
			receipt["operation_id"], receipt["request_hash"] = "coverage-operation", "00"
		},
		"created type":      func(wrapper map[string]any) { coverageReceiptFields(t, wrapper)["created"] = true },
		"existing identity": func(wrapper map[string]any) { coverageReceiptFields(t, wrapper)["created"] = []any{"fgraph/at"} },
		"repeated identity": func(wrapper map[string]any) {
			coverageReceiptFields(t, wrapper)["created"] = []any{"new/name", "new/name"}
		},
	}
	suffix := 3000
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			state, err := db.newSnapshotRestoreState(ctx, db.store.sql)
			if err != nil {
				t.Fatal(err)
			}
			wrapper := coverageSnapshotReceipt(suffix)
			suffix++
			mutate(wrapper)
			if err := db.restoreSnapshotReceipt(ctx, db.store.sql, state, wrapper); err == nil {
				t.Fatalf("invalid receipt unexpectedly restored: %#v", wrapper)
			}
		})
	}

	for name, mutate := range map[string]func(map[string]any, map[string]any){
		"hash mismatch": func(_, event map[string]any) { event["source"] = "changed" },
		"identity mismatch": func(receipt, event map[string]any) {
			receipt["event"] = "00000000-0000-4000-8000-000000000fa1"
		},
		"origin mismatch":  func(receipt, event map[string]any) { receipt["origin_at"] = coverageInt64(t, event["at"]) + 1 },
		"created missing":  func(_, event map[string]any) { event["created"] = true },
		"created mismatch": func(receipt, event map[string]any) { receipt["created"] = []any{"other/name"} },
	} {
		t.Run("event data "+name, func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			state, err := db.newSnapshotRestoreState(ctx, db.store.sql)
			if err != nil {
				t.Fatal(err)
			}
			wrapper := coverageSnapshotReceipt(suffix)
			suffix++
			receipt := coverageReceiptFields(t, wrapper)
			event := coverageEventRecord(suffix)
			receipt["event"] = event["event"]
			receipt["origin_at"] = event["at"]
			receipt["created"] = event["created"]
			_, digest, err := canonicalEventData(event)
			if err != nil {
				t.Fatal(err)
			}
			receipt["event_hash"] = hex.EncodeToString(digest[:])
			receipt["event_data"] = event
			mutate(receipt, event)
			if name != "hash mismatch" {
				_, digest, err = canonicalEventData(event)
				if err != nil {
					t.Fatal(err)
				}
				receipt["event_hash"] = hex.EncodeToString(digest[:])
			}
			if err := db.restoreSnapshotReceipt(ctx, db.store.sql, state, wrapper); err == nil {
				t.Fatalf("invalid event data unexpectedly restored: %#v", wrapper)
			}
		})
	}
}

func TestCoverageV2SnapshotFactAndFooterValidation(t *testing.T) {
	ctx := context.Background()
	state := &snapshotRestoreState{
		identities: map[string]int64{"name:item/entity": 65, "name:item/value": 66, "eid:00000000-0000-4000-8000-000000000111": 67},
		events:     map[string]int64{genesisEventID: GenesisTx, "00000000-0000-4000-8000-000000000222": 68},
	}
	db := fixedDB(t, ":memory:")
	base := func() map[string]any {
		return map[string]any{"fact": []any{"item/entity", "item/value", int64(1), "int", "00000000-0000-4000-8000-000000000222", nil}}
	}
	for name, mutate := range map[string]func(map[string]any){
		"wrapper extra": func(wrapper map[string]any) { wrapper["extra"] = true },
		"tuple type":    func(wrapper map[string]any) { wrapper["fact"] = true },
		"tuple length":  func(wrapper map[string]any) { wrapper["fact"] = []any{} },
		"entity":        func(wrapper map[string]any) { coverageFactTuple(t, wrapper)[0] = "missing/entity" },
		"attribute":     func(wrapper map[string]any) { coverageFactTuple(t, wrapper)[1] = "missing/attribute" },
		"attribute selector": func(wrapper map[string]any) {
			coverageFactTuple(t, wrapper)[1] = map[string]any{"eid": "00000000-0000-4000-8000-000000000111"}
		},
		"tag type":          func(wrapper map[string]any) { coverageFactTuple(t, wrapper)[3] = true },
		"assert event type": func(wrapper map[string]any) { coverageFactTuple(t, wrapper)[4] = true },
		"assert event unknown": func(wrapper map[string]any) {
			coverageFactTuple(t, wrapper)[4] = "00000000-0000-4000-8000-000000000333"
		},
		"assert event genesis": func(wrapper map[string]any) { coverageFactTuple(t, wrapper)[4] = genesisEventID },
		"value tag": func(wrapper map[string]any) {
			fact := coverageFactTuple(t, wrapper)
			fact[2], fact[3] = "x", "int"
		},
	} {
		t.Run(name, func(t *testing.T) {
			wrapper := base()
			mutate(wrapper)
			if err := db.restoreSnapshotFact(ctx, db.store.sql, state, wrapper); err == nil {
				t.Fatalf("invalid fact unexpectedly restored: %#v", wrapper)
			}
		})
	}
	for name, value := range map[string]any{
		"ref form":   map[string]any{"bad": "item/entity"},
		"ref target": map[string]any{"ref": "missing/entity"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.snapshotStoredValue(value, "ref", state); err == nil {
				t.Fatalf("invalid ref %#v unexpectedly stored", value)
			}
		})
	}
	if _, err := db.snapshotStoredValue(int64(1), "text", state); err == nil {
		t.Fatal("mismatched scalar tag unexpectedly stored")
	}
	if _, err := state.resolveSelector("missing/entity"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown restore selector error = %v", err)
	}

	digest := sha256.New()
	if _, err := digest.Write([]byte("body\n")); err != nil {
		t.Fatal(err)
	}
	validHash := hex.EncodeToString(digest.Sum(nil))
	validFooter := func() map[string]any {
		return map[string]any{"fgraph": "end", "sha256": validHash, "receipts": int64(2), "facts": int64(3)}
	}
	footerState := &snapshotRestoreState{receiptN: 2, factN: 3}
	for name, mutate := range map[string]func(map[string]any){
		"keys":          func(footer map[string]any) { footer["extra"] = true },
		"kind":          func(footer map[string]any) { footer["fgraph"] = "snapshot/1" },
		"hash form":     func(footer map[string]any) { footer["sha256"] = "00" },
		"hash mismatch": func(footer map[string]any) { footer["sha256"] = strings.Repeat("0", 64) },
		"receipt type":  func(footer map[string]any) { footer["receipts"] = "2" },
		"fact type":     func(footer map[string]any) { footer["facts"] = "3" },
		"receipt count": func(footer map[string]any) { footer["receipts"] = int64(1) },
		"fact count":    func(footer map[string]any) { footer["facts"] = int64(4) },
	} {
		t.Run("footer "+name, func(t *testing.T) {
			footer := validFooter()
			mutate(footer)
			if err := validateSnapshotFooter(footer, digest, footerState); err == nil {
				t.Fatalf("invalid footer unexpectedly accepted: %#v", footer)
			}
		})
	}
	if err := validateSnapshotFooter(validFooter(), digest, footerState); err != nil {
		t.Fatalf("valid footer rejected: %v", err)
	}
}

func TestCoverageV2DatomValidationAndCursors(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "datom/ref", Ref()); err != nil {
		t.Fatal(err)
	}
	first, firstErr := db.Transact(ctx, []any{
		E{"id": "datom/a", "datom/name": "alpha", "datom/ref": RefTo("datom/b")},
		E{"id": "datom/b", "datom/name": "beta"},
	})
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	second, secondErr := db.Transact(ctx, E{"id": "datom/a", "datom/name": "changed"})
	if secondErr != nil {
		t.Fatal(secondErr)
	}

	for name, value := range map[string]any{
		"int": int64(1), "float": float64(1.5), "text": "x", "blob": []byte{1, 2}, "invalid": true,
	} {
		t.Run("seek "+name, func(t *testing.T) {
			encoded := encodeDatomSeekValue(value)
			if name == "invalid" {
				if _, err := encoded.decode(); err == nil {
					t.Fatal("invalid seek value decoded")
				}
				return
			}
			if _, err := encoded.decode(); err != nil {
				t.Fatalf("seek value %#v: %v", encoded, err)
			}
		})
	}
	if _, err := (datomSeekValue{Kind: "blob", Text: "zz"}).decode(); !errors.Is(err, ErrType) {
		t.Fatalf("malformed blob seek error = %v", err)
	}
	seek := &datomSeek{E: 1, A: 2, T: 3, Tx: 4, Added: 1, ID: 5}
	for _, index := range []string{"eavt", "avet", "vaet"} {
		if got := seek.arguments(index, "v"); len(got) != 7 {
			t.Fatalf("%s seek arguments = %#v", index, got)
		}
	}

	for name, options := range map[string]DatomOptions{
		"index":      {Index: "bad"},
		"source":     {Source: "bad"},
		"components": {Components: []any{1, 2, 3, 4, 5, 6}},
		"limit low":  {Limit: -1},
		"limit high": {Limit: maxDatomPage + 1},
		"digest":     {Components: []any{math.NaN()}},
	} {
		t.Run("option "+name, func(t *testing.T) {
			if _, err := db.Datoms(ctx, options); err == nil {
				t.Fatalf("invalid datom options unexpectedly succeeded: %#v", options)
			}
		})
	}

	for name, options := range map[string]DatomOptions{
		"entity":           {Index: "eavt", Components: []any{true}},
		"attribute type":   {Index: "avet", Components: []any{true}},
		"attribute syntax": {Index: "avet", Components: []any{"bad"}},
		"value":            {Index: "avet", Components: []any{"datom/name", nil}},
		"vaet scalar":      {Index: "vaet", Components: []any{"datom/b"}},
		"tx":               {Index: "eavt", Components: []any{"datom/a", "datom/name", "changed", true}},
		"added":            {Index: "eavt", Components: []any{"datom/a", "datom/name", "changed", second.Tx, "yes"}},
	} {
		t.Run("component "+name, func(t *testing.T) {
			if _, err := db.Datoms(ctx, options); err == nil {
				t.Fatalf("invalid datom component unexpectedly succeeded: %#v", options)
			}
		})
	}
	if page, err := db.Datoms(ctx, DatomOptions{Index: "avet", Components: []any{"missing/attribute"}}); err != nil || len(page.Items) != 0 {
		t.Fatalf("missing attribute page = %#v, %v", page, err)
	}
	if page, err := db.Datoms(ctx, DatomOptions{Index: "eavt", Components: []any{"missing/entity"}}); err != nil || len(page.Items) != 0 {
		t.Fatalf("missing entity page = %#v, %v", page, err)
	}
	if page, err := db.Datoms(ctx, DatomOptions{Index: "vaet", Components: []any{RefTo("missing/ref")}}); err != nil || len(page.Items) != 0 {
		t.Fatalf("missing reference page = %#v, %v", page, err)
	}

	for name, options := range map[string]DatomOptions{
		"eavt current": {Index: "eavt", Components: []any{"datom/a", "datom/name"}, Limit: 1},
		"eavt history": {Index: "eavt", Source: "history", Components: []any{"datom/a", "datom/name"}, Limit: 1},
		"avet history": {Index: "avet", Source: "history", Components: []any{"datom/name"}, Limit: 1},
		"vaet current": {Index: "vaet", Components: []any{RefTo("datom/b"), "datom/ref"}, Limit: 1},
	} {
		t.Run(name, func(t *testing.T) {
			page, err := db.Datoms(ctx, options)
			if err != nil || len(page.Items) == 0 {
				t.Fatalf("first page = %#v, %v", page, err)
			}
			if page.NextCursor != "" {
				options.Cursor = page.NextCursor
				if _, err := db.Datoms(ctx, options); err != nil {
					t.Fatalf("cursor page: %v", err)
				}
			}
		})
	}
	if _, err := db.At(ctx, first.Tx); err != nil {
		t.Fatal(err)
	} else if historical, err := db.At(ctx, first.Tx); err != nil {
		t.Fatal(err)
	} else if page, err := historical.Datoms(ctx, DatomOptions{Index: "eavt", Components: []any{"datom/a"}}); err != nil || len(page.Items) == 0 {
		t.Fatalf("historical datoms = %#v, %v", page, err)
	}

	validCursor, cursorErr := encodeDatomCursor(datomCursor{
		Digest: "different", Index: "eavt", Source: "current", Basis: second.Tx,
		Format: FormatVersion, Seek: &datomSeek{V: datomSeekValue{Kind: "int"}},
	})
	if cursorErr != nil {
		t.Fatal(cursorErr)
	}
	if _, err := db.Datoms(ctx, DatomOptions{Cursor: validCursor}); !errors.Is(err, ErrConflict) {
		t.Fatalf("foreign cursor error = %v", err)
	}
	futureDigest, digestErr := datomArgumentsDigest(DatomOptions{Index: "eavt", Source: "current"})
	if digestErr != nil {
		t.Fatal(digestErr)
	}
	future, futureErr := encodeDatomCursor(datomCursor{
		Digest: futureDigest, Index: "eavt", Source: "current", Basis: second.Tx + 100,
		Format: FormatVersion, Seek: &datomSeek{V: datomSeekValue{Kind: "int"}},
	})
	if futureErr != nil {
		t.Fatal(futureErr)
	}
	if _, err := db.Datoms(ctx, DatomOptions{Cursor: future}); !errors.Is(err, ErrConflict) {
		t.Fatalf("future cursor error = %v", err)
	}
	badSeek, badSeekErr := encodeDatomCursor(datomCursor{
		Digest: futureDigest, Index: "eavt", Source: "current", Basis: second.Tx,
		Format: FormatVersion, Seek: &datomSeek{V: datomSeekValue{Kind: "invalid"}},
	})
	if badSeekErr != nil {
		t.Fatal(badSeekErr)
	}
	if _, err := db.Datoms(ctx, DatomOptions{Cursor: badSeek}); !errors.Is(err, ErrType) {
		t.Fatalf("bad seek cursor error = %v", err)
	}
	if _, err := encodeDatomCursor(datomCursor{Seek: &datomSeek{V: datomSeekValue{Kind: "float", Float: math.NaN()}}}); !errors.Is(err, ErrFormat) {
		t.Fatalf("unencodable cursor error = %v", err)
	}
	for name, raw := range map[string]string{
		"large":        strings.Repeat("x", maxDatomCursorBytes+1),
		"base64":       "!!",
		"unknown":      base64.RawURLEncoding.EncodeToString([]byte(`{"digest":"x","index":"eavt","source":"current","basis":64,"format":2,"seek":{},"extra":true}`)),
		"missing seek": base64.RawURLEncoding.EncodeToString([]byte(`{"digest":"x","index":"eavt","source":"current","basis":64,"format":2}`)),
		"format":       base64.RawURLEncoding.EncodeToString([]byte(`{"digest":"x","index":"eavt","source":"current","basis":64,"format":1,"seek":{}}`)),
		"basis":        base64.RawURLEncoding.EncodeToString([]byte(`{"digest":"x","index":"eavt","source":"current","basis":1,"format":2,"seek":{}}`)),
		"trailing":     base64.RawURLEncoding.EncodeToString([]byte(`{"digest":"x","index":"eavt","source":"current","basis":64,"format":2,"seek":{}}{}`)),
	} {
		t.Run("cursor "+name, func(t *testing.T) {
			if _, err := decodeDatomCursor(raw); err == nil {
				t.Fatalf("malformed cursor %q accepted", raw)
			}
		})
	}
}

func TestCoverageV2SchemaHelpersAndShapeDefinitions(t *testing.T) {
	ctx := context.Background()
	text := "text"
	boolean := true
	dims := int64(3)
	model := "embedding/v1"
	document := "doc"
	declaration := DeclaredAttribute{
		Type: &text, Many: &boolean, Unique: &boolean, NoHistory: &boolean,
		Dims: &dims, Doc: &document, VectorModel: &model,
	}
	if wire := declaredAttributeWire(declaration); len(wire) != 7 || wire["vector_model"] != model {
		t.Fatalf("declared wire = %#v", wire)
	}
	if wire := effectiveAttributeWire(effectiveAttribute(attributeSchema{
		typeName: "vector", many: true, unique: true, nohistory: true,
		dims: 3, dimsSet: true, vectorModel: model,
	}, declaration)); len(wire) != 7 || wire["dims"] != int64(3) || wire["doc"] != document {
		t.Fatalf("effective wire = %#v", wire)
	}
	if pointerValue((*string)(nil)) != nil || pointerValue(&text) != text {
		t.Fatal("pointer value conversion is inconsistent")
	}
	if got := stringValues([]string{"a", "b"}); len(got) != 2 || got[1] != "b" {
		t.Fatalf("string values = %#v", got)
	}

	var applied DeclaredAttribute
	for _, item := range []struct {
		value     any
		attribute int64
	}{
		{attribute: 5, value: true},
		{attribute: 6, value: false},
		{attribute: 7, value: true},
		{attribute: 8, value: "text"},
		{attribute: 9, value: int64(2)},
		{attribute: 10, value: "docs"},
		{attribute: 14, value: "model"},
	} {
		if err := applyDeclaredAttribute(&applied, item.attribute, item.value, "schema/value"); err != nil {
			t.Fatal(err)
		}
	}
	for name, item := range map[string]struct {
		value     any
		attribute int64
	}{
		"many": {attribute: 5, value: "yes"}, "unique": {attribute: 6, value: int64(1)},
		"nohistory": {attribute: 7, value: nil}, "type": {attribute: 8, value: true},
		"doc": {attribute: 10, value: int64(1)}, "model": {attribute: 14, value: []byte("x")},
		"dims": {attribute: 9, value: "2"},
	} {
		t.Run("declaration "+name, func(t *testing.T) {
			if err := applyDeclaredAttribute(&DeclaredAttribute{}, item.attribute, item.value, "schema/value"); !errors.Is(err, ErrFormat) {
				t.Fatalf("invalid declaration error = %v", err)
			}
		})
	}
	if got := sortedUniqueStrings([]string{"b", "a", "b"}); strings.Join(got, ",") != "a,b" {
		t.Fatalf("sorted unique values = %v", got)
	}
	if _, err := normalizeShapeAttributes("required", []string{"bad"}); !errors.Is(err, ErrSchema) {
		t.Fatalf("invalid shape attribute error = %v", err)
	}

	db := fixedDB(t, ":memory:")
	if _, err := db.DeclareShape(ctx, "", ShapeDefinition{}); err == nil {
		t.Fatal("invalid shape name unexpectedly accepted")
	}
	if _, err := db.DeclareShape(ctx, "shape/invalid", ShapeDefinition{Required: []string{"bad"}}); !errors.Is(err, ErrSchema) {
		t.Fatalf("invalid required attribute error = %v", err)
	}
	if _, err := db.Validate(ctx, true); !errors.Is(err, ErrType) {
		t.Fatalf("invalid validation selector error = %v", err)
	}
	if _, err := db.Validate(ctx, "missing/entity"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing validation entity error = %v", err)
	}
	if _, err := db.Declare(ctx, "shape/required", Type("text")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "shape/broken"}); err != nil {
		t.Fatal(err)
	}
	entityReport, transactErr := db.Transact(ctx, E{"id": "shape/member", "shape/required": "present"})
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	var shapeID, entityID, attributeID int64
	for name, destination := range map[string]*int64{
		"shape/broken": &shapeID, "shape/member": &entityID, "shape/required": &attributeID,
	} {
		if err := db.store.sql.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE name=?", name).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.store.sql.ExecContext(ctx, `INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES
		(?,16,?,0,?,NULL),(?,18,1,1,?,NULL),(?,15,?,0,?,NULL)`,
		shapeID, attributeID, entityReport.Tx, shapeID, entityReport.Tx,
		entityID, shapeID, entityReport.Tx); err != nil {
		t.Fatal(err)
	}
	report, validateErr := db.Validate(ctx, "shape/member")
	if validateErr != nil || report.Valid {
		t.Fatalf("broken shape validation = %#v, %v", report, validateErr)
	}
	foundDefinition := false
	for _, violation := range report.Violations {
		if violation.Code == "shape_definition" {
			foundDefinition = true
		}
	}
	if !foundDefinition {
		t.Fatalf("shape definition violation missing: %#v", report.Violations)
	}
	if shapes, err := db.readShapes(ctx, db.store.sql); err != nil || len(shapes) == 0 {
		t.Fatalf("read shapes = %#v, %v", shapes, err)
	}
	if identities, err := db.schemaIdentities(ctx, db.store.sql, entityReport.Tx, "shape/", false); err != nil || len(identities) < 3 {
		t.Fatalf("schema identities = %#v, %v", identities, err)
	}
	if snapshot, err := db.Schema(ctx, "fgraph/", true); err != nil || len(snapshot.Attributes) != 18 {
		t.Fatalf("system schema = %#v, %v", snapshot, err)
	}

	corrupt := fixedDB(t, ":memory:")
	bad, corruptErr := corrupt.Transact(ctx, E{"id": "corrupt/entity", "corrupt/value": "x"})
	if corruptErr != nil {
		t.Fatal(corruptErr)
	}
	coveragePublicCorruptTag(t, corrupt, coveragePublicFactID(t, bad, "corrupt/value"))
	var corruptAttribute int64
	if err := corrupt.store.sql.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE name='corrupt/value'").Scan(&corruptAttribute); err != nil {
		t.Fatal(err)
	}
	if _, err := corrupt.schemaObservation(ctx, corrupt.store.sql, corruptAttribute, "corrupt/value"); !errors.Is(err, ErrFormat) {
		t.Fatalf("corrupt schema observation error = %v", err)
	}

	malformed := fixedDB(t, ":memory:")
	shape, shapeErr := malformed.Transact(ctx, E{"id": "shape/malformed"})
	if shapeErr != nil {
		t.Fatal(shapeErr)
	}
	latest := shape.Tx
	if err := malformed.store.sql.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE name='shape/malformed'").Scan(&shapeID); err != nil {
		t.Fatal(err)
	}
	if _, err := malformed.store.sql.ExecContext(ctx, "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (?,16,'bad',3,?,NULL)", shapeID, latest); err != nil {
		t.Fatal(err)
	}
	if _, err := malformed.readShape(ctx, malformed.store.sql, shapeID); !errors.Is(err, ErrFormat) {
		t.Fatalf("malformed shape error = %v", err)
	}
}

func TestCoverageV2SearchBoundsAndFilterHelpers(t *testing.T) {
	ctx := context.Background()
	for name, vector := range map[string][]float32{
		"empty": {}, "zero": {0, 0}, "nan": {float32(math.NaN())}, "infinity": {float32(math.Inf(1))},
	} {
		t.Run("vector "+name, func(t *testing.T) {
			if err := validateCosineVector(vector); err == nil {
				t.Fatalf("invalid cosine vector %#v accepted", vector)
			}
		})
	}
	if err := validateCosineVector([]float32{1, 0}); err != nil {
		t.Fatal(err)
	}
	if (&searchWork{limit: 0}).remaining() != 0 {
		t.Fatal("negative search work remained")
	}
	work := &searchWork{limit: 1}
	if err := work.spend(); err != nil {
		t.Fatal(err)
	}
	if err := work.spend(); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("exhausted search work error = %v", err)
	}

	longText := strings.Repeat("x", maxMatchedValueBytes-1) + "é-tail"
	bounded := boundMatchedFact(Fact{V: longText, Tag: TagText})
	textValue, ok := bounded.V.(string)
	if !ok || !bounded.ValueTruncated || !utf8.ValidString(textValue) || !strings.HasSuffix(textValue, "…") {
		t.Fatalf("bounded matched text = %#v", bounded)
	}
	for name, vector := range map[string]any{
		"any": []any{1, 2}, "float32": []float32{1, 2, 3}, "float64": []float64{1}, "missing": true,
	} {
		t.Run("matched vector "+name, func(t *testing.T) {
			fact := boundMatchedFact(Fact{V: map[string]any{"vector": vector}, Tag: TagVector})
			if !fact.ValueTruncated {
				t.Fatalf("vector fact was not bounded: %#v", fact)
			}
		})
	}
	if fact := boundMatchedFact(Fact{V: int64(1), Tag: TagInt}); fact.ValueTruncated {
		t.Fatalf("scalar fact was truncated: %#v", fact)
	}

	expanded := SearchResult{Hits: []SearchHit{{Entity: "keep"}}, Expanded: []SearchHit{{Pull: map[string]any{"large": strings.Repeat("x", maxSearchOutputBytes)}}}}
	boundSearchOutput(&expanded)
	if !expanded.Truncated || len(expanded.Expanded) != 0 || len(expanded.Hits) != 1 {
		t.Fatalf("expanded output bound = %#v", expanded)
	}
	matched := SearchResult{Hits: []SearchHit{{Entity: "keep", Matched: []Fact{{V: strings.Repeat("x", maxSearchOutputBytes)}}}}}
	boundSearchOutput(&matched)
	if !matched.Truncated || len(matched.Hits) != 1 || matched.Hits[0].Matched != nil {
		t.Fatalf("matched output bound = %#v", matched)
	}
	pulled := SearchResult{Hits: []SearchHit{{Entity: "drop", Pull: map[string]any{"large": strings.Repeat("x", maxSearchOutputBytes)}}}}
	boundSearchOutput(&pulled)
	if !pulled.Truncated || len(pulled.Hits) != 0 {
		t.Fatalf("tail hit output bound = %#v", pulled)
	}
	small := SearchResult{Hits: []SearchHit{{Entity: "small"}}}
	boundSearchOutput(&small)
	if small.Truncated {
		t.Fatalf("small output was truncated: %#v", small)
	}

	ranked := []rankedRawFact{
		{entity: 1, score: 0.5, raw: rawFact{id: 2}},
		{entity: 1, score: 0.5, raw: rawFact{id: 1}},
		{entity: 2, score: 0.4, raw: rawFact{id: 3}},
	}
	if result, _ := rankRawEntityCandidatesBounded(ranked, 1); len(result) != 1 || result[0].raw.id != 1 {
		t.Fatalf("ranked entity candidates = %#v", result)
	}

	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "search/vector", Type("vector"), Dims(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "search/tag", Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "search/a", "search/text": "alpha needle", "search/vector": Vector([]float32{1, 0}), "search/tag": []any{"one", "shared"}},
		E{"id": "search/b", "search/text": "beta needle", "search/vector": Vector([]float32{0, 1}), "search/tag": []any{"two", "shared"}},
	}); err != nil {
		t.Fatal(err)
	}
	if result, err := db.Search(ctx, SearchOpts{Text: "needle", Filters: [][]any{{"search/tag", "one"}}, K: 1}); err != nil || len(result.Hits) != 1 || result.Hits[0].Entity != "search/a" {
		t.Fatalf("filter-correct keyword search = %#v, %v", result, err)
	}
	if result, err := db.Search(ctx, SearchOpts{Vector: []float32{1, 0}, VectorAttribute: "search/vector", Filters: [][]any{{"search/tag", "shared"}}, K: 2}); err != nil || len(result.Hits) != 2 {
		t.Fatalf("filtered vector search = %#v, %v", result, err)
	}
	if result, err := db.Search(ctx, SearchOpts{Text: "needle", Filters: [][]any{{"missing/filter", true}}}); err != nil || len(result.Hits) != 0 {
		t.Fatalf("impossible search filter = %#v, %v", result, err)
	}

	tooManyFilters := make([][]any, maxSearchFilters+1)
	tooManyAttributes := make([]string, maxSearchAttributes+1)
	for name, options := range map[string]SearchOpts{
		"text attributes without text":    {Vector: []float32{1, 0}, VectorAttribute: "search/vector", TextAttributes: []string{"search/text"}},
		"vector attribute without vector": {Text: "needle", VectorAttribute: "search/vector"},
		"filters":                         {Text: "needle", Filters: tooManyFilters},
		"attributes":                      {Text: "needle", TextAttributes: tooManyAttributes},
		"k low":                           {Text: "needle", K: -1}, "k high": {Text: "needle", K: maxSearchK + 1},
		"expand low": {Text: "needle", Expand: -1}, "expand high": {Text: "needle", Expand: maxSearchExpand + 1},
	} {
		t.Run("option "+name, func(t *testing.T) {
			if _, err := db.Search(ctx, options); err == nil {
				t.Fatalf("invalid search options unexpectedly succeeded: %#v", options)
			}
		})
	}
}

func TestCoverageV2MaintenanceWireAndValueHelpers(t *testing.T) {
	validRedaction := func() map[string]any {
		return map[string]any{
			"fgraph": "event/1", "event": "00000000-0000-4000-8000-000000000444",
			"at": int64(1_767_225_601_000_000), "created": []any{}, "asserted": []any{}, "retracted": []any{},
			"redacted": true, "redacts": []any{
				"00000000-0000-4000-8000-000000000111",
				"00000000-0000-4000-8000-000000000222",
			},
		}
	}
	if targets, ok := validateExcisionEventRecord(validRedaction()); !ok || len(targets) != 2 {
		t.Fatalf("valid redaction = %v, %t", targets, ok)
	}
	for name, mutate := range map[string]func(map[string]any){
		"extra":              func(record map[string]any) { record["extra"] = true },
		"kind":               func(record map[string]any) { record["fgraph"] = "event/2" },
		"flag":               func(record map[string]any) { record["redacted"] = false },
		"created type":       func(record map[string]any) { record["created"] = true },
		"created nonempty":   func(record map[string]any) { record["created"] = []any{"x/y"} },
		"asserted type":      func(record map[string]any) { record["asserted"] = true },
		"retracted nonempty": func(record map[string]any) { record["retracted"] = []any{true} },
		"redacts type":       func(record map[string]any) { record["redacts"] = true },
		"target type":        func(record map[string]any) { record["redacts"] = []any{true} },
		"target UUID":        func(record map[string]any) { record["redacts"] = []any{"bad"} },
		"target duplicate": func(record map[string]any) {
			record["redacts"] = []any{"00000000-0000-4000-8000-000000000111", "00000000-0000-4000-8000-000000000111"}
		},
		"target order": func(record map[string]any) {
			record["redacts"] = []any{"00000000-0000-4000-8000-000000000222", "00000000-0000-4000-8000-000000000111"}
		},
	} {
		t.Run("redaction "+name, func(t *testing.T) {
			record := validRedaction()
			mutate(record)
			if _, ok := validateExcisionEventRecord(record); ok {
				t.Fatalf("invalid redaction accepted: %#v", record)
			}
		})
	}

	target, ok := eventSelectorKey("target/name")
	if !ok {
		t.Fatal("target selector did not canonicalize")
	}
	if _, ok := eventSelectorKey(math.NaN()); ok {
		t.Fatal("non-finite selector canonicalized")
	}
	if !eventValueReferencesSelector(map[string]any{"ref": "target/name"}, target) || eventValueReferencesSelector("target/name", target) {
		t.Fatal("event reference matching is inconsistent")
	}
	baseEvent := func() map[string]any {
		return map[string]any{"created": []any{}, "asserted": []any{}, "retracted": []any{}}
	}
	for name, mutate := range map[string]func(map[string]any){
		"created type":   func(record map[string]any) { record["created"] = true },
		"created match":  func(record map[string]any) { record["created"] = []any{"target/name"} },
		"asserted type":  func(record map[string]any) { record["asserted"] = true },
		"asserted tuple": func(record map[string]any) { record["asserted"] = []any{true} },
		"entity match": func(record map[string]any) {
			record["asserted"] = []any{[]any{"target/name", "item/value", "x", "text"}}
		},
		"attribute match": func(record map[string]any) {
			record["asserted"] = []any{[]any{"item/entity", "target/name", "x", "text"}}
		},
		"value match": func(record map[string]any) {
			record["asserted"] = []any{[]any{"item/entity", "item/ref", map[string]any{"ref": "target/name"}, "ref"}}
		},
		"retracted type":     func(record map[string]any) { record["retracted"] = true },
		"tx facts type":      func(record map[string]any) { record["tx_facts"] = true },
		"tx fact tuple":      func(record map[string]any) { record["tx_facts"] = []any{true} },
		"tx attribute match": func(record map[string]any) { record["tx_facts"] = []any{[]any{"target/name", "x", "text"}} },
		"tx value match": func(record map[string]any) {
			record["tx_facts"] = []any{[]any{"item/ref", map[string]any{"ref": "target/name"}, "ref"}}
		},
	} {
		t.Run("event reference "+name, func(t *testing.T) {
			record := baseEvent()
			mutate(record)
			matched, err := eventRecordReferencesSelector(record, target)
			if strings.Contains(name, "type") || strings.Contains(name, "tuple") {
				if err == nil {
					t.Fatalf("malformed event reference unexpectedly accepted: %#v", record)
				}
				return
			}
			if err != nil || !matched {
				t.Fatalf("event reference %s = %t, %v", name, matched, err)
			}
		})
	}
	if matched, err := eventRecordReferencesSelector(baseEvent(), target); err != nil || matched {
		t.Fatalf("unrelated event reference = %t, %v", matched, err)
	}

	validJSON := []byte(`{"a":1}`)
	for name, item := range map[string]struct {
		scalar  any
		class   string
		raw     []byte
		tag     Tag
		expects bool
	}{
		"ref":               {tag: TagRef, class: "integer", scalar: int64(1), raw: []byte("1"), expects: true},
		"ref zero":          {tag: TagRef, class: "integer", scalar: int64(0), raw: []byte("0")},
		"bool":              {tag: TagBool, class: "integer", scalar: int64(1), raw: []byte("1"), expects: true},
		"bool invalid":      {tag: TagBool, class: "integer", scalar: int64(2), raw: []byte("2")},
		"int":               {tag: TagInt, class: "integer", scalar: int64(1), raw: []byte("1"), expects: true},
		"int class":         {tag: TagInt, class: "text", scalar: int64(1), raw: []byte("1")},
		"float":             {tag: TagFloat, class: "real", scalar: float64(1), raw: []byte("1.0"), expects: true},
		"float nan":         {tag: TagFloat, class: "real", scalar: math.NaN()},
		"text":              {tag: TagText, class: "text", scalar: "x", raw: []byte("x"), expects: true},
		"text utf8":         {tag: TagText, class: "text", scalar: "x", raw: []byte{0xff}},
		"instant":           {tag: TagInstant, class: "integer", scalar: minInstantMicros, expects: true},
		"instant range":     {tag: TagInstant, class: "integer", scalar: minInstantMicros - 1},
		"bytes":             {tag: TagBytes, class: "blob", scalar: []byte{1}, raw: []byte{1}, expects: true},
		"vector indirect":   {tag: TagVector, class: "blob", expects: true},
		"text indirect":     {tag: TagTextRef, class: "blob", expects: true},
		"bytes indirect":    {tag: TagBytesRef, class: "blob", expects: true},
		"json":              {tag: TagJSON, class: "text", scalar: string(validJSON), raw: validJSON, expects: true},
		"json noncanonical": {tag: TagJSON, class: "text", scalar: "", raw: []byte(`{"a": 1}`)},
		"json class":        {tag: TagJSON, class: "blob", raw: validJSON},
		"unknown":           {tag: Tag(99), class: "text", scalar: "x", raw: []byte("x")},
	} {
		t.Run("physical "+name, func(t *testing.T) {
			if got := validPhysicalValue(item.tag, item.class, item.scalar, item.raw); got != item.expects {
				t.Fatalf("physical value validity = %t, want %t", got, item.expects)
			}
		})
	}

	largeText := strings.Repeat("x", BlobThreshold+1)
	largeBytes := bytes.Repeat([]byte{1}, BlobThreshold+1)
	vectorBytes := []byte{0, 0, 0, 0}
	for name, item := range map[string]struct {
		data    any
		key     any
		tag     Tag
		expects bool
	}{
		"text": {tag: TagTextRef, data: largeText, key: func() []byte {
			digest := indirectDigest(TagTextRef, []byte(largeText))
			return digest[:]
		}(), expects: true},
		"bytes": {tag: TagBytesRef, data: largeBytes, key: func() []byte {
			digest := indirectDigest(TagBytesRef, largeBytes)
			return digest[:]
		}(), expects: true},
		"vector": {tag: TagVector, data: vectorBytes, key: func() []byte {
			digest := indirectDigest(TagVector, vectorBytes)
			return digest[:]
		}(), expects: true},
		"key type":     {tag: TagVector, data: vectorBytes, key: "bad"},
		"key length":   {tag: TagVector, data: vectorBytes, key: []byte{1}},
		"text type":    {tag: TagTextRef, data: true, key: make([]byte, 32)},
		"text short":   {tag: TagTextRef, data: "x", key: make([]byte, 32)},
		"bytes type":   {tag: TagBytesRef, data: true, key: make([]byte, 32)},
		"bytes short":  {tag: TagBytesRef, data: []byte{1}, key: make([]byte, 32)},
		"vector type":  {tag: TagVector, data: true, key: make([]byte, 32)},
		"vector empty": {tag: TagVector, data: []byte{}, key: make([]byte, 32)},
		"vector width": {tag: TagVector, data: []byte{1}, key: make([]byte, 32)},
		"tag":          {tag: TagInt, data: largeBytes, key: make([]byte, 32)},
		"digest":       {tag: TagVector, data: vectorBytes, key: make([]byte, 32)},
	} {
		t.Run("indirect "+name, func(t *testing.T) {
			if got := validIndirectBlob(item.tag, item.key, item.data); got != item.expects {
				t.Fatalf("indirect blob validity = %t, want %t", got, item.expects)
			}
		})
	}

	if !matchesGenesisFact(genesisFactValue{e: 1, a: 8, value: []byte("instant"), storageClass: "text", tag: int64(TagText), tx: GenesisTx}, 1, 8, "instant") {
		t.Fatal("valid genesis fact did not match")
	}
	if matchesGenesisFact(genesisFactValue{}, 1, 8, "instant") {
		t.Fatal("invalid genesis fact matched")
	}
	if doctorValue(nil) != "None" || doctorValue("x") != `"x"` || doctorValue(int64(2)) != "2" {
		t.Fatal("doctor value rendering is inconsistent")
	}
	if !equalFTSRows([]ftsRow{{id: 1, text: "x"}}, []ftsRow{{id: 1, text: "x"}}) || equalFTSRows(nil, []ftsRow{{}}) || equalFTSRows([]ftsRow{{id: 1}}, []ftsRow{{id: 2}}) {
		t.Fatal("FTS row equality is inconsistent")
	}
}

func TestCoverageV2EventHistoryPolicy(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "history/kept", Type("text")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "history/lost", Type("text"), NoHistory()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "history/vector", Type("vector"), Dims(2)); err != nil {
		t.Fatal(err)
	}
	err := db.withRead(ctx, func(runner sqlRunner) error {
		for name, item := range map[string]struct {
			record map[string]any
			want   bool
			bad    bool
		}{
			"kept":    {map[string]any{"asserted": []any{[]any{"e", "history/kept", "v", "text"}}, "retracted": []any{}}, false, false},
			"lost":    {map[string]any{"asserted": []any{[]any{"e", "history/lost", "v", "text"}}, "retracted": []any{}}, true, false},
			"vector":  {map[string]any{"asserted": []any{}, "retracted": []any{}, "tx_facts": []any{[]any{"history/vector", []any{1, 0}, "vector"}}}, true, false},
			"missing": {map[string]any{"asserted": []any{[]any{"e", "history/missing", "v", "text"}}, "retracted": []any{}}, false, true},
		} {
			t.Run(name, func(t *testing.T) {
				got, historyErr := db.eventMayLoseHistory(ctx, runner, item.record)
				if item.bad {
					if historyErr == nil {
						t.Fatal("missing history attribute unexpectedly succeeded")
					}
					return
				}
				if historyErr != nil || got != item.want {
					t.Fatalf("history policy = %t, %v, want %t", got, historyErr, item.want)
				}
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCoverageV2MCPResources(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	wide := E{"id": "wide-resource"}
	txFacts := E{}
	for index := range 105 {
		wide[fmt.Sprintf("resource/a%03d", index)] = index
		txFacts[fmt.Sprintf("receipt/a%03d", index)] = index
	}
	wideReport, transactErr := db.Transact(ctx, wide, WithTxFacts(txFacts))
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	for index := range 101 {
		if _, err := db.Transact(ctx, E{"id": fmt.Sprintf("change-%03d", index), "change/value": index}); err != nil {
			t.Fatal(err)
		}
	}

	server := NewMCPServer(db, MCPOptions{})
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, serverErr := server.Connect(ctx, serverTransport, nil)
	if serverErr != nil {
		t.Fatal(serverErr)
	}
	t.Cleanup(func() {
		if closeErr := serverSession.Close(); closeErr != nil {
			t.Errorf("close MCP server session: %v", closeErr)
		}
	})
	client := mcp.NewClient(&mcp.Implementation{Name: "coverage-resources", Version: "1"}, nil)
	clientSession, clientErr := client.Connect(ctx, clientTransport, nil)
	if clientErr != nil {
		t.Fatal(clientErr)
	}
	t.Cleanup(func() {
		if closeErr := clientSession.Close(); closeErr != nil {
			t.Errorf("close MCP client session: %v", closeErr)
		}
	})

	read := func(uri string, destination any) error {
		t.Helper()
		result, readErr := clientSession.ReadResource(ctx, &mcp.ReadResourceParams{URI: uri})
		if readErr != nil {
			return readErr
		}
		if len(result.Contents) != 1 {
			t.Fatalf("resource %q contents = %#v", uri, result.Contents)
		}
		return json.Unmarshal([]byte(result.Contents[0].Text), destination)
	}

	var entityPage struct {
		NextURI string  `json:"next_uri"`
		Items   []Datom `json:"items"`
	}
	if err := read("fgraph://entity/wide-resource", &entityPage); err != nil {
		t.Fatal(err)
	}
	if len(entityPage.Items) != 100 || entityPage.NextURI == "" {
		t.Fatalf("wide entity first page = %#v", entityPage)
	}
	var entityTail struct {
		NextURI string  `json:"next_uri"`
		Items   []Datom `json:"items"`
	}
	if err := read(entityPage.NextURI, &entityTail); err != nil {
		t.Fatal(err)
	}
	if len(entityTail.Items) != 5 || entityTail.NextURI != "" {
		t.Fatalf("wide entity second page = %#v", entityTail)
	}
	if err := read("fgraph://entity/wide-resource?at=not-a-tx", &entityTail); err == nil {
		t.Fatalf("invalid entity basis error = %v", err)
	}
	if err := read(fmt.Sprintf("fgraph://entity/wide-resource?at=%d", wideReport.Tx), &entityTail); err != nil {
		t.Fatalf("historical entity resource: %v", err)
	}

	var receipt struct {
		EventReceipt
		Truncated bool `json:"truncated"`
	}
	if err := read(fmt.Sprintf("fgraph://tx/%d", wideReport.Tx), &receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.Truncated || len(receipt.Facts) != 100 {
		t.Fatalf("wide receipt = %#v", receipt)
	}
	if err := read("fgraph://tx/"+url.PathEscape(wideReport.EventID), &receipt); err != nil {
		t.Fatalf("event UUID receipt: %v", err)
	}
	if err := read("fgraph://tx/not-found", &receipt); err == nil {
		t.Fatal("unknown transaction resource unexpectedly succeeded")
	}
	if err := read("fgraph://tx/not-an-id", &receipt); err == nil {
		t.Fatal("invalid transaction resource unexpectedly succeeded")
	}

	var changes struct {
		NextURI string           `json:"next_uri"`
		Events  []map[string]any `json:"events"`
	}
	if err := read("fgraph://changes?since=64", &changes); err != nil {
		t.Fatal(err)
	}
	if len(changes.Events) != 100 {
		t.Fatalf("change first page events = %d", len(changes.Events))
	}
	firstAssertions, assertionsOK := changes.Events[0]["asserted"].([]any)
	if changes.NextURI == "" || changes.Events[0]["fgraph"] != "event/1" || !assertionsOK || len(firstAssertions) != 105 {
		t.Fatalf("change first page = events:%d next:%q first:%#v", len(changes.Events), changes.NextURI, changes.Events[0])
	}
	for _, local := range []string{"tx", "status", "basis_tx", "truncated"} {
		if _, exists := changes.Events[0][local]; exists {
			t.Fatalf("portable change event exposed local field %q: %#v", local, changes.Events[0])
		}
	}
	var changesTail struct {
		NextURI string           `json:"next_uri"`
		Events  []map[string]any `json:"events"`
	}
	if err := read(changes.NextURI, &changesTail); err != nil {
		t.Fatal(err)
	}
	if len(changesTail.Events) != 2 || changesTail.NextURI != "" {
		t.Fatalf("change second page = %#v", changesTail)
	}
	if err := read("fgraph://changes?since=-1", &changesTail); err == nil {
		t.Fatalf("negative since error = %v", err)
	}
	if err := read("fgraph://changes?since=63", &changesTail); err == nil {
		t.Fatalf("pre-genesis since error = %v", err)
	}
	if err := read("fgraph://changes?since=nope", &changesTail); err == nil {
		t.Fatalf("invalid since error = %v", err)
	}

	snapshot, schemaErr := db.Schema(ctx, "resource/", false)
	if schemaErr != nil {
		t.Fatal(schemaErr)
	}
	for name, cursor := range map[string]mcpResourceCursor{
		"outside": {Version: 1, Resource: "schema", Argument: "resource/", Basis: snapshot.BasisTx, Offset: 999, Digest: snapshot.Digest},
		"digest":  {Version: 1, Resource: "schema", Argument: "resource/", Basis: snapshot.BasisTx, Offset: 1, Digest: "sha256:" + strings.Repeat("0", 64)},
	} {
		t.Run("schema "+name, func(t *testing.T) {
			raw, encodeErr := encodeMCPResourceCursor(cursor)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			if err := read("fgraph://schema?prefix=resource%2F&cursor="+url.QueryEscape(raw), &entityTail); err == nil {
				t.Fatalf("schema %s cursor error = %v", name, err)
			}
		})
	}
}

func TestCoverageV2MCPResourceHelpers(t *testing.T) {
	if _, err := parseMCPResourceURI(nil); !errors.Is(err, ErrType) {
		t.Fatalf("nil resource request error = %v", err)
	}
	if _, err := parseMCPResourceURI(&mcp.ReadResourceRequest{Params: &mcp.ReadResourceParams{URI: "https://example.test"}}); err == nil {
		t.Fatal("foreign resource URI unexpectedly accepted")
	}
	if got := mcpEntitySelector("00000000-0000-4000-8000-000000000040"); fmt.Sprint(got) == "00000000-0000-4000-8000-000000000040" {
		t.Fatalf("UUID selector was not wrapped: %#v", got)
	}
	if got := mcpEntitySelector("named"); got != "named" {
		t.Fatalf("named selector = %#v", got)
	}
	if _, err := mcpJSONResource("fgraph://oversized", strings.Repeat("x", MaxMCPOutputBytes)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized resource error = %v", err)
	}

	valid := mcpResourceCursor{Version: 1, Resource: "schema", Argument: "x", Basis: GenesisTx, Offset: 1}
	raw, err := encodeMCPResourceCursor(valid)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeMCPResourceCursor(raw, "schema", "x")
	if err != nil || decoded.Offset != 1 {
		t.Fatalf("cursor round trip = %#v, %v", decoded, err)
	}
	unknown := base64.RawURLEncoding.EncodeToString([]byte(`{"version":1,"resource":"schema","argument":"x","basis":64,"extra":true}`))
	trailing := base64.RawURLEncoding.EncodeToString([]byte(`{"version":1,"resource":"schema","argument":"x","basis":64}{}`))
	for name, candidate := range map[string]string{
		"too large":    strings.Repeat("x", maxMCPResourceCursor+1),
		"base64":       "!!",
		"noncanonical": raw + "=",
		"unknown":      unknown,
		"trailing":     trailing,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeMCPResourceCursor(candidate, "schema", "x"); err == nil {
				t.Fatalf("cursor %q unexpectedly accepted", candidate)
			}
		})
	}
	for name, candidate := range map[string]mcpResourceCursor{
		"version":  {Version: 2, Resource: "schema", Argument: "x", Basis: GenesisTx},
		"resource": {Version: 1, Resource: "changes", Argument: "x", Basis: GenesisTx},
		"argument": {Version: 1, Resource: "schema", Argument: "y", Basis: GenesisTx},
		"basis":    {Version: 1, Resource: "schema", Argument: "x", Basis: 0},
		"offset":   {Version: 1, Resource: "schema", Argument: "x", Basis: GenesisTx, Offset: -1},
		"position": {Version: 1, Resource: "schema", Argument: "x", Basis: GenesisTx, Position: -1},
	} {
		t.Run(name, func(t *testing.T) {
			raw, encodeErr := encodeMCPResourceCursor(candidate)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			if _, err := decodeMCPResourceCursor(raw, "schema", "x"); !errors.Is(err, ErrConflict) {
				t.Fatalf("cursor mismatch error = %v", err)
			}
		})
	}
}
