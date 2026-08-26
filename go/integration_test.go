package fgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func fixedDB(t *testing.T, path string) *DB {
	t.Helper()
	db, err := Open(path, WithClock(func() int64 { return 1_767_225_600_000_000 }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTest(t, db) })
	return db
}

func closeTest(t testing.TB, closer interface{ Close() error }) {
	t.Helper()
	if err := closer.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
}

func TestLifecycleMaintenanceAndPortability(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "source.db")
	db := fixedDB(t, path)

	identity, identityErr := db.Transact(ctx, E{"id": "empty"})
	if identityErr != nil || identity.Tx == 0 || identity.Status != "applied" || identity.IDs["empty"] == 0 {
		t.Fatalf("identity transaction = %+v, %v", identity, identityErr)
	}
	before, statsErr := db.Stats(ctx)
	if statsErr != nil {
		t.Fatal(statsErr)
	}
	if _, err := db.Transact(ctx, E{}); err != nil {
		t.Fatal(err)
	}
	after, statsErr := db.Stats(ctx)
	if statsErr != nil {
		t.Fatal(statsErr)
	}
	if before.Facts != after.Facts {
		t.Fatal("empty anonymous map changed facts")
	}
	empty, emptyErr := db.Entity(ctx, "empty")
	if emptyErr != nil || len(empty) != 0 {
		t.Fatalf("empty entity = %v, %v", empty, emptyErr)
	}

	if _, err := db.Declare(ctx, "person/email", Type("text"), Unique(), Doc("Login address")); err != nil {
		t.Fatal(err)
	}
	first, firstErr := db.Add(ctx, E{
		"id": "ada", "person/email": "ada@example.test", "person/name": "Ada",
	}, WithBy("tester"), WithSource("unit"), WithMeta(E{"ticket": 7}), WithTxFacts(E{"audit/kind": "seed"}))
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	upsert, upsertErr := db.Transact(ctx, E{"person/email": "ada@example.test", "person/city": "Lyon"})
	if upsertErr != nil || upsert.Tx == 0 {
		t.Fatalf("upsert = %+v, %v", upsert, upsertErr)
	}
	entity, entityErr := db.Entity(ctx, "ada")
	if entityErr != nil || entity["person/city"] != "Lyon" {
		t.Fatalf("entity = %v, %v", entity, entityErr)
	}
	why, whyErr := db.Why(ctx, "ada", "person/name")
	if whyErr != nil || why[0].Provenance["fgraph/by"] != "tester" {
		t.Fatalf("why = %#v, %v", why, whyErr)
	}
	view, viewErr := db.AtInstant(ctx, first.At)
	if viewErr != nil {
		t.Fatal(viewErr)
	}
	if _, err := view.Transact(ctx, E{"id": "bad", "x/y": 1}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("as-of write error = %v", err)
	}
	if _, err := db.ViewAt(ctx, time.UnixMicro(first.At).UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	if txView, err := db.ViewAt(ctx, first.Tx); err != nil {
		t.Fatal(err)
	} else if atEntity, err := txView.Entity(ctx, "ada"); err != nil || atEntity["person/name"] != "Ada" {
		t.Fatalf("transaction view = %#v, %v", atEntity, err)
	}
	if instantView, err := db.ViewAt(ctx, first.At); err != nil {
		t.Fatal(err)
	} else if atEntity, err := instantView.Entity(ctx, "ada"); err != nil || atEntity["person/name"] != "Ada" {
		t.Fatalf("integer instant view = %#v, %v", atEntity, err)
	}
	if _, err := db.ViewAt(ctx, int64(1)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pre-genesis instant error = %v", err)
	}
	if _, err := db.Changes(ctx, first.Tx); err != nil {
		t.Fatal(err)
	}

	if err := db.Speculate(ctx, func(spec *DB) error {
		if _, err := spec.Transact(ctx, E{"id": "spec", "spec/value": 1}); err != nil {
			return err
		}
		got, err := spec.Entity(ctx, "spec")
		if err != nil || got["spec/value"] != int64(1) {
			t.Fatalf("spec entity = %v, %v", got, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Entity(ctx, "spec"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("speculation persisted: %v", err)
	}
	if err := db.Speculate(ctx, func(spec *DB) error {
		return spec.Speculate(ctx, func(*DB) error { return nil })
	}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("nested speculation error = %v", err)
	}

	undo, undoErr := db.Undo(ctx, upsert.Tx)
	if undoErr != nil || undo.Tx == 0 {
		t.Fatalf("undo = %+v, %v", undo, undoErr)
	}
	if _, err := db.Entity(ctx, "ada"); err != nil {
		t.Fatal(err)
	}

	var exported bytes.Buffer
	if err := db.Tail(ctx, &exported, GenesisTx); err != nil {
		t.Fatal(err)
	}
	target := fixedDB(t, filepath.Join(dir, "target.db"))
	if _, err := target.Apply(ctx, bytes.NewReader(exported.Bytes())); err != nil {
		t.Fatal(err)
	}
	var roundtrip bytes.Buffer
	if err := target.Tail(ctx, &roundtrip, GenesisTx); err != nil {
		t.Fatal(err)
	}
	if imported, err := target.Entity(ctx, "ada"); err != nil || imported["person/name"] != "Ada" {
		t.Fatalf("roundtrip entity = %v, %v\n%s", imported, err, roundtrip.String())
	}

	backup := filepath.Join(dir, "backup.db")
	if err := db.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	copyDB, copyErr := Open(backup, WithReadOnly())
	if copyErr != nil {
		t.Fatal(copyErr)
	}
	if _, err := copyDB.Stats(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := copyDB.Transact(ctx, E{"id": "blocked", "x/y": 1}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only error = %v", err)
	}
	closeTest(t, copyDB)
	if report, err := db.Doctor(ctx); err != nil || !report.OK || report.FTSRows != report.ExpectedFTSRows || report.FTSRowsRebuilt != 0 {
		t.Fatalf("doctor = %+v, %v", report, err)
	}

	if _, err := db.Transact(ctx, E{"id": "erase", "secret/text": strings.Repeat("s", 300)}); err != nil {
		t.Fatal(err)
	}
	excise, exciseErr := db.Excise(ctx, "erase")
	if exciseErr != nil || excise.Tx == 0 || len(excise.Retracted) == 0 {
		t.Fatalf("excise = %+v, %v", excise, exciseErr)
	}
	if _, err := db.Excise(ctx, "fgraph/at"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("system excise error = %v", err)
	}
	if _, err := db.Excise(ctx, excise.Tx); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("tx excise error = %v", err)
	}

	if data, err := MarshalWire(E{"r": RefTo("ada"), "i": Instant(1), "b": Bytes([]byte{1}), "v": Vector([]float32{1}), "j": JSON(E{"a": 1}), "t": Tmp("x")}); err != nil || !bytes.Contains(data, []byte(`"ref"`)) {
		t.Fatalf("wire = %s, %v", data, err)
	}
}

func TestMetadataOnlyTransactions(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	seed := E{"id": "stable", "item/value": "same"}
	if _, err := db.Transact(ctx, seed); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		want any
		opt  TxOption
		name string
		attr string
	}{
		{name: "empty by", opt: WithBy(""), attr: "fgraph/by", want: ""},
		{name: "empty source", opt: WithSource(""), attr: "fgraph/source", want: ""},
		{name: "null meta", opt: WithMeta(nil), attr: "fgraph/meta", want: map[string]any{"json": nil}},
		{name: "tx facts", opt: WithTxFacts(E{"audit/kind": "metadata-only"}), attr: "audit/kind", want: "metadata-only"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			report, err := db.Transact(ctx, seed, test.opt)
			if err != nil || report.Tx == 0 {
				t.Fatalf("metadata transaction = %+v, %v", report, err)
			}
			entity, err := db.Entity(ctx, report.Tx)
			if err != nil {
				t.Fatal(err)
			}
			got, ok := entity[test.attr]
			if !ok {
				t.Fatalf("transaction entity %#v lacks %q", entity, test.attr)
			}
			gotJSON, marshalErr := json.Marshal(got)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			wantJSON, marshalErr := json.Marshal(test.want)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if !bytes.Equal(gotJSON, wantJSON) {
				t.Fatalf("%s = %s, want %s", test.attr, gotJSON, wantJSON)
			}
		})
	}
}

func TestSystemAndTransactionEntitiesAreImmutable(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	before, beforeErr := db.RawFacts(ctx, true)
	if beforeErr != nil {
		t.Fatal(beforeErr)
	}
	for _, write := range []any{
		E{"id": int64(1), "app/value": "bad"},
		E{"id": "fgraph/at", "app/value": "bad"},
		[]any{"assert", int64(1), "app/value", "bad"},
		[]any{"retract", int64(1)},
	} {
		if _, err := db.Transact(ctx, write); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("system write %#v error = %v", write, err)
		}
	}
	if _, err := db.Declare(ctx, "fgraph/at", Doc("bad")); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("system declaration error = %v", err)
	}
	if _, err := db.Undo(ctx, GenesisTx); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("genesis undo error = %v", err)
	}
	report, reportErr := db.Transact(ctx, E{"id": "normal", "app/value": "ok"})
	if reportErr != nil {
		t.Fatal(reportErr)
	}
	for _, write := range []any{
		E{"id": report.Tx, "app/value": "bad"},
		[]any{"retract", report.Tx},
	} {
		if _, err := db.Transact(ctx, write); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("transaction write %#v error = %v", write, err)
		}
	}
	after, afterErr := db.RawFacts(ctx, true)
	if afterErr != nil {
		t.Fatal(afterErr)
	}
	// Only the normal transaction may have changed storage.
	if len(after) <= len(before) {
		t.Fatalf("normal transaction missing: before=%d after=%d", len(before), len(after))
	}
	if !reflect.DeepEqual(after[:len(before)], before) {
		t.Fatal("system facts changed after rejected writes")
	}
}

func TestMultipleHandlesAndConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "shared.db")
	first := fixedDB(t, path)
	second := fixedDB(t, path)
	if _, err := first.Transact(ctx, E{"id": "from-first", "shared/value": 1}); err != nil {
		t.Fatal(err)
	}
	if entity, err := second.Entity(ctx, "from-first"); err != nil || entity["shared/value"] != int64(1) {
		t.Fatalf("second handle refresh = %#v, %v", entity, err)
	}
	if _, err := second.Transact(ctx, E{"id": "from-second", "shared/value": 2}); err != nil {
		t.Fatal(err)
	}
	if entity, err := first.Entity(ctx, "from-second"); err != nil || entity["shared/value"] != int64(2) {
		t.Fatalf("first handle refresh = %#v, %v", entity, err)
	}

	const writers = 4
	const writesPerWorker = 15
	errorsOut := make(chan error, writers*2)
	var group sync.WaitGroup
	for worker := 0; worker < writers; worker++ {
		group.Add(2)
		go func() {
			defer group.Done()
			for item := 0; item < writesPerWorker; item++ {
				name := fmt.Sprintf("worker-%d-%d", worker, item)
				if _, err := first.Transact(ctx, E{"id": name, "shared/worker": worker, "shared/item": item}); err != nil {
					errorsOut <- err
					return
				}
			}
		}()
		go func() {
			defer group.Done()
			for item := 0; item < writesPerWorker; item++ {
				if _, err := first.Stats(ctx); err != nil {
					errorsOut <- err
					return
				}
				if _, err := first.Entity(ctx, "fgraph/at"); err != nil {
					errorsOut <- err
					return
				}
			}
		}()
	}
	group.Wait()
	close(errorsOut)
	for err := range errorsOut {
		t.Fatal(err)
	}
	result, err := first.Query(ctx, Q{Find: []any{"?e"}, Where: []any{[]any{"?e", "shared/worker", "_"}}}, nil)
	if err != nil || len(result.Rows) != writers*writesPerWorker {
		t.Fatalf("concurrent rows = %d, %v", len(result.Rows), err)
	}
}

func TestFollowAndFileValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "follow.db")
	db := fixedDB(t, path)
	stream := db.Follow(ctx, FollowOptions{Since: GenesisTx, Interval: time.Millisecond})
	report, err := db.Transact(ctx, E{"id": "event", "event/name": "created"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-stream:
		if event.Tx != report.Tx {
			t.Fatalf("event tx=%d want=%d", event.Tx, report.Tx)
		}
	case <-ctx.Done():
		t.Fatal("follow timed out")
	}

	badPath := filepath.Join(t.TempDir(), "bad.db")
	bad, err := Open(badPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bad.store.sql.Exec("DROP VIEW fgraph_now"); err != nil {
		t.Fatal(err)
	}
	closeTest(t, bad)
	if _, err := Open(badPath); !errors.Is(err, ErrFormat) {
		t.Fatalf("partial format error = %v", err)
	}
	if _, err := Open(""); !errors.Is(err, ErrFormat) {
		t.Fatalf("empty path error = %v", err)
	}
}

func TestFollowTraversesCommittedReceiptsAcrossLargeAllocationGap(t *testing.T) {
	// The race detector makes the deliberate 2,000-row allocation setup much
	// slower; the assertion still distinguishes receipt iteration from scanning
	// every numeric gap because the follower must deliver the first receipt.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db := fixedDB(t, ":memory:")
	identities := make([]any, 2_000)
	for index := range identities {
		identities[index] = E{"id": fmt.Sprintf("gap-%04d", index)}
	}
	report, err := db.Transact(ctx, identities)
	if err != nil || report.Tx == 0 {
		t.Fatalf("identity allocation gap = %+v, %v", report, err)
	}
	stream := db.Follow(ctx, FollowOptions{Since: report.Tx, Interval: time.Millisecond})
	committed, err := db.Transact(ctx, E{"id": "after-gap", "event/name": "created"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-stream:
		if event.Err != nil || event.Tx != committed.Tx {
			t.Fatalf("event across allocation gap = %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("follow scanned the numeric allocation gap instead of committed receipts")
	}
}

func TestQueryAggregatesAndFailures(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	_, err := db.Transact(ctx, []any{
		E{"id": "a", "item/name": "Alpha", "item/value": 1},
		E{"id": "b", "item/name": "Beta", "item/value": 2},
		E{"id": "c", "item/name": "Gamma", "item/value": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, aggregate := range []string{"count", "count-distinct", "sum", "min", "max", "avg"} {
		result, err := db.Qry(ctx, Q{Find: []any{[]any{aggregate, "?v"}}, Where: []any{[]any{"?e", "item/value", "?v"}}}, nil)
		if err != nil || len(result.Rows) != 1 {
			t.Fatalf("%s = %+v, %v", aggregate, result, err)
		}
	}
	predicates := []any{
		[]any{"!=", "?v", int64(99)},
		[]any{"<", "?v", int64(3)},
		[]any{"<=", "?v", int64(2)},
		[]any{">", "?v", int64(0)},
		[]any{"contains", "?n", "ph"},
		[]any{"starts-with", "?n", "Al"},
	}
	for _, predicate := range predicates {
		where := []any{[]any{"?e", "item/value", "?v"}, []any{"?e", "item/name", "?n"}, predicate}
		if _, err := db.Query(ctx, Q{Find: []any{"?e"}, Where: where}, nil); err != nil {
			t.Fatalf("predicate %v: %v", predicate, err)
		}
	}
	failures := []Q{
		{},
		{Find: []any{"?x"}, Where: []any{[]any{"?e", "item/value", "?v"}}},
		{Find: []any{"?e"}, Where: []any{[]any{"bogus", int64(1), 1}}},
		{Find: []any{"?e"}, Where: []any{Object{Fields: []Field{{Name: "or", Value: []any{[]any{[]any{"?e", "item/name", "?n"}}, []any{[]any{"?e", "item/value", "?v"}}}}}}}},
	}
	for _, query := range failures {
		if _, err := db.Query(ctx, query, nil); !errors.Is(err, ErrQuery) {
			t.Fatalf("query %#v error = %v", query, err)
		}
	}
}

func TestSearchContractEdges(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "doc/vector", Type("vector")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "doc/link", Ref(), Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{
		"id": "zeta", "doc/text": "héllo token", "doc/vector": Vector([]float32{0.8, 0.6}),
		"doc/link": []any{RefTo("neighbor")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{
		"id": "alpha", "doc/text": "héllo token", "doc/vector": Vector([]float32{1, 0}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "zero", "doc/vector": Vector([]float32{0, 0})}); err != nil {
		t.Fatal(err)
	}

	if got := ftsQuery("héllo,_42! world"); got != `"héllo" "_42" "world"` {
		t.Fatalf("ftsQuery = %q", got)
	}
	punctuation, punctuationErr := db.Search(ctx, SearchOpts{Text: "!!!", K: 2})
	if punctuationErr != nil || len(punctuation.Hits) != 0 {
		t.Fatalf("punctuation search = %#v, %v", punctuation, punctuationErr)
	}
	if _, keywordErr := db.Search(ctx, SearchOpts{Text: "héllo", VectorAttribute: "missing/vector", K: 2}); !errors.Is(keywordErr, ErrType) {
		t.Fatalf("keyword with vector attribute error = %v", keywordErr)
	}
	if _, err := db.Search(ctx, SearchOpts{Vector: []float32{0, 0}, VectorAttribute: "doc/vector"}); !errors.Is(err, ErrType) {
		t.Fatalf("zero vector error = %v", err)
	}
	if _, err := db.Search(ctx, SearchOpts{Vector: []float32{1, 0}, VectorAttribute: "missing/vector"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown vector attribute error = %v", err)
	}
	if _, err := db.Search(ctx, SearchOpts{Vector: []float32{1, 0}, VectorAttribute: "doc/text"}); !errors.Is(err, ErrType) {
		t.Fatalf("non-vector attribute error = %v", err)
	}

	hybrid, hybridErr := db.Search(ctx, SearchOpts{Text: "token", Vector: []float32{1, 0}, VectorAttribute: "doc/vector", K: 2, Expand: 1})
	if hybridErr != nil {
		t.Fatal(hybridErr)
	}
	if len(hybrid.Hits) != 2 || hybrid.Hits[0].Entity != "alpha" || hybrid.Hits[1].Entity != "zeta" {
		t.Fatalf("hybrid lexical tie order = %#v", hybrid.Hits)
	}
	for _, hit := range hybrid.Hits {
		if hit.Entity == "zero" {
			t.Fatalf("undefined zero-vector candidate was ranked: %#v", hybrid.Hits)
		}
	}
	if len(hybrid.Expanded) != 1 || hybrid.Expanded[0].Entity != "neighbor" || len(hybrid.Expanded[0].Via) != 1 {
		t.Fatalf("expanded = %#v", hybrid.Expanded)
	}
	via, ok := hybrid.Expanded[0].Via[0].(Fact)
	if !ok || via.E != "zeta" || via.A != "doc/link" {
		t.Fatalf("via = %#v", hybrid.Expanded[0].Via)
	}
	encoded, marshalErr := json.Marshal(via)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if !bytes.Contains(encoded, []byte(`"ref":"neighbor"`)) {
		t.Fatalf("via wire value = %s", encoded)
	}
}

func TestValueBoundaries(t *testing.T) {
	values := []any{int(1), int8(1), int16(1), int32(1), int64(1), uint(1), uint64(1), float32(1.5), float64(1.5), true, "x", time.Unix(0, 0), []byte{1}, BytesValue{1}, VectorValue{1}, JSONValue{Value: E{"x": 1}}}
	for _, value := range values {
		if _, err := scalarValue(value); err != nil {
			t.Errorf("scalar %T: %v", value, err)
		}
	}
	bad := []any{nil, math.NaN(), math.Inf(1), uint64(math.MaxInt64) + 1, strings.Repeat("x", MaxValueBytes+1), make([]byte, MaxValueBytes+1)}
	for _, value := range bad {
		if _, err := scalarValue(value); err == nil {
			t.Errorf("scalar %T unexpectedly accepted", value)
		}
	}
	canonical, canonicalErr := canonicalJSON(E{"line": "\u2028\n", "f": math.Copysign(0, -1), "small": 1e-7, "big": 1e21, "unicode": "é"})
	if canonicalErr != nil || strings.Contains(string(canonical), `\u2028`) || !strings.Contains(string(canonical), `1e+21`) {
		t.Fatalf("canonical = %s, %v", canonical, canonicalErr)
	}
	wantCanonical := `{"a":{"b":0,"é":"x"},"bigexp":1e+21,"bigfixed":1e+20,"exp":1e-7,"fixed":0.000001,"z":1}`
	canonical, canonicalErr = canonicalJSON(E{
		"z": 1.0, "a": E{"é": "x", "b": math.Copysign(0, -1)}, "exp": 1e-7,
		"fixed": 1e-6, "bigfixed": 1e20, "bigexp": 1e21,
	})
	if canonicalErr != nil || string(canonical) != wantCanonical {
		t.Fatalf("canonical numeric contract = %s, %v", canonical, canonicalErr)
	}
	for _, input := range []string{`{"x":1,"x":2}`, `{"x":`, `1 2`, `9223372036854775808`} {
		if _, err := DecodeJSON(strings.NewReader(input)); !errors.Is(err, ErrType) {
			t.Errorf("DecodeJSON(%q) error = %v, want TypeError", input, err)
		}
	}
	decoded, decodeErr := DecodeJSON(strings.NewReader(`{"nested":{"a":1},"items":[true,"ok"]}`))
	if decodeErr != nil {
		t.Fatalf("valid DecodeJSON: %v", decodeErr)
	}
	if object, ok := decoded.(Object); !ok || len(object.Fields) != 2 {
		t.Fatalf("valid DecodeJSON = %#v", decoded)
	}
	for _, boundary := range []struct {
		want   string
		micros int64
	}{
		{micros: minInstantMicros, want: "0001-01-01T00:00:00.000000Z"},
		{micros: maxInstantMicros, want: "9999-12-31T23:59:59.999999Z"},
	} {
		stored, err := scalarValue(Instant(boundary.micros))
		micros, microsOK := stored.logical.(int64)
		if err != nil || !microsOK || formatInstant(micros) != boundary.want {
			t.Fatalf("instant boundary %d = %#v, %v", boundary.micros, stored, err)
		}
	}
	for _, outside := range []int64{minInstantMicros - 1, maxInstantMicros + 1} {
		if _, err := scalarValue(Instant(outside)); !errors.Is(err, ErrType) {
			t.Fatalf("instant outside range %d error = %v", outside, err)
		}
	}
	if _, err := Open(":memory:", WithClock(func() int64 { return maxInstantMicros + 1 })); !errors.Is(err, ErrType) {
		t.Fatalf("out-of-range genesis clock error = %v", err)
	}
}

func TestExportCanonicalVector(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{
		"id": "vector", "item/embedding": Vector([]float32{1, 0, -0}),
		"item/data": JSON(E{"z": 1.0, "a": "é"}),
	}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := db.Tail(ctx, &output, GenesisTx); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(output.Bytes(), []byte(`{"vector":[1,0,0]}`)) {
		t.Fatalf("vector not canonical in export: %s", output.String())
	}
	if !bytes.Contains(output.Bytes(), []byte(`{"json":{"a":"é","z":1}}`)) {
		t.Fatalf("nested JSON not canonical in export: %s", output.String())
	}
}

func TestApplyRejectsLocalNumericSelectors(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	report, transactErr := db.Transact(ctx, E{"item/value": "keep"})
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	domain := factsForAttribute(report.Asserted, "item/value")
	if len(domain) != 1 {
		t.Fatalf("domain assertions = %#v", domain)
	}
	localID, ok := domain[0].E.(int64)
	if !ok {
		t.Fatalf("anonymous entity = %#v", domain[0].E)
	}
	line := fmt.Sprintf(`{"fgraph":"event/1","event":"11111111-1111-4111-8111-111111111111","at":1767225605000000,"created":[],"asserted":[],"retracted":[[%d,"item/value","keep","text"]]}`+"\n", localID)
	if _, err := db.Apply(ctx, strings.NewReader(line)); !errors.Is(err, ErrType) {
		t.Fatalf("numeric selector apply error = %v, want TypeError", err)
	}
	entity, entityErr := db.Entity(ctx, localID)
	if entityErr != nil || entity["item/value"] != "keep" {
		t.Fatalf("foreign retraction collided with local entity: %#v, %v", entity, entityErr)
	}
}

func TestApplyPreservesEmptyEventTransactionReference(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	stream := strings.NewReader(
		`{"fgraph":"event/1","event":"11111111-1111-4111-8111-111111111111","at":1767225605000000,"created":[],"asserted":[],"retracted":[]}` + "\n" +
			`{"fgraph":"event/1","event":"22222222-2222-4222-8222-222222222222","at":1767225606000000,"created":[],"tx_facts":[["fgraph/undoes",{"ref":{"eid":"11111111-1111-4111-8111-111111111111"}},"ref"]],"asserted":[],"retracted":[]}` + "\n",
	)
	if _, err := db.Apply(ctx, stream); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := db.Tail(ctx, &output, GenesisTx); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("imported empty transaction lines = %d\n%s", len(lines), output.String())
	}
	second, decodeErr := DecodeJSON(strings.NewReader(lines[1]))
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	line, lineOK := objectMap(second)
	if !lineOK {
		t.Fatalf("second export line = %T", second)
	}
	txFacts, txFactsOK := line["tx_facts"].([]any)
	if !txFactsOK || len(txFacts) != 1 {
		t.Fatalf("tx_facts = %#v", line["tx_facts"])
	}
	tuple, tupleOK := txFacts[0].([]any)
	if !tupleOK || len(tuple) != 3 {
		t.Fatalf("tx_fact tuple = %#v", txFacts[0])
	}
	reference, referenceOK := objectMap(tuple[1])
	if !referenceOK {
		t.Fatalf("tx_fact reference = %#v", tuple[1])
	}
	firstLine, decodeErr := DecodeJSON(strings.NewReader(lines[0]))
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	firstObject, firstOK := objectMap(firstLine)
	if !firstOK {
		t.Fatalf("first export line = %T", firstLine)
	}
	wireReference, wireReferenceOK := objectMap(reference["ref"])
	if !wireReferenceOK || !reflect.DeepEqual(wireReference, map[string]any{"eid": firstObject["event"]}) {
		t.Fatalf("undo ref = %v, first portable event = %v", reference["ref"], firstObject["event"])
	}
}

func TestMCPTools(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	server := NewMCPServer(db, MCPOptions{Write: true, Embed: func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil }})
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, serverErr := server.Connect(ctx, serverTransport, nil)
	if serverErr != nil {
		t.Fatal(serverErr)
	}
	defer closeTest(t, serverSession)
	client := mcp.NewClient(&mcp.Implementation{Name: "unit-client", Version: "1"}, nil)
	clientSession, clientErr := client.Connect(ctx, clientTransport, nil)
	if clientErr != nil {
		t.Fatal(clientErr)
	}
	defer closeTest(t, clientSession)
	call := func(name string, args map[string]any) *mcp.CallToolResult {
		t.Helper()
		if name == "remember" {
			basis, err := db.latestTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			args["operation_id"] = fmt.Sprintf("mcp-test:%s:%d", name, basis)
		}
		if name == "forget" || name == "undo" {
			basis, err := db.latestTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			args["operation_id"] = fmt.Sprintf("mcp-test:%s:%d", name, basis)
			args["if_basis_tx"] = basis
		}
		result, err := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		if result.IsError {
			t.Fatalf("%s failed: %+v", name, result.Content)
		}
		return result
	}
	remember := call("remember", map[string]any{"text": "remember this build", "source": "test"})
	var report TxReport
	decodeMCPData(t, remember, &report)
	call("recall", map[string]any{"query": "build", "k": 3})
	call("about", map[string]any{"entity": int64(67)})
	call("why", map[string]any{"entity": int64(67), "attribute": "memory/text"})
	call("history", map[string]any{"entity": int64(67), "attribute": "memory/text"})
	call("query", map[string]any{"q": map[string]any{"find": []any{"?t"}, "where": []any{[]any{"?e", "memory/text", "?t"}}}})
	schema := call("schema", map[string]any{"prefix": "memory/"})
	var discovered struct {
		Attributes []SchemaAttribute `json:"attributes"`
	}
	decodeMCPData(t, schema, &discovered)
	if len(discovered.Attributes) != 2 {
		t.Fatalf("schema envelope = %#v", schema.StructuredContent)
	}
	call("remember", map[string]any{"key": "mcp-keyed", "text": "first keyed note"})
	call("remember", map[string]any{"key": "mcp-keyed", "text": "updated keyed note"})
	if keyed, keyedErr := db.Entity(ctx, "mcp-keyed"); keyedErr != nil || keyed["memory/text"] != "updated keyed note" {
		t.Fatalf("keyed MCP note = %#v, %v", keyed, keyedErr)
	}
	keyWithoutText, keyErr := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: "remember", Arguments: map[string]any{"key": "mcp-keyed"},
	})
	if keyErr != nil || !keyWithoutText.IsError {
		t.Fatalf("remember key without text = %#v, %v", keyWithoutText, keyErr)
	}
	call("remember", map[string]any{"facts": map[string]any{"id": "mcp-map", "memory/kind": "map"}})
	call("remember", map[string]any{"facts": []any{"assert", "mcp-op", "memory/kind", "op"}})
	call("remember", map[string]any{
		"facts": []any{
			map[string]any{"id": "mcp-array-map", "memory/kind": "array"},
			[]any{"assert", "mcp-array-op", "memory/kind", "array-op"},
		},
		"text": "combined note",
	})
	emptyRemember, emptyErr := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: "remember", Arguments: map[string]any{"facts": []any{}}})
	if emptyErr != nil {
		t.Fatal(emptyErr)
	}
	if !emptyRemember.IsError {
		t.Fatalf("empty remember unexpectedly succeeded: %#v", emptyRemember)
	}
	for entity, want := range map[string]string{
		"mcp-map": "map", "mcp-op": "op", "mcp-array-map": "array", "mcp-array-op": "array-op",
	} {
		got, entityErr := db.Entity(ctx, entity)
		if entityErr != nil || got["memory/kind"] != want {
			t.Fatalf("MCP remember %s = %#v, %v", entity, got, entityErr)
		}
	}
	provenance, provenanceErr := db.Why(ctx, "mcp-map", "memory/kind")
	if provenanceErr != nil || len(provenance) != 1 || provenance[0].Provenance["fgraph/by"] != "mcp:unit-client" {
		t.Fatalf("MCP provenance = %#v, %v", provenance, provenanceErr)
	}
	invalidForget, invalidForgetErr := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: "forget", Arguments: map[string]any{"entity": "mcp-map", "value": "map"},
	})
	if invalidForgetErr != nil || !invalidForget.IsError {
		t.Fatalf("forget value without attribute = %#v, %v", invalidForget, invalidForgetErr)
	}
	if entity, entityErr := db.Entity(ctx, "mcp-map"); entityErr != nil || entity["memory/kind"] != "map" {
		t.Fatalf("invalid forget mutated entity = %#v, %v", entity, entityErr)
	}
	call("forget", map[string]any{"entity": int64(67), "attribute": "memory/text"})
	if report.Tx != 0 {
		call("undo", map[string]any{"tx": report.Tx})
	}
	readOnly := NewMCPServer(db, MCPOptions{ReadOnly: true})
	roClientTransport, roServerTransport := mcp.NewInMemoryTransports()
	roServerSession, roServerErr := readOnly.Connect(ctx, roServerTransport, nil)
	if roServerErr != nil {
		t.Fatal(roServerErr)
	}
	defer closeTest(t, roServerSession)
	roClient := mcp.NewClient(&mcp.Implementation{Name: "reader", Version: "1"}, nil)
	roClientSession, roClientErr := roClient.Connect(ctx, roClientTransport, nil)
	if roClientErr != nil {
		t.Fatal(roClientErr)
	}
	defer closeTest(t, roClientSession)
	listed, err := roClientSession.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	gotNames := make([]string, len(listed.Tools))
	for i, tool := range listed.Tools {
		gotNames[i] = tool.Name
	}
	wantNames := []string{"about", "datoms", "explain", "history", "query", "recall", "receipt", "schema", "why"}
	if !slices.Equal(gotNames, wantNames) {
		t.Fatalf("read-only MCP tools = %v, want %v", gotNames, wantNames)
	}
}

func TestMCPV1InventoryEnvelopeReceiptAndLimits(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	first, err := db.Transact(ctx, E{"id": "bounded", "item/value": "one", "item/extra": true})
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.Transact(ctx, E{"id": "bounded", "item/value": "two"})
	if err != nil {
		t.Fatal(err)
	}
	server := NewMCPServer(db, MCPOptions{Write: true})
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, serverSession)
	client := mcp.NewClient(&mcp.Implementation{Name: "v1-contract", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, clientSession)

	listed, err := clientSession.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(listed.Tools))
	for index, tool := range listed.Tools {
		names[index] = tool.Name
	}
	want := []string{"about", "datoms", "explain", "forget", "history", "query", "recall", "receipt", "remember", "schema", "undo", "why"}
	if !slices.Equal(names, want) || slices.Contains(names, "excise") {
		t.Fatalf("write MCP tools = %v, want %v", names, want)
	}

	call := func(name string, arguments map[string]any) *mcp.CallToolResult {
		t.Helper()
		result, callErr := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: arguments})
		if callErr != nil {
			t.Fatal(callErr)
		}
		if result.IsError {
			t.Fatalf("%s failed: %#v", name, result.Content)
		}
		return result
	}
	receiptResult := call("receipt", map[string]any{"tx": first.Tx})
	var receipt EventReceipt
	if basis := decodeMCPData(t, receiptResult, &receipt); basis != second.Tx || receipt.Tx != first.Tx || receipt.ReadBasisTx != second.Tx {
		t.Fatalf("receipt envelope basis=%d receipt=%#v", basis, receipt)
	}
	for _, tool := range []string{"why", "history"} {
		result := call(tool, map[string]any{"entity": "bounded", "limit": 1})
		var page struct {
			Items     []json.RawMessage `json:"items"`
			Truncated bool              `json:"truncated"`
		}
		decodeMCPData(t, result, &page)
		if len(page.Items) != 1 || !page.Truncated {
			t.Fatalf("%s page = %#v", tool, page)
		}
	}
	queryResult := call("query", map[string]any{"q": map[string]any{
		"find": []any{"?value"}, "where": []any{[]any{"bounded", "item/value", "?value"}},
	}})
	var result Result
	decodeMCPData(t, queryResult, &result)
	if len(result.Rows) != 1 || result.Rows[0][0] != "two" {
		t.Fatalf("query envelope data = %#v", result)
	}
	schemaResult := call("schema", map[string]any{"prefix": "item/", "limit": 1})
	var schemaPage mcpSchemaResult
	firstSchemaBasis := decodeMCPData(t, schemaResult, &schemaPage)
	if len(schemaPage.Attributes) != 1 || schemaPage.NextCursor == nil || !schemaPage.Truncated {
		t.Fatalf("first schema tool page = %#v", schemaPage)
	}
	if _, err := db.Transact(ctx, E{"id": "later-schema", "item/later": true}); err != nil {
		t.Fatal(err)
	}
	schemaResult = call("schema", map[string]any{"cursor": *schemaPage.NextCursor, "limit": 1})
	var schemaTail mcpSchemaResult
	if basis := decodeMCPData(t, schemaResult, &schemaTail); basis != firstSchemaBasis || len(schemaTail.Attributes) != 1 || schemaTail.NextCursor != nil || schemaTail.Truncated {
		t.Fatalf("pinned schema tool page basis=%d page=%#v", basis, schemaTail)
	}

	for name, arguments := range map[string]map[string]any{
		"recall k":      {"query": "two", "k": 21},
		"recall expand": {"query": "two", "expand": 3},
		"about depth":   {"entity": "bounded", "depth": 3},
		"why limit":     {"entity": "bounded", "limit": 101},
		"history limit": {"entity": "bounded", "limit": 0},
		"query limit": {"q": map[string]any{
			"find": []any{"?value"}, "where": []any{[]any{"bounded", "item/value", "?value"}}, "limit": 1001,
		}},
		"datoms limit":         {"limit": 101},
		"schema limit low":     {"limit": 0},
		"schema limit high":    {"limit": 101},
		"schema cursor":        {"cursor": "!!"},
		"schema cursor prefix": {"prefix": "other/", "cursor": *schemaPage.NextCursor},
	} {
		t.Run(name, func(t *testing.T) {
			tool := strings.Split(name, " ")[0]
			response, callErr := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: arguments})
			if callErr != nil {
				t.Fatal(callErr)
			}
			if !response.IsError {
				t.Fatalf("%s unexpectedly succeeded: %#v", name, response.StructuredContent)
			}
		})
	}
}
