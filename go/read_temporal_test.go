package fgraph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestLogicalValueCorruptionAndRenderingMatrix(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	err := db.withRead(ctx, func(runner sqlRunner) error {
		checks := []struct {
			raw  any
			want any
			tag  Tag
		}{
			{raw: int64(1), tag: TagBool, want: true},
			{raw: int64(4), tag: TagInt, want: int64(4)},
			{raw: int64(5), tag: TagInstant, want: int64(5)},
			{raw: int64(6), tag: TagRef, want: int64(6)},
			{raw: 1.25, tag: TagFloat, want: 1.25},
			{raw: "text", tag: TagText, want: "text"},
			{raw: []byte{1, 2}, tag: TagBytes, want: []byte{1, 2}},
		}
		for _, check := range checks {
			got, err := db.logicalValue(ctx, runner, check.raw, check.tag)
			if err != nil || !reflect.DeepEqual(got, check.want) {
				t.Errorf("logicalValue(%#v,%d) = %#v, %v", check.raw, check.tag, got, err)
			}
		}
		if _, err := db.logicalValue(ctx, runner, "not bytes", TagBytes); !errors.Is(err, ErrFormat) {
			t.Errorf("direct bytes corruption error = %v", err)
		}

		vectorData := make([]byte, 8)
		binary.LittleEndian.PutUint32(vectorData, math.Float32bits(1.5))
		binary.LittleEndian.PutUint32(vectorData[4:], math.Float32bits(-2))
		longText := string(bytes.Repeat([]byte("x"), BlobThreshold+1))
		longBytes := bytes.Repeat([]byte{3}, BlobThreshold+1)
		textGood := indirectDigest(TagTextRef, []byte(longText))
		bytesGood := indirectDigest(TagBytesRef, longBytes)
		vectorGood := indirectDigest(TagVector, vectorData)
		textBad := bytes.Repeat([]byte{0x11}, 32)
		bytesBad := bytes.Repeat([]byte{0x22}, 32)
		vectorBad := bytes.Repeat([]byte{0x33}, 32)
		missing := bytes.Repeat([]byte{0x44}, 32)
		blobs := []struct {
			data any
			hash []byte
		}{
			{hash: textGood[:], data: longText},
			{hash: textBad, data: []byte("wrong")},
			{hash: bytesGood[:], data: longBytes},
			{hash: bytesBad, data: "wrong"},
			{hash: vectorGood[:], data: vectorData},
			{hash: vectorBad, data: []byte{1}},
		}
		for _, blob := range blobs {
			if _, err := runner.ExecContext(ctx, "INSERT INTO fgraph_blobs(hash,data) VALUES (?,?)", blob.hash, blob.data); err != nil {
				return err
			}
		}
		if got, err := db.logicalValue(ctx, runner, textGood[:], TagTextRef); err != nil || got != longText {
			t.Errorf("text ref = %#v, %v", got, err)
		}
		if got, err := db.logicalValue(ctx, runner, bytesGood[:], TagBytesRef); err != nil || !reflect.DeepEqual(got, longBytes) {
			t.Errorf("bytes ref = %#v, %v", got, err)
		}
		if got, err := db.logicalValue(ctx, runner, vectorGood[:], TagVector); err != nil || !reflect.DeepEqual(got, []float32{1.5, -2}) {
			t.Errorf("vector ref = %#v, %v", got, err)
		}
		for _, bad := range []struct {
			raw any
			tag Tag
		}{
			{"hash", TagTextRef},
			{missing, TagTextRef},
			{textBad, TagTextRef},
			{bytesBad, TagBytesRef},
			{vectorBad, TagVector},
			{int64(1), TagJSON},
			{"{", TagJSON},
			{nil, Tag(99)},
		} {
			if _, err := db.logicalValue(ctx, runner, bad.raw, bad.tag); !errors.Is(err, ErrFormat) {
				t.Errorf("corrupt logical value %#v/%d error = %v", bad.raw, bad.tag, err)
			}
		}
		if got, err := db.logicalValue(ctx, runner, `{"a":1,"b":1.5}`, TagJSON); err != nil {
			t.Errorf("valid JSON = %#v, %v", got, err)
		}
		if _, err := db.renderRaw(ctx, runner, rawFact{id: 1, e: 65, a: 999, v: int64(1), t: TagInt, tx: 65}, nil); !errors.Is(err, ErrFormat) {
			t.Errorf("missing rendered attr error = %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, check := range []struct {
		value any
		tag   Tag
	}{
		{int64(65), TagRef},
		{int64(1), TagInstant},
		{[]byte{1}, TagBytes},
		{[]byte{1}, TagBytesRef},
		{[]float32{1}, TagVector},
		{E{"a": 1}, TagJSON},
		{"plain", TagText},
	} {
		if got := db.renderLogical(check.value, check.tag); got == nil {
			t.Errorf("renderLogical(%d) returned nil", check.tag)
		}
	}
	rendered, ok := db.renderLogical([]float32{0.1, -0.2}, TagVector).(map[string]any)
	if !ok {
		t.Fatalf("rendered vector has type %T", rendered)
	}
	renderedVector := rendered["vector"]
	wantVector := []any{float64(float32(0.1)), float64(float32(-0.2))}
	if !reflect.DeepEqual(renderedVector, wantVector) {
		t.Fatalf("rendered vector = %#v, want %#v", renderedVector, wantVector)
	}
}

func TestReadReferenceDepthAndRawFactsEdges(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "node/links", Ref(), Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "a", "node/name": "A", "node/links": []any{RefTo("b"), RefTo("c")}},
		E{"id": "b", "node/name": "B", "node/links": []any{RefTo("a")}},
		E{"id": "c", "node/name": "C"},
	}); err != nil {
		t.Fatal(err)
	}
	deep, err := db.Entity(ctx, "a", 3)
	if err != nil {
		t.Fatal(err)
	}
	links, ok := deep["node/links"].([]any)
	if !ok || len(links) != 2 {
		t.Fatalf("deep links = %#v", deep)
	}
	if _, err := db.Entity(ctx, "a", -1); !errors.Is(err, ErrType) {
		t.Fatalf("negative depth error = %v", err)
	}
	for _, ref := range []any{int64(0), int(0), float64(1.5), true, []any{"missing/unique", "x"}} {
		if _, err := db.Entity(ctx, ref); err == nil {
			t.Errorf("invalid/missing entity ref %#v accepted", ref)
		}
	}
	if _, err := db.Entity(ctx, float64(999)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown integral float reference error = %v", err)
	}
	if facts, err := db.RawFacts(ctx, false); err != nil || len(facts) == 0 {
		t.Fatalf("raw non-genesis facts = %d, %v", len(facts), err)
	}
	if _, err := db.RawFacts(ctx, true); err != nil {
		t.Fatal(err)
	}
	if stats, err := db.Stats(ctx); err != nil || stats.Attributes < 2 || stats.Size != 0 {
		t.Fatalf("memory stats = %+v, %v", stats, err)
	}

	// Remove the local cache entry to exercise the authoritative SQLite lookup.
	delete(db.store.names, "a")
	if entity, err := db.Entity(ctx, "a"); err != nil || entity["node/name"] != "A" {
		t.Fatalf("uncached name lookup = %#v, %v", entity, err)
	}
}

func TestTemporalInputErrorsAndUndoAllTags(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	for _, value := range []any{Instant(1_767_225_600_000_000), "2026-01-01T00:00:00Z", int(GenesisTx), int64(GenesisTx)} {
		if _, err := db.ViewAt(ctx, value); err != nil {
			t.Errorf("ViewAt(%#v) = %v", value, err)
		}
	}
	for _, value := range []any{"not-an-instant", "2026-01-01T00:00:00,5Z", true} {
		if _, err := db.ViewAt(ctx, value); !errors.Is(err, ErrType) {
			t.Errorf("invalid ViewAt(%#v) error = %v", value, err)
		}
	}
	if _, err := db.AtInstant(ctx, minInstantMicros); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pre-genesis AtInstant error = %v", err)
	}
	for _, call := range []func() error{
		func() error { _, err := db.History(ctx, "missing"); return err },
		func() error { _, err := db.Why(ctx, "missing"); return err },
	} {
		if err := call(); !errors.Is(err, ErrNotFound) {
			t.Errorf("missing temporal entity error = %v", err)
		}
	}
	seed, seedErr := db.Transact(ctx, E{"id": "history", "item/value": "one"}, WithBy("writer"), WithSource("source"))
	if seedErr != nil {
		t.Fatal(seedErr)
	}
	changed, changedErr := db.Transact(ctx, E{"id": "history", "item/value": "two"}, WithBy("editor"), WithSource("edit"))
	if changedErr != nil {
		t.Fatal(changedErr)
	}
	if history, err := db.History(ctx, "history", "item/value"); err != nil || len(history) != 2 || history[0].RxAt != changed.At || history[0].RxBy != "editor" {
		t.Fatalf("augmented history = %#v, %v", history, err)
	}
	for _, call := range []func() error{
		func() error { _, err := db.History(ctx, "history", "missing/attr"); return err },
		func() error { _, err := db.Why(ctx, "history", "missing/attr"); return err },
	} {
		if err := call(); !errors.Is(err, ErrNotFound) {
			t.Errorf("missing temporal attr error = %v", err)
		}
	}
	if _, err := db.Diff(ctx, changed.Tx, seed.Tx); !errors.Is(err, ErrType) {
		t.Fatalf("reverse diff error = %v", err)
	}
	if diff, err := db.Changes(ctx, seed.Tx, changed.Tx); err != nil || len(diff.Asserted) == 0 || len(diff.Retracted) == 0 {
		t.Fatalf("bounded changes = %+v, %v", diff, err)
	}
	if err := db.Speculate(ctx, nil); !errors.Is(err, ErrType) {
		t.Fatalf("nil speculate error = %v", err)
	}
	sentinel := errors.New("callback failed")
	if err := db.Speculate(ctx, func(*DB) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("callback error = %v", err)
	}
	if _, err := db.Undo(ctx, 999_999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown undo error = %v", err)
	}
	metadataOnly, metadataErr := db.Transact(ctx, E{"id": "history", "item/value": "two"}, WithBy("metadata"))
	if metadataErr != nil {
		t.Fatal(metadataErr)
	}
	optionCalls := 0
	statefulBy := TxOption(func(options *txOptions) {
		optionCalls++
		by := "metadata-undo"
		options.by = &by
	})
	if report, err := db.Undo(ctx, metadataOnly.Tx, statefulBy, WithOperationID("undo:metadata")); err != nil || report.Tx == 0 || report.Status != "applied" {
		t.Fatalf("empty undo = %+v, %v", report, err)
	}
	if optionCalls != 1 {
		t.Fatalf("stateful undo option calls = %d, want 1", optionCalls)
	}

	all, allErr := db.Transact(ctx, E{
		"id": "undo-all", "all/ref": RefTo("history"), "all/instant": Instant(1),
		"all/bytes": Bytes([]byte{1}), "all/vector": Vector([]float32{1}), "all/json": JSON(E{"x": 1}), "all/text": "x",
	})
	if allErr != nil {
		t.Fatal(allErr)
	}
	if report, err := db.Undo(ctx, all.Tx); err != nil || report.Tx == 0 || len(report.Retracted) == 0 {
		t.Fatalf("all-tag undo = %+v, %v", report, err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	select {
	case _, ok := <-db.Follow(canceled, FollowOptions{Since: changed.Tx, Interval: time.Millisecond}):
		if ok {
			t.Fatal("canceled follower emitted an event")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled follower did not close")
	}
	closed := fixedDB(t, ":memory:")
	closeTest(t, closed)
	select {
	case event, ok := <-closed.Follow(ctx, FollowOptions{}):
		if !ok || !errors.Is(event.Err, ErrFormat) {
			t.Fatalf("closed follower event = %#v, open = %v", event, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("closed follower did not close")
	}
}

func TestHistoricalDiffChangesAndFollowRespectViewHorizon(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	first, err := db.Transact(ctx, E{"id": "horizon", "item/value": "one"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.Transact(ctx, E{"id": "horizon", "item/value": "two"})
	if err != nil {
		t.Fatal(err)
	}
	view := db.atTx(first.Tx)
	for _, value := range []any{second.Tx, time.UnixMicro(second.At).UTC().Format(time.RFC3339Nano)} {
		nested, nestedErr := view.At(ctx, value)
		if nestedErr != nil {
			t.Fatalf("nested At(%v) = %v", value, nestedErr)
		}
		entity, entityErr := nested.Entity(ctx, "horizon")
		if entityErr != nil || entity["item/value"] != "one" {
			t.Fatalf("nested At(%v) escaped horizon: %#v, %v", value, entity, entityErr)
		}
	}
	diff, err := view.Diff(ctx, GenesisTx, second.Tx)
	if err != nil {
		t.Fatal(err)
	}
	for _, fact := range append(diff.Asserted, diff.Retracted...) {
		if fact.Tx > first.Tx || (fact.Rx != nil && *fact.Rx > first.Tx) {
			t.Fatalf("historical diff leaked future fact %+v", fact)
		}
	}
	changes, err := view.Changes(ctx, first.Tx)
	if err != nil || len(changes.Asserted) != 0 || len(changes.Retracted) != 0 {
		t.Fatalf("changes at historical horizon = %+v, %v", changes, err)
	}
	events := view.Follow(ctx, FollowOptions{})
	event, ok := <-events
	if !ok || !errors.Is(event.Err, ErrUnsupported) {
		t.Fatalf("historical follow event = %+v, open=%t", event, ok)
	}
	if _, open := <-events; open {
		t.Fatal("historical follower remained open after terminal error")
	}
	var exported bytes.Buffer
	if err := view.Tail(ctx, &exported, GenesisTx); err != nil {
		t.Fatal(err)
	}
	if lines := bytes.Count(exported.Bytes(), []byte{'\n'}); lines != 1 {
		t.Fatalf("historical export emitted %d transactions:\n%s", lines, exported.String())
	}
}

func TestHistoryJSONPreservesZeroTimestampAndEmptyProvenance(t *testing.T) {
	ctx := context.Background()
	timestamps := []int64{-1_000_000, 0, 1_000_000}
	index := 0
	db, openErr := Open(":memory:", WithClock(func() int64 {
		value := timestamps[index]
		index++
		return value
	}))
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer closeTest(t, db)
	if _, err := db.Transact(ctx, E{"id": "zero-history", "item/value": "present"}, WithBy(""), WithSource("")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{"retract", "zero-history", "item/value"}, WithBy(""), WithSource("")); err != nil {
		t.Fatal(err)
	}
	history, err := db.History(ctx, "zero-history", "item/value")
	if err != nil || len(history) != 1 {
		t.Fatalf("history = %+v, %v", history, err)
	}
	encoded, err := json.Marshal(history[0])
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeJSON(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	object, ok := objectMap(decoded)
	if !ok || object["at"] != int64(0) || object["by"] != "" || object["source"] != "" ||
		object["rx_at"] != int64(1_000_000) || object["rx_by"] != "" || object["rx_source"] != "" {
		t.Fatalf("presence-aware history JSON = %s", encoded)
	}
}

func TestVisibilityAndRawHelpers(t *testing.T) {
	db := fixedDB(t, ":memory:")
	if query, args := db.visibility("f"); query != "f.rx IS NULL" || args != nil {
		t.Fatalf("current visibility = %q %#v", query, args)
	}
	view := db.atTx(70)
	if query, args := view.visibility("x"); query == "" || !reflect.DeepEqual(args, []any{int64(70), int64(70)}) {
		t.Fatalf("past visibility = %q %#v", query, args)
	}
	if got, err := inputValue(int64(1), TagInt); err != nil || got != int64(1) {
		t.Fatalf("scalar inputValue = %#v", got)
	}
	if got := sqliteBool([]byte("1")); got {
		t.Fatalf("unexpected SQLite bool conversion")
	}
	if got := asInt64(int(4)); got != 4 || asInt64(true) != 1 || asInt64(false) != 0 {
		t.Fatalf("asInt64 supported values = %d/%d/%d", got, asInt64(true), asInt64(false))
	}
	if got := asInt64("bad"); got != 0 {
		t.Fatalf("asInt64 fallback = %d", got)
	}
}
