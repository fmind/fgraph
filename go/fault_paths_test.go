package fgraph

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

func closedRunnerDB(t *testing.T) (*DB, *sql.DB) {
	t.Helper()
	runner, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Ping(); err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
	root := &store{
		sql: runner, path: ":memory:", clock: func() int64 { return 1_767_225_600_000_000 },
		names: map[string]int64{"known": 65, "item/value": 66, "item/vector": 67},
	}
	return &DB{store: root, exec: runner}, runner
}

func TestTypedDatabaseFailurePaths(t *testing.T) {
	ctx := context.Background()
	db, runner := closedRunnerDB(t)
	wantKind := func(name string, want, err error) {
		t.Helper()
		if !errors.Is(err, want) {
			t.Errorf("%s error = %v, want %v", name, err, want)
		}
	}

	_, err := db.queryFacts(ctx, runner)
	wantKind("queryFacts", ErrFormat, err)
	_, err = db.keywordCandidatesBounded(ctx, runner, "word", nil, nil, searchCandidateLimit, &searchWork{limit: 10})
	wantKind("keywordCandidatesBounded", ErrQuery, err)
	_, err = db.vectorCandidatesBounded(ctx, runner, []float32{1}, "", nil, searchCandidateLimit, &searchWork{limit: 10})
	wantKind("vectorCandidatesBounded", ErrFormat, err)
	_, _, err = db.expandSearch(ctx, runner, []int64{65}, 1)
	wantKind("expandSearch", ErrFormat, err)
	_, _, err = db.resolveSearchFilters(ctx, runner, [][]any{{"item/value", int64(1)}})
	wantKind("resolveSearchFilters", ErrFormat, err)
	_, err = db.transactionInfo(ctx, runner, 65)
	wantKind("transactionInfo", ErrFormat, err)
	_, err = db.rangeFacts(ctx, runner, "tx", 64, 65)
	wantKind("rangeFacts", ErrFormat, err)
	_, _, err = db.resolveNumericEntity(ctx, runner, 65)
	wantKind("resolveNumericEntity", ErrFormat, err)
	_, err = db.pullEntity(ctx, runner, 65, 1, map[int64]bool{})
	wantKind("pullEntity", ErrFormat, err)
	_, err = newAllocator(ctx, runner, db.store)
	wantKind("newAllocator", ErrFormat, err)
	_, err = db.attributeName(ctx, runner, 66)
	wantKind("attributeName", ErrFormat, err)
	_, err = db.liveFacts(ctx, runner, 65, nil)
	wantKind("liveFacts", ErrFormat, err)
	_, _, err = db.uniqueOwner(ctx, runner, 66, storedValue{storage: int64(1), tag: TagInt})
	wantKind("uniqueOwner", ErrFormat, err)
	_, err = db.schemaFor(ctx, runner, 66, nil)
	wantKind("schemaFor", ErrFormat, err)
	_, err = db.nextTimestamp(ctx, runner, nil)
	wantKind("nextTimestamp", ErrFormat, err)
	_, err = db.insertFact(ctx, runner, plannedFact{e: 65, a: 66, attr: "item/value", value: storedValue{storage: 1, tag: TagInt}}, 68)
	wantKind("insertFact", ErrConflict, err)
	_, err = db.insertFact(ctx, runner, plannedFact{e: 65, a: 66, attr: "item/value", value: storedValue{blob: "blob", hash: []byte{1}, storage: []byte{1}, tag: TagTextRef}}, 68)
	wantKind("insertBlob", ErrFormat, err)

	_, err = db.Entity(ctx, "known")
	wantKind("Entity", ErrFormat, err)
	_, err = db.RawFacts(ctx, true)
	wantKind("RawFacts", ErrFormat, err)
	_, err = db.Stats(ctx)
	wantKind("Stats", ErrFormat, err)
	_, err = db.History(ctx, "known")
	wantKind("History", ErrFormat, err)
	_, err = db.Why(ctx, "known")
	wantKind("Why", ErrFormat, err)
	_, err = db.AtInstant(ctx, 1)
	wantKind("AtInstant", ErrFormat, err)
	_, err = db.ViewAt(ctx, int64(65))
	wantKind("ViewAt", ErrFormat, err)
	_, err = db.Diff(ctx, 64, 65)
	wantKind("Diff", ErrFormat, err)
	_, err = db.Changes(ctx, 64)
	wantKind("Changes", ErrFormat, err)
	_, err = db.Undo(ctx, 65)
	wantKind("Undo", ErrFormat, err)
	_, err = db.latestTx(ctx)
	wantKind("latestTx", ErrFormat, err)
	_, err = db.Search(ctx, SearchOpts{Text: "word"})
	wantKind("Search", ErrQuery, err)
	_, err = db.Transact(ctx, E{"id": "new", "item/value": 1})
	wantKind("Transact", ErrFormat, err)
	_, err = db.Declare(ctx, "item/value", Type("int"))
	wantKind("Declare", ErrFormat, err)
	if err := db.Tail(ctx, &bytes.Buffer{}, GenesisTx); !errors.Is(err, ErrFormat) {
		t.Errorf("event stream error = %v", err)
	}
	if _, err := db.eventRecordForTx(ctx, 65); !errors.Is(err, ErrNotFound) {
		t.Errorf("eventRecordForTx error = %v", err)
	}
	if _, err := db.Apply(ctx, strings.NewReader(`{"fgraph":"event/1","event":"11111111-1111-4111-8111-111111111111","at":1,"created":[],"asserted":[],"retracted":[]}`+"\n")); !errors.Is(err, ErrFormat) {
		t.Errorf("Apply error = %v", err)
	}
	if err := db.Backup(ctx, t.TempDir()+"/backup.db"); !errors.Is(err, ErrFormat) {
		t.Errorf("Backup error = %v", err)
	}
	if _, err := db.Doctor(ctx); !errors.Is(err, ErrFormat) {
		t.Errorf("Doctor error = %v", err)
	}
	if _, err := db.Excise(ctx, "known"); !errors.Is(err, ErrFormat) {
		t.Errorf("Excise error = %v", err)
	}
	if err := db.Speculate(ctx, func(*DB) error { return nil }); !errors.Is(err, ErrUnsupported) {
		t.Errorf("Speculate error = %v", err)
	}

	select {
	case event, ok := <-db.Follow(ctx, FollowOptions{Interval: time.Millisecond}):
		if !ok || !errors.Is(event.Err, ErrFormat) {
			t.Fatalf("failed follower event = %#v, open = %v", event, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("failed follower did not close")
	}
}

func TestClosedRootConnectionPaths(t *testing.T) {
	ctx := context.Background()
	db, runner := closedRunnerDB(t)
	db.exec = nil
	if err := db.withRead(ctx, func(sqlRunner) error { return nil }); !errors.Is(err, ErrFormat) {
		t.Fatalf("withRead closed pool error = %v", err)
	}
	if err := db.store.refreshNames(ctx, runner); !errors.Is(err, ErrFormat) {
		t.Fatalf("refreshNames closed pool error = %v", err)
	}
	if err := db.Speculate(ctx, func(*DB) error { return nil }); !errors.Is(err, ErrFormat) {
		t.Fatalf("Speculate closed pool error = %v", err)
	}
}
