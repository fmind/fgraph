package fgraph

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func coveragePublicExec(t *testing.T, db *DB, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		if _, err := db.store.sql.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}
}

func coveragePublicFactID(t *testing.T, report TxReport, attribute string) int64 {
	t.Helper()
	for _, fact := range report.Asserted {
		if fact.A == attribute {
			return fact.ID
		}
	}
	t.Fatalf("transaction has no %q fact: %+v", attribute, report)
	return 0
}

func coveragePublicCorruptTag(t *testing.T, db *DB, factID int64) {
	t.Helper()
	coveragePublicExec(t, db, "PRAGMA ignore_check_constraints=ON")
	if _, err := db.store.sql.ExecContext(context.Background(), "UPDATE fgraph_facts SET t=99 WHERE id=?", factID); err != nil {
		t.Fatal(err)
	}
}

func TestCoveragePublicMaintenanceFailuresAreAtomic(t *testing.T) {
	ctx := context.Background()

	t.Run("backup rejects an uninspectable destination", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		loop := filepath.Join(t.TempDir(), "loop")
		if err := os.Symlink("loop", loop); err != nil {
			t.Fatal(err)
		}
		if err := db.Backup(ctx, loop); !errors.Is(err, ErrConflict) {
			t.Fatalf("backup symlink loop error = %v", err)
		}
	})

	t.Run("integrity check reports a closed connection", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		conn, err := db.store.sql.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := readIntegrityCheck(ctx, conn); !errors.Is(err, ErrFormat) {
			t.Fatalf("closed integrity connection error = %v", err)
		}
	})

	for name, prepare := range map[string]func(*testing.T, *DB, TxReport){
		"classification query": func(t *testing.T, db *DB, _ TxReport) {
			coveragePublicExec(t, db, "DROP VIEW fgraph_now", "DROP VIEW fgraph_view", "DROP TABLE fgraph_facts")
		},
		"allocator metadata": func(t *testing.T, db *DB, _ TxReport) {
			coveragePublicExec(t, db, "DROP TABLE fgraph_meta")
		},
		"fact rendering": func(t *testing.T, db *DB, report TxReport) {
			coveragePublicCorruptTag(t, db, coveragePublicFactID(t, report, "item/value"))
		},
		"text index deletion": func(t *testing.T, db *DB, _ TxReport) {
			coveragePublicExec(t, db, "DROP TABLE fgraph_fts")
		},
		"fact deletion": func(t *testing.T, db *DB, _ TxReport) {
			coveragePublicExec(t, db, `CREATE TRIGGER block_excise BEFORE DELETE ON fgraph_facts
				BEGIN SELECT RAISE(ABORT,'blocked excise'); END`)
		},
		"blob collection": func(t *testing.T, db *DB, _ TxReport) {
			coveragePublicExec(t, db, "DROP TABLE fgraph_blobs")
		},
		"receipt insertion": func(t *testing.T, db *DB, _ TxReport) {
			coveragePublicExec(t, db, `CREATE TRIGGER block_receipt BEFORE INSERT ON fgraph_facts
				BEGIN SELECT RAISE(ABORT,'blocked receipt'); END`)
		},
		"allocator flush": func(t *testing.T, db *DB, _ TxReport) {
			coveragePublicExec(t, db, `CREATE TRIGGER block_allocator BEFORE UPDATE ON fgraph_meta
				BEGIN SELECT RAISE(ABORT,'blocked allocator'); END`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			report, err := db.Transact(ctx, E{"id": "excise-me", "item/value": "indexed text"})
			if err != nil {
				t.Fatal(err)
			}
			prepare(t, db, report)
			if _, err := db.Excise(ctx, "excise-me"); err == nil {
				t.Fatal("corrupt excision unexpectedly succeeded")
			}
		})
	}

	t.Run("timestamp validation", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		if _, err := db.Transact(ctx, E{"id": "excise-me", "item/value": 1}); err != nil {
			t.Fatal(err)
		}
		db.store.clock = func() int64 { return maxInstantMicros + 1 }
		if _, err := db.Excise(ctx, "excise-me"); !errors.Is(err, ErrType) {
			t.Fatalf("invalid excision timestamp error = %v", err)
		}
	})

	t.Run("invalid selector", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		if _, err := db.Excise(ctx, true); !errors.Is(err, ErrType) {
			t.Fatalf("invalid excision selector error = %v", err)
		}
	})
}

func TestCoveragePublicReadCompositionAndCorruption(t *testing.T) {
	ctx := context.Background()

	t.Run("recursive pull propagates child corruption", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		if _, err := db.Declare(ctx, "node/link", Ref()); err != nil {
			t.Fatal(err)
		}
		child, err := db.Transact(ctx, E{"id": "child", "node/name": "child"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Transact(ctx, E{"id": "parent", "node/link": RefTo("child")}); err != nil {
			t.Fatal(err)
		}
		coveragePublicCorruptTag(t, db, coveragePublicFactID(t, child, "node/name"))
		if _, err := db.Entity(ctx, "parent", 3); !errors.Is(err, ErrFormat) {
			t.Fatalf("recursive corrupt pull error = %v", err)
		}
	})

	t.Run("schema corruption is not hidden", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		if _, err := db.Declare(ctx, "node/name", Type("text")); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Transact(ctx, E{"id": "node", "node/name": "value"}); err != nil {
			t.Fatal(err)
		}
		attribute := db.store.names["node/name"]
		if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_facts SET v=1 WHERE e=? AND a=8 AND rx IS NULL", attribute); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Entity(ctx, "node"); !errors.Is(err, ErrFormat) {
			t.Fatalf("corrupt schema pull error = %v", err)
		}
	})

	t.Run("empty allocated entity and statistics failure", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		var empty map[string]any
		if err := db.withRead(ctx, func(runner sqlRunner) error {
			var err error
			empty, err = db.pullEntity(ctx, runner, 999_999, 1, map[int64]bool{})
			return err
		}); err != nil || empty != nil {
			t.Fatalf("unknown internal pull = %#v, %v", empty, err)
		}

		coveragePublicExec(t, db, "DROP VIEW fgraph_now", "DROP VIEW fgraph_view", "DROP TABLE fgraph_facts")
		view := &DB{store: db.store, exec: db.store.sql}
		if _, err := view.Stats(ctx); !errors.Is(err, ErrFormat) {
			t.Fatalf("statistics over corrupt storage error = %v", err)
		}
	})

	t.Run("cache refresh failure", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		coveragePublicExec(t, db, "DROP TABLE fgraph_ids")
		db.store.dataVersion = -2
		if _, err := db.Stats(ctx); !errors.Is(err, ErrFormat) {
			t.Fatalf("statistics cache refresh error = %v", err)
		}
	})
}

func TestCoveragePublicSemanticSearchBoundaries(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "search/vector", Type("vector"), Dims(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "text", "search/text": "needle"}); err != nil {
		t.Fatal(err)
	}

	for name, options := range map[string]SearchOpts{
		"unknown attribute":    {Vector: []float32{1, 0}, VectorAttribute: "missing/vector"},
		"non-vector attribute": {Vector: []float32{1, 0}, VectorAttribute: "search/text"},
		"declared dimensions":  {Vector: []float32{1}, VectorAttribute: "search/vector"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.Search(ctx, options); err == nil {
				t.Fatalf("invalid semantic search %#v succeeded", options)
			}
		})
	}

	items := make([]any, 0, 55)
	for index := range 55 {
		items = append(items, E{"id": "vector-" + string(rune('A'+index)), "search/vector": Vector([]float32{1, 0})})
	}
	if _, err := db.Transact(ctx, items); err != nil {
		t.Fatal(err)
	}
	result, err := db.Search(ctx, SearchOpts{Vector: []float32{1, 0}, VectorAttribute: "search/vector", K: 100})
	if err != nil || len(result.Hits) != 55 || result.Truncated {
		t.Fatalf("semantic candidate cap = %d, %v", len(result.Hits), err)
	}

	t.Run("stored dimension mismatch", func(t *testing.T) {
		other := fixedDB(t, ":memory:")
		if _, err := other.Transact(ctx, E{"id": "vector", "search/vector": Vector([]float32{1, 0})}); err != nil {
			t.Fatal(err)
		}
		attribute := other.store.names["search/vector"]
		if _, err := other.store.sql.ExecContext(ctx, "DELETE FROM fgraph_facts WHERE e=? AND a=9", attribute); err != nil {
			t.Fatal(err)
		}
		if _, err := other.Search(ctx, SearchOpts{Vector: []float32{1}, VectorAttribute: "search/vector"}); !errors.Is(err, ErrType) {
			t.Fatalf("stored vector dimension error = %v", err)
		}
	})

	t.Run("expansion propagates neighbor corruption", func(t *testing.T) {
		other := fixedDB(t, ":memory:")
		if _, err := other.Declare(ctx, "graph/link", Ref()); err != nil {
			t.Fatal(err)
		}
		neighbor, err := other.Transact(ctx, E{"id": "neighbor", "graph/name": "broken"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := other.Transact(ctx, E{"id": "seed", "graph/text": "needle", "graph/link": RefTo("neighbor")}); err != nil {
			t.Fatal(err)
		}
		coveragePublicCorruptTag(t, other, coveragePublicFactID(t, neighbor, "graph/name"))
		if _, err := other.Search(ctx, SearchOpts{Text: "needle", Expand: 1}); !errors.Is(err, ErrFormat) {
			t.Fatalf("expanded corrupt neighbor error = %v", err)
		}
	})
}

func TestCoveragePublicTemporalFailuresAndReversal(t *testing.T) {
	ctx := context.Background()

	for name, inspect := range map[string]func(context.Context, *DB) error{
		"history": func(ctx context.Context, db *DB) error { _, err := db.History(ctx, true); return err },
		"why":     func(ctx context.Context, db *DB) error { _, err := db.Why(ctx, true); return err },
	} {
		t.Run(name+" rejects an invalid selector", func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			if err := inspect(ctx, db); !errors.Is(err, ErrType) {
				t.Fatalf("invalid temporal selector error = %v", err)
			}
		})
	}

	t.Run("history rejects corrupt transaction attribution", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		report, err := db.Transact(ctx, E{"id": "history", "item/value": 1}, WithBy("writer"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_facts SET v=1,t=2 WHERE id=?", coveragePublicFactID(t, report, "fgraph/by")); err != nil {
			t.Fatal(err)
		}
		if _, err := db.History(ctx, "history"); !errors.Is(err, ErrFormat) {
			t.Fatalf("corrupt history attribution error = %v", err)
		}
	})

	t.Run("why rejects corrupt transaction source", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		report, err := db.Transact(ctx, E{"id": "why", "item/value": 1}, WithSource("source"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_facts SET v=1,t=2 WHERE id=?", coveragePublicFactID(t, report, "fgraph/source")); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Why(ctx, "why"); !errors.Is(err, ErrFormat) {
			t.Fatalf("corrupt why source error = %v", err)
		}
	})

	t.Run("undo restores a retraction", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		if _, err := db.Transact(ctx, E{"id": "undo", "item/value": "before"}); err != nil {
			t.Fatal(err)
		}
		changed, err := db.Transact(ctx, E{"id": "undo", "item/value": "after"})
		if err != nil {
			t.Fatal(err)
		}
		undone, err := db.Undo(ctx, changed.Tx)
		if err != nil || len(undone.Asserted) == 0 || len(undone.Retracted) == 0 {
			t.Fatalf("undo replacement = %+v, %v", undone, err)
		}
		entity, err := db.Entity(ctx, "undo")
		if err != nil || entity["item/value"] != "before" {
			t.Fatalf("entity after undo = %#v, %v", entity, err)
		}
	})

	t.Run("follow surfaces a temporal decoding failure", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		report, err := db.Transact(ctx, E{"id": "follow", "item/value": 1})
		if err != nil {
			t.Fatal(err)
		}
		coveragePublicCorruptTag(t, db, coveragePublicFactID(t, report, "item/value"))
		events := db.Follow(ctx, FollowOptions{Since: GenesisTx, Interval: time.Millisecond})
		select {
		case event, ok := <-events:
			if !ok || !errors.Is(event.Err, ErrFormat) {
				t.Fatalf("corrupt follow event = %+v, open=%t", event, ok)
			}
		case <-time.After(time.Second):
			t.Fatal("corrupt follower did not terminate")
		}
	})

	t.Run("speculation reports rollback failure", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		err := db.Speculate(ctx, func(view *DB) error {
			conn, ok := view.exec.(*sql.Conn)
			if !ok {
				t.Fatalf("speculative runner type = %T", view.exec)
			}
			return conn.Close()
		})
		if !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "roll back speculation") {
			t.Fatalf("closed speculation error = %v", err)
		}
	})

	for name, input := range map[string]struct {
		logical any
		tag     Tag
	}{
		"bytes":  {logical: "not bytes", tag: TagBytes},
		"vector": {logical: "not vector", tag: TagVector},
	} {
		t.Run("malformed "+name+" undo value", func(t *testing.T) {
			if _, err := inputValue(input.logical, input.tag); !errors.Is(err, ErrFormat) {
				t.Fatalf("malformed undo value error = %v", err)
			}
		})
	}
}
