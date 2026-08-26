package fgraph

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestTransactionalSQLiteFailureRollbackMatrix(t *testing.T) {
	ctx := context.Background()
	testCase := func(name string, prepare func(*testing.T, *DB), write func(*DB) error, want error) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			prepare(t, db)
			if err := write(db); !errors.Is(err, want) {
				t.Fatalf("write error = %v, want %v", err, want)
			}
		})
	}
	seed := func(t *testing.T, db *DB) {
		t.Helper()
		if _, err := db.Transact(ctx, E{"id": "seed", "item/value": 1, "item/text": "text"}); err != nil {
			t.Fatal(err)
		}
	}
	testCase("blob insert", func(t *testing.T, db *DB) {
		_, err := db.store.sql.ExecContext(ctx, "DROP TABLE fgraph_blobs")
		if err != nil {
			t.Fatal(err)
		}
	}, func(db *DB) error {
		_, err := db.Transact(ctx, E{"id": "long", "item/text": strings.Repeat("x", 300)})
		return err
	}, ErrFormat)
	testCase("text index insert", func(t *testing.T, db *DB) {
		_, err := db.store.sql.ExecContext(ctx, "DROP TABLE fgraph_fts")
		if err != nil {
			t.Fatal(err)
		}
	}, func(db *DB) error {
		_, err := db.Transact(ctx, E{"id": "text", "item/text": "indexed"})
		return err
	}, ErrFormat)
	testCase("blob collection", func(t *testing.T, db *DB) {
		if _, err := db.Declare(ctx, "item/vector", Type("vector"), Dims(2)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Transact(ctx, E{"id": "blob", "item/vector": Vector([]float32{1, 0})}); err != nil {
			t.Fatal(err)
		}
		_, err := db.store.sql.ExecContext(ctx, `CREATE TRIGGER fail_blob_collection BEFORE DELETE ON fgraph_blobs
			BEGIN SELECT RAISE(ABORT,'blocked blob collection'); END`)
		if err != nil {
			t.Fatal(err)
		}
	}, func(db *DB) error {
		_, err := db.Transact(ctx, E{"id": "blob", "item/vector": Vector([]float32{0, 1})})
		return err
	}, ErrFormat)
	testCase("allocator flush", func(t *testing.T, db *DB) {
		_, err := db.store.sql.ExecContext(ctx, `CREATE TRIGGER fail_next_id BEFORE UPDATE ON fgraph_meta
			BEGIN SELECT RAISE(ABORT,'blocked next_id'); END`)
		if err != nil {
			t.Fatal(err)
		}
	}, func(db *DB) error {
		_, err := db.Transact(ctx, E{"id": "flush", "item/value": 1})
		return err
	}, ErrFormat)
	testCase("fact update", func(t *testing.T, db *DB) {
		seed(t, db)
		_, err := db.store.sql.ExecContext(ctx, `CREATE TRIGGER fail_fact_update BEFORE UPDATE ON fgraph_facts
			BEGIN SELECT RAISE(ABORT,'blocked update'); END`)
		if err != nil {
			t.Fatal(err)
		}
	}, func(db *DB) error {
		_, err := db.Transact(ctx, E{"id": "seed", "item/value": 2})
		return err
	}, ErrFormat)
	testCase("nohistory delete", func(t *testing.T, db *DB) {
		seed(t, db)
		if _, err := db.Declare(ctx, "item/value", NoHistory()); err != nil {
			t.Fatal(err)
		}
		_, err := db.store.sql.ExecContext(ctx, `CREATE TRIGGER fail_fact_delete BEFORE DELETE ON fgraph_facts
			BEGIN SELECT RAISE(ABORT,'blocked delete'); END`)
		if err != nil {
			t.Fatal(err)
		}
	}, func(db *DB) error {
		_, err := db.Transact(ctx, E{"id": "seed", "item/value": 2})
		return err
	}, ErrFormat)
	testCase("text index delete", func(t *testing.T, db *DB) {
		seed(t, db)
		if _, err := db.store.sql.ExecContext(ctx, "DROP TABLE fgraph_fts"); err != nil {
			t.Fatal(err)
		}
	}, func(db *DB) error {
		_, err := db.Retract(ctx, "seed", "item/text")
		return err
	}, ErrFormat)
}

func TestDoctorSQLiteFailureMatrix(t *testing.T) {
	ctx := context.Background()
	testCase := func(name string, repair bool, prepare func(*testing.T, *DB)) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			prepare(t, db)
			if _, err := db.Doctor(ctx, repair); !errors.Is(err, ErrFormat) {
				t.Fatalf("doctor error = %v", err)
			}
		})
	}
	testCase("missing blobs", false, func(t *testing.T, db *DB) {
		if _, err := db.store.sql.ExecContext(ctx, "DROP TABLE fgraph_blobs"); err != nil {
			t.Fatal(err)
		}
	})
	testCase("blocked orphan delete", true, func(t *testing.T, db *DB) {
		if _, err := db.store.sql.ExecContext(ctx, "INSERT INTO fgraph_blobs(hash,data) VALUES (?,?)", []byte("x"), "orphan"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.store.sql.ExecContext(ctx, `CREATE TRIGGER fail_blob_delete BEFORE DELETE ON fgraph_blobs
			BEGIN SELECT RAISE(ABORT,'blocked blob delete'); END`); err != nil {
			t.Fatal(err)
		}
	})
	testCase("missing FTS", false, func(t *testing.T, db *DB) {
		if _, err := db.store.sql.ExecContext(ctx, "DROP TABLE fgraph_fts"); err != nil {
			t.Fatal(err)
		}
	})
	testCase("malformed FTS", false, func(t *testing.T, db *DB) {
		if _, err := db.store.sql.ExecContext(ctx, "DROP TABLE fgraph_fts"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.store.sql.ExecContext(ctx, "CREATE TABLE fgraph_fts(wrong TEXT)"); err != nil {
			t.Fatal(err)
		}
	})
	testCase("missing facts", false, func(t *testing.T, db *DB) {
		if _, err := db.store.sql.ExecContext(ctx, "DROP VIEW fgraph_now"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.store.sql.ExecContext(ctx, "DROP VIEW fgraph_view"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.store.sql.ExecContext(ctx, "DROP TABLE fgraph_facts"); err != nil {
			t.Fatal(err)
		}
	})
	testCase("missing ids", false, func(t *testing.T, db *DB) {
		if _, err := db.store.sql.ExecContext(ctx, "DROP VIEW fgraph_now"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.store.sql.ExecContext(ctx, "DROP VIEW fgraph_view"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.store.sql.ExecContext(ctx, "DROP TABLE fgraph_ids"); err != nil {
			t.Fatal(err)
		}
	})
}
