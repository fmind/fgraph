package fgraph

import (
	"context"
	"errors"
	"testing"
)

func TestApplySchemaManifestDiscoversReplacementAfterConcurrentWriter(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/schema-race.db"
	target := fixedDB(t, path)
	concurrent := fixedDB(t, path)
	if _, err := target.Declare(ctx, "stale/attribute", Type("text")); err != nil {
		t.Fatal(err)
	}

	var concurrentReport TxReport
	var concurrentErr error
	interleave := func(_ *txOptions) {
		concurrentReport, concurrentErr = concurrent.Declare(ctx, "race/new", Type("text"))
	}
	applyReport, err := target.ApplySchemaManifest(ctx, SchemaManifest{
		FGraph:     "schema/1",
		Attributes: []SchemaManifestAttribute{},
		Shapes:     []ShapeInfo{},
	}, interleave)
	if err != nil {
		t.Fatal(err)
	}
	if concurrentErr != nil {
		t.Fatal(concurrentErr)
	}
	if concurrentReport.Tx >= applyReport.Tx {
		t.Fatalf("concurrent tx %d must commit before schema replacement tx %d", concurrentReport.Tx, applyReport.Tx)
	}
	manifest, err := target.SchemaManifest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Attributes) != 0 {
		t.Fatalf("replacement left concurrent declarations behind: %#v", manifest.Attributes)
	}
}

func TestSchemaManifestTransactionErrorPaths(t *testing.T) {
	ctx := context.Background()
	empty := SchemaManifest{
		FGraph:     "schema/1",
		Attributes: []SchemaManifestAttribute{},
		Shapes:     []ShapeInfo{},
	}

	t.Run("historical basis", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		if _, err := db.Declare(ctx, "later/attribute", Type("text")); err != nil {
			t.Fatal(err)
		}
		view, err := db.At(ctx, int64(GenesisTx))
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := view.SchemaManifest(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(manifest.Attributes) != 0 {
			t.Fatalf("genesis manifest includes later declarations: %#v", manifest.Attributes)
		}
	})

	t.Run("basis read", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		if _, err := db.store.sql.ExecContext(ctx, "DROP TABLE fgraph_events"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.SchemaManifest(ctx); !errors.Is(err, ErrFormat) {
			t.Fatalf("manifest basis error = %v, want FormatError", err)
		}
	})

	t.Run("identity read", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		if _, err := db.store.sql.ExecContext(ctx, "DROP TABLE fgraph_ids"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.schemaManifestOn(ctx, db.store.sql); !errors.Is(err, ErrFormat) {
			t.Fatalf("manifest identity error = %v, want FormatError", err)
		}
	})

	t.Run("declaration read", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		if _, err := db.Declare(ctx, "broken/attribute", Type("text")); err != nil {
			t.Fatal(err)
		}
		attribute := db.store.names["broken/attribute"]
		if _, err := db.store.sql.ExecContext(
			ctx,
			"UPDATE fgraph_facts SET t=?,v=? WHERE e=? AND a=8",
			TagInt,
			int64(1),
			attribute,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := db.SchemaManifest(ctx); !errors.Is(err, ErrFormat) {
			t.Fatalf("manifest declaration error = %v, want FormatError", err)
		}
	})

	t.Run("replacement discovery", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		if _, err := db.store.sql.ExecContext(ctx, "DROP TABLE fgraph_facts"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ApplySchemaManifest(ctx, empty); !errors.Is(err, ErrFormat) {
			t.Fatalf("manifest replacement error = %v, want FormatError", err)
		}
	})

	t.Run("transaction cache refresh", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		if _, err := db.store.sql.ExecContext(ctx, "DROP TABLE fgraph_ids"); err != nil {
			t.Fatal(err)
		}
		db.store.dataVersion = -1
		if _, err := db.Transact(ctx, []any{}); !errors.Is(err, ErrFormat) {
			t.Fatalf("transaction cache error = %v, want FormatError", err)
		}
	})

	t.Run("transaction allocator", func(t *testing.T) {
		db := fixedDB(t, ":memory:")
		if _, err := db.store.sql.ExecContext(ctx, "DROP TABLE fgraph_meta"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Transact(ctx, []any{}); !errors.Is(err, ErrFormat) {
			t.Fatalf("transaction allocator error = %v, want FormatError", err)
		}
	})
}
