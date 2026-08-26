package fgraph

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestReadRejectsTamperedIndirectBlob(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{"id": "value", "value/data": strings.Repeat("x", BlobThreshold+1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_blobs SET data=?", strings.Repeat("y", BlobThreshold+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Entity(ctx, "value"); !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "content-addressed hash") {
		t.Fatalf("tampered blob error = %v, want FormatError", err)
	}
}
