package fgraph

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMCPOutputAndShutdownBoundaries(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", WithClock(func() int64 { return 1_767_225_600_000_000 }))
	if err != nil {
		t.Fatal(err)
	}

	if _, envelope, err := toolMCPOutput(ctx, db, map[string]any{"value": int64(1)}, nil, GenesisTx); err != nil {
		t.Fatalf("basis-pinned MCP output: %v", err)
	} else if output, ok := envelope.(mcpEnvelope); !ok || output.BasisTx != GenesisTx {
		t.Fatalf("basis-pinned MCP envelope = %#v", envelope)
	}
	if _, _, err := boundedMCPOutput(GenesisTx, make(chan int), nil); !errors.Is(err, ErrFormat) {
		t.Fatalf("unsupported MCP output error = %v, want FormatError", err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := toolMCPOutput(ctx, db, nil, fail(ErrFormat, "closed MCP database"), GenesisTx); !errors.Is(err, ErrFormat) {
		t.Fatalf("MCP result error = %v, want FormatError", err)
	}
	if _, err := db.mcpResourceView(ctx, GenesisTx); !errors.Is(err, ErrFormat) {
		t.Fatalf("closed MCP resource view error = %v, want FormatError", err)
	}
}

func TestMCPReadBasisIsPinnedBeforeEvaluation(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	seeded, err := db.Transact(ctx, E{"id": "pinned/entity", "pinned/value": "seen"})
	if err != nil {
		t.Fatal(err)
	}
	view, basis, err := mcpPinnedView(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if basis != seeded.Tx {
		t.Fatalf("pinned basis = %d, want %d", basis, seeded.Tx)
	}
	advanced, err := db.Transact(ctx, E{"id": "pinned/entity", "pinned/new": "unseen"})
	if err != nil {
		t.Fatal(err)
	}
	entity, err := view.Entity(ctx, "pinned/entity")
	if err != nil {
		t.Fatal(err)
	}
	if entity["pinned/value"] != "seen" || entity["pinned/new"] != nil {
		t.Fatalf("pinned entity at %d after tx %d = %#v", basis, advanced.Tx, entity)
	}
	if got := mcpReportBasis(TxReport{Tx: advanced.Tx, BasisTx: basis}); got != advanced.Tx {
		t.Fatalf("applied report basis = %d, want %d", got, advanced.Tx)
	}
	if got := mcpReportBasis(TxReport{BasisTx: basis}); got != basis {
		t.Fatalf("no-op report basis = %d, want %d", got, basis)
	}
}

func TestRunMCPCanceledContext(t *testing.T) {
	db := fixedDB(t, ":memory:")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := RunMCP(ctx, db, MCPOptions{ReadOnly: true}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled MCP server error = %v, want context cancellation", err)
	}
}

func TestCLIPortabilityAndMCPFailures(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	dir := t.TempDir()
	path := filepath.Join(dir, "source.db")
	if _, err := runCLIForTest(t, "", "init", "--db", path); err != nil {
		t.Fatal(err)
	}

	if _, err := runCLIForTest(t, "", "mcp", "--db", path, "--write", "--read-only"); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("conflicting MCP modes error = %v", err)
	}
	if _, err := runCLIForTest(t, "", "mcp", "--db", filepath.Join(dir, "missing.db"), "--read-only"); !errors.Is(err, ErrFormat) {
		t.Fatalf("missing read-only MCP database error = %v, want FormatError", err)
	}
	if _, err := runCLIForTest(t, "not a snapshot\n", "restore", "--db", filepath.Join(dir, "restore.db")); !errors.Is(err, ErrType) {
		t.Fatalf("malformed restore error = %v, want TypeError", err)
	}
	if _, err := runCLIForTest(t, "", "backup", "--db", path, filepath.Join(dir, "missing", "backup.db")); !errors.Is(err, ErrFormat) {
		t.Fatalf("unwritable backup error = %v, want FormatError", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	if err := RunCLI(ctx, []string{"fgraph", "mcp", "--db", path, "--read-only"}, strings.NewReader(""), &stdout, &stderr); err == nil || (!errors.Is(err, context.Canceled) && !errors.Is(err, os.ErrClosed)) {
		t.Fatalf("canceled MCP CLI error = %v, want typed cancellation or closed-input error", err)
	}
}
