package fgraph

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func connectMCPTestClient(t *testing.T, db *DB) (*mcp.ClientSession, context.Context) {
	t.Helper()
	ctx := context.Background()
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, err := NewMCPServer(db, MCPOptions{}).Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTest(t, serverSession) })
	client := mcp.NewClient(&mcp.Implementation{Name: "mcp-contract-test", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTest(t, clientSession) })
	return clientSession, ctx
}

func TestMCPSchemaToolPagesAttributesAndShapesAsOneSequence(t *testing.T) {
	db := fixedDB(t, ":memory:")
	ctx := context.Background()
	for _, attribute := range []string{"page/a", "page/b"} {
		if _, err := db.Declare(ctx, attribute, Type("int")); err != nil {
			t.Fatal(err)
		}
	}
	for _, shape := range []struct {
		name     string
		required string
	}{
		{name: "shape/first", required: "page/a"},
		{name: "shape/second", required: "page/b"},
	} {
		if _, err := db.DeclareShape(ctx, shape.name, ShapeDefinition{Required: []string{shape.required}}); err != nil {
			t.Fatal(err)
		}
	}

	client, ctx := connectMCPTestClient(t, db)
	call := func(arguments map[string]any) mcpSchemaResult {
		t.Helper()
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "schema", Arguments: arguments})
		if err != nil {
			t.Fatal(err)
		}
		if result.IsError {
			t.Fatalf("schema tool failed: %#v", result.Content)
		}
		var page mcpSchemaResult
		decodeMCPData(t, result, &page)
		if len(page.Attributes)+len(page.Shapes) > 2 {
			t.Fatalf("schema page exceeded requested combined limit: %#v", page)
		}
		return page
	}

	first := call(map[string]any{"prefix": "page/", "limit": 2})
	if len(first.Attributes) != 2 || len(first.Shapes) != 0 || first.NextCursor == nil || !first.Truncated {
		t.Fatalf("first schema page = %#v", first)
	}
	if _, err := db.Declare(ctx, "page/later", Type("int")); err != nil {
		t.Fatal(err)
	}
	second := call(map[string]any{"cursor": *first.NextCursor, "limit": 2})
	if len(second.Attributes) != 0 || len(second.Shapes) != 2 || second.NextCursor != nil || second.Truncated {
		t.Fatalf("second schema page = %#v", second)
	}
	if second.BasisTx != first.BasisTx || second.Shapes[0].Name != "shape/first" || second.Shapes[1].Name != "shape/second" {
		t.Fatalf("schema pagination lost its pinned ordered snapshot: first=%#v second=%#v", first, second)
	}
}

func TestMCPDestructiveWritesRecordNegotiatedClientProvenance(t *testing.T) {
	db := fixedDB(t, ":memory:")
	ctx := context.Background()
	seed, err := db.Transact(ctx, E{"id": "provenance/item", "provenance/value": "present"})
	if err != nil {
		t.Fatal(err)
	}

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, err := NewMCPServer(db, MCPOptions{Write: true}).Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTest(t, serverSession) })
	client := mcp.NewClient(&mcp.Implementation{Name: "provenance-client", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTest(t, clientSession) })

	basis, err := db.latestTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	forgetResult, err := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: "forget", Arguments: map[string]any{
		"entity": "provenance/item", "attribute": "provenance/value",
		"operation_id": "mcp:forget-provenance", "if_basis_tx": basis,
	}})
	if err != nil || forgetResult.IsError {
		t.Fatalf("forget result = %#v, %v", forgetResult, err)
	}
	var forgotten TxReport
	decodeMCPData(t, forgetResult, &forgotten)
	assertMCPReceiptBy(t, db, forgotten.Tx, "mcp:provenance-client")

	basis, err = db.latestTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	undoResult, err := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: "undo", Arguments: map[string]any{
		"tx": forgotten.Tx, "operation_id": "mcp:undo-provenance", "if_basis_tx": basis,
	}})
	if err != nil || undoResult.IsError {
		t.Fatalf("undo result = %#v, %v", undoResult, err)
	}
	var undone TxReport
	decodeMCPData(t, undoResult, &undone)
	assertMCPReceiptBy(t, db, undone.Tx, "mcp:provenance-client")
	if undone.Tx <= seed.Tx {
		t.Fatalf("undo report = %#v", undone)
	}
}

func assertMCPReceiptBy(t *testing.T, db *DB, tx int64, want string) {
	t.Helper()
	receipt, err := db.Receipt(context.Background(), tx)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.By == nil || *receipt.By != want {
		t.Fatalf("receipt %d by = %#v, want %q", tx, receipt.By, want)
	}
}

func TestMCPChangesResourceReturnsCompletePortableEvents(t *testing.T) {
	db := fixedDB(t, ":memory:")
	ctx := context.Background()
	facts := E{"id": "changes/wide"}
	for index := range 25 {
		facts[fmt.Sprintf("changes/value-%02d", index)] = int64(index)
	}
	if _, err := db.Transact(ctx, facts); err != nil {
		t.Fatal(err)
	}
	expected, err := db.EventRecords(ctx, GenesisTx)
	if err != nil {
		t.Fatal(err)
	}
	basis, err := db.latestTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want, err := canonicalMCPBytes(map[string]any{"basis_tx": basis, "events": expected})
	if err != nil {
		t.Fatal(err)
	}

	client, ctx := connectMCPTestClient(t, db)
	result, err := client.ReadResource(ctx, &mcp.ReadResourceParams{URI: "fgraph://changes?since=64"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contents) != 1 {
		t.Fatalf("changes resource contents = %#v", result.Contents)
	}
	if got := result.Contents[0].Text; got != string(want) {
		t.Fatalf("changes resource is not the canonical event stream\ngot:  %s\nwant: %s", got, want)
	}
}

func TestMCPChangesRejectsTamperedEventsAndOutOfRangeCursors(t *testing.T) {
	db := fixedDB(t, ":memory:")
	ctx := context.Background()
	report, err := db.Transact(ctx, E{"id": "changes/tamper", "changes/value": "kept"})
	if err != nil {
		t.Fatal(err)
	}
	client, ctx := connectMCPTestClient(t, db)

	empty, err := client.ReadResource(ctx, &mcp.ReadResourceParams{URI: fmt.Sprintf("fgraph://changes?since=%d", report.Tx)})
	if err != nil || len(empty.Contents) != 1 || empty.Contents[0].Text != fmt.Sprintf(`{"basis_tx":%d,"events":[]}`, report.Tx) {
		t.Fatalf("empty change page = %#v, %v", empty, err)
	}
	if _, err := client.ReadResource(ctx, &mcp.ReadResourceParams{URI: "fgraph://changes?since=63"}); err == nil {
		t.Fatal("pre-genesis change boundary was accepted")
	}
	for name, position := range map[string]int64{"before genesis": GenesisTx - 1, "after basis": report.Tx + 1} {
		t.Run(name, func(t *testing.T) {
			cursor, encodeErr := encodeMCPResourceCursor(mcpResourceCursor{
				Version: 1, Resource: "changes", Argument: "64", Basis: report.Tx, Position: position,
			})
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			uri := "fgraph://changes?since=64&cursor=" + url.QueryEscape(cursor)
			if _, err := client.ReadResource(ctx, &mcp.ReadResourceParams{URI: uri}); err == nil {
				t.Fatalf("out-of-range cursor %d was accepted", position)
			}
		})
	}

	if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_events SET event_data='{}' WHERE tx=?", report.Tx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReadResource(ctx, &mcp.ReadResourceParams{URI: "fgraph://changes?since=64"}); err == nil {
		t.Fatal("change resource accepted tampered canonical event data")
	}
}
