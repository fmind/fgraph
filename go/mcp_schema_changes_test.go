package fgraph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"strings"
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

type mcpOversizedEvent struct {
	Event     string `json:"event"`
	EventHash string `json:"event_hash"`
	URI       string `json:"uri"`
	Bytes     int    `json:"bytes"`
}

type mcpChangesResourcePage struct {
	OversizedEvent *mcpOversizedEvent `json:"oversized_event"`
	NextURI        string             `json:"next_uri"`
	Events         []map[string]any   `json:"events"`
	BasisTx        int64              `json:"basis_tx"`
}

type mcpEventResourceChunk struct {
	NextURI   string `json:"next_uri"`
	EventHash string `json:"event_hash"`
	Encoding  string `json:"encoding"`
	Event     string `json:"event"`
	Data      string `json:"data"`
	BasisTx   int64  `json:"basis_tx"`
	Offset    int    `json:"offset"`
}

func readMCPResourceJSON(
	t *testing.T,
	client *mcp.ClientSession,
	ctx context.Context,
	uri string,
	destination any,
) map[string]json.RawMessage {
	t.Helper()
	result, err := client.ReadResource(ctx, &mcp.ReadResourceParams{URI: uri})
	if err != nil {
		t.Fatalf("read resource %q: %v", uri, err)
	}
	if len(result.Contents) != 1 {
		t.Fatalf("resource %q contents = %#v", uri, result.Contents)
	}
	if err := json.Unmarshal([]byte(result.Contents[0].Text), destination); err != nil {
		t.Fatalf("decode resource %q: %v", uri, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(result.Contents[0].Text), &fields); err != nil {
		t.Fatalf("decode resource fields %q: %v", uri, err)
	}
	return fields
}

func TestMCPChangesChunksOversizedEventWithoutBlockingLaterEvents(t *testing.T) {
	db := fixedDB(t, ":memory:")
	ctx := context.Background()
	large, err := db.Transact(ctx, E{
		"id": "changes/oversized", "changes/payload": strings.Repeat("x", 300<<10),
	})
	if err != nil {
		t.Fatal(err)
	}
	later, err := db.Transact(ctx, E{"id": "changes/later", "changes/value": "reachable"})
	if err != nil {
		t.Fatal(err)
	}
	records, err := db.EventRecords(ctx, GenesisTx, large.Tx)
	if err != nil || len(records) != 1 {
		t.Fatalf("large event records = %#v, %v", records, err)
	}
	expected, err := canonicalJSON(records[0])
	if err != nil {
		t.Fatal(err)
	}
	expectedDigest := sha256.Sum256(expected)

	client, ctx := connectMCPTestClient(t, db)
	var first mcpChangesResourcePage
	fields := readMCPResourceJSON(t, client, ctx, "fgraph://changes?since=64", &first)
	if len(fields) != 4 || first.BasisTx != later.Tx || len(first.Events) != 0 || first.OversizedEvent == nil || first.NextURI == "" {
		t.Fatalf("oversized changes page = %#v, fields=%v", first, fields)
	}
	pointer := first.OversizedEvent
	if pointer.Event != large.EventID || pointer.EventHash != hex.EncodeToString(expectedDigest[:]) || pointer.Bytes != len(expected) {
		t.Fatalf("oversized event pointer = %#v", pointer)
	}
	if len(pointer.EventHash) != 64 || pointer.EventHash != strings.ToLower(pointer.EventHash) || strings.HasPrefix(pointer.EventHash, "sha256:") {
		t.Fatalf("oversized event digest = %q", pointer.EventHash)
	}

	// Offset is optional on the first request and must default to byte zero.
	firstChunkURI, err := url.Parse(pointer.URI)
	if err != nil {
		t.Fatal(err)
	}
	query := firstChunkURI.Query()
	query.Del("offset")
	firstChunkURI.RawQuery = query.Encode()

	assembled := make([]byte, 0, len(expected))
	chunkURI := firstChunkURI.String()
	for chunkURI != "" {
		var chunk mcpEventResourceChunk
		chunkFields := readMCPResourceJSON(t, client, ctx, chunkURI, &chunk)
		wantFields := 6
		if chunk.NextURI != "" {
			wantFields++
		}
		if len(chunkFields) != wantFields || chunk.BasisTx != later.Tx || chunk.Event != large.EventID || chunk.EventHash != pointer.EventHash || chunk.Encoding != "base64" || chunk.Offset != len(assembled) {
			t.Fatalf("event chunk = %#v, fields=%v", chunk, chunkFields)
		}
		decoded, decodeErr := base64.StdEncoding.DecodeString(chunk.Data)
		if decodeErr != nil || len(decoded) == 0 || len(decoded) > maxMCPEventChunkBytes {
			t.Fatalf("event chunk data = %d bytes, %v", len(decoded), decodeErr)
		}
		assembled = append(assembled, decoded...)
		chunkURI = chunk.NextURI
	}
	if !bytes.Equal(assembled, expected) {
		t.Fatalf("reassembled event differs: got %d bytes, want %d", len(assembled), len(expected))
	}
	if digest := sha256.Sum256(assembled); digest != expectedDigest {
		t.Fatalf("reassembled event digest = %x, want %x", digest, expectedDigest)
	}

	var tail mcpChangesResourcePage
	tailFields := readMCPResourceJSON(t, client, ctx, first.NextURI, &tail)
	if len(tailFields) != 2 || tail.BasisTx != later.Tx || len(tail.Events) != 1 || tail.Events[0]["event"] != later.EventID || tail.OversizedEvent != nil || tail.NextURI != "" {
		t.Fatalf("changes after oversized event = %#v, fields=%v", tail, tailFields)
	}

	var nonReceiptBasis int64
	if err := db.store.sql.QueryRowContext(ctx, `SELECT id FROM fgraph_ids
		WHERE id>=? AND id<=? AND NOT EXISTS(SELECT 1 FROM fgraph_events WHERE tx=id)
		ORDER BY id LIMIT 1`, GenesisTx, later.Tx).Scan(&nonReceiptBasis); err != nil {
		t.Fatal(err)
	}
	wrongDigest := "0" + pointer.EventHash[1:]
	if wrongDigest == pointer.EventHash {
		wrongDigest = "1" + pointer.EventHash[1:]
	}
	invalid := map[string]string{
		"missing basis":     fmt.Sprintf("fgraph://event/%s?digest=%s", large.EventID, pointer.EventHash),
		"missing digest":    fmt.Sprintf("fgraph://event/%s?basis=%d", large.EventID, later.Tx),
		"non-receipt basis": mcpEventResourceURI(large.EventID, nonReceiptBasis, 0, pointer.EventHash),
		"invisible event":   mcpEventResourceURI(large.EventID, GenesisTx, 0, pointer.EventHash),
		"uppercase digest":  mcpEventResourceURI(large.EventID, later.Tx, 0, strings.ToUpper(pointer.EventHash)),
		"wrong digest":      mcpEventResourceURI(large.EventID, later.Tx, 0, wrongDigest),
		"negative offset":   fmt.Sprintf("fgraph://event/%s?basis=%d&digest=%s&offset=-1", large.EventID, later.Tx, pointer.EventHash),
		"outside offset":    mcpEventResourceURI(large.EventID, later.Tx, len(expected), pointer.EventHash),
	}
	for name, uri := range invalid {
		t.Run(name, func(t *testing.T) {
			if _, err := client.ReadResource(ctx, &mcp.ReadResourceParams{URI: uri}); err == nil {
				t.Fatalf("invalid event resource URI was accepted: %s", uri)
			}
		})
	}

	var original string
	if err := db.store.sql.QueryRowContext(ctx, "SELECT event_data FROM fgraph_events WHERE tx=?", large.Tx).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_events SET event_data=? WHERE tx=?", original+" ", large.Tx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReadResource(ctx, &mcp.ReadResourceParams{URI: pointer.URI}); err == nil {
		t.Fatal("event resource accepted non-canonical durable payload")
	}
	if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_events SET event_data=? WHERE tx=?", original, large.Tx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Excise(ctx, "changes/oversized", WithOperationID("mcp:redact-oversized"), IfBasis(later.Tx)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReadResource(ctx, &mcp.ReadResourceParams{URI: pointer.URI}); err == nil {
		t.Fatal("event resource served a redacted payload")
	}
}

func TestMCPFinalOversizedEventHasNoContinuation(t *testing.T) {
	db := fixedDB(t, ":memory:")
	ctx := context.Background()
	if _, err := db.Transact(ctx, E{
		"id": "changes/final-oversized", "changes/payload": strings.Repeat("z", 300<<10),
	}); err != nil {
		t.Fatal(err)
	}
	client, ctx := connectMCPTestClient(t, db)
	var page mcpChangesResourcePage
	fields := readMCPResourceJSON(t, client, ctx, "fgraph://changes?since=64", &page)
	if len(fields) != 3 || len(page.Events) != 0 || page.OversizedEvent == nil || page.NextURI != "" {
		t.Fatalf("final oversized changes page = %#v, fields=%v", page, fields)
	}
}

func TestMCPChangesEnforcesAggregateEventByteBudget(t *testing.T) {
	db := fixedDB(t, ":memory:")
	ctx := context.Background()
	for index := range 2 {
		if _, err := db.Transact(ctx, E{
			"id":              fmt.Sprintf("changes/budget-%d", index),
			"changes/payload": strings.Repeat(string(rune('a'+index)), 110<<10),
		}); err != nil {
			t.Fatal(err)
		}
	}
	client, ctx := connectMCPTestClient(t, db)
	var first mcpChangesResourcePage
	readMCPResourceJSON(t, client, ctx, "fgraph://changes?since=64", &first)
	if len(first.Events) != 1 || first.NextURI == "" || first.OversizedEvent != nil {
		t.Fatalf("first byte-budgeted page = %#v", first)
	}
	firstRaw, err := canonicalJSON(first.Events[0])
	if err != nil || len(firstRaw) > maxMCPChangesPageBytes {
		t.Fatalf("first page event bytes = %d, %v", len(firstRaw), err)
	}
	var second mcpChangesResourcePage
	readMCPResourceJSON(t, client, ctx, first.NextURI, &second)
	if len(second.Events) != 1 || second.NextURI != "" || second.OversizedEvent != nil {
		t.Fatalf("second byte-budgeted page = %#v", second)
	}
	secondRaw, err := canonicalJSON(second.Events[0])
	if err != nil || len(firstRaw)+len(secondRaw) <= maxMCPChangesPageBytes {
		t.Fatalf("aggregate event bytes = %d, %v", len(firstRaw)+len(secondRaw), err)
	}
}

func TestMCPEventResourceHelperIntegrityBoundaries(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	report, transactErr := db.Transact(ctx, E{"id": "event/helper", "event/value": "original"})
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	if _, err := db.Transact(ctx, E{"id": "event/later", "event/value": "later"}); err != nil {
		t.Fatal(err)
	}
	records, recordsErr := db.EventRecords(ctx, GenesisTx, report.Tx)
	if recordsErr != nil || len(records) != 1 {
		t.Fatalf("event records = %#v, %v", records, recordsErr)
	}
	original, canonicalErr := canonicalJSON(records[0])
	if canonicalErr != nil {
		t.Fatal(canonicalErr)
	}
	originalDigest := sha256.Sum256(original)
	eventUUID, uuidErr := parseUUID(report.EventID)
	if uuidErr != nil {
		t.Fatal(uuidErr)
	}

	validURI, parseErr := url.Parse(mcpEventResourceURI(report.EventID, report.Tx, 0, hex.EncodeToString(originalDigest[:])))
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	historical := db.atTx(report.Tx)
	if _, _, basis, offset, _, err := historical.mcpEventCoordinates(ctx, validURI); err != nil || basis != report.Tx || offset != 0 {
		t.Fatalf("historical event coordinates = basis %d offset %d, %v", basis, offset, err)
	}
	invalidID, parseErr := url.Parse(fmt.Sprintf("fgraph://event/not-a-uuid?basis=%d&digest=%s", report.Tx, hex.EncodeToString(originalDigest[:])))
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	if _, _, _, _, _, err := db.mcpEventCoordinates(ctx, invalidID); !errors.Is(err, ErrType) {
		t.Fatalf("invalid event id error = %v, want TypeError", err)
	}
	futureBasis, parseErr := url.Parse(mcpEventResourceURI(report.EventID, report.Tx+100, 0, hex.EncodeToString(originalDigest[:])))
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	if _, _, _, _, _, err := db.mcpEventCoordinates(ctx, futureBasis); !errors.Is(err, ErrType) {
		t.Fatalf("future event basis error = %v, want TypeError", err)
	}
	closed, openErr := Open(":memory:")
	if openErr != nil {
		t.Fatal(openErr)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, err := closed.mcpEventCoordinates(ctx, validURI); !errors.Is(err, ErrFormat) {
		t.Fatalf("closed event coordinates error = %v, want FormatError", err)
	}

	missingUUID, missingUUIDErr := parseUUID("11111111-1111-4111-8111-111111111111")
	if missingUUIDErr != nil {
		t.Fatal(missingUUIDErr)
	}
	if _, err := db.mcpEventPayload(ctx, formatUUID(missingUUID), missingUUID, report.Tx, hex.EncodeToString(originalDigest[:])); !errors.Is(err, ErrConflict) {
		t.Fatalf("missing event payload error = %v, want Conflict", err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "PRAGMA ignore_check_constraints=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_events SET event_hash=x'00' WHERE tx=?", report.Tx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.mcpEventPayload(ctx, report.EventID, eventUUID, report.Tx, hex.EncodeToString(originalDigest[:])); !errors.Is(err, ErrConflict) {
		t.Fatalf("malformed event hash error = %v, want Conflict", err)
	}

	altered := maps.Clone(records[0])
	altered["event"] = "22222222-2222-4222-8222-222222222222"
	alteredData, alteredErr := canonicalJSON(altered)
	if alteredErr != nil {
		t.Fatal(alteredErr)
	}
	alteredDigest := sha256.Sum256(alteredData)
	if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_events SET event_hash=?,event_data=? WHERE tx=?", alteredDigest[:], string(alteredData), report.Tx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.mcpEventPayload(ctx, report.EventID, eventUUID, report.Tx, hex.EncodeToString(alteredDigest[:])); !errors.Is(err, ErrConflict) {
		t.Fatalf("mismatched event identity error = %v, want Conflict", err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "PRAGMA ignore_check_constraints=OFF"); err != nil {
		t.Fatal(err)
	}
}

func TestMCPEventResourceSQLFaults(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("scripted MCP event fault")
	event := "11111111-1111-4111-8111-111111111111"
	eventUUID, uuidErr := parseUUID(event)
	if uuidErr != nil {
		t.Fatal(uuidErr)
	}
	digest := strings.Repeat("0", sha256.Size*2)
	uri, parseErr := url.Parse(mcpEventResourceURI(event, GenesisTx, 0, digest))
	if parseErr != nil {
		t.Fatal(parseErr)
	}

	basis := scriptedQuery{
		contains: "SELECT COALESCE(MAX(tx),0)", columns: []string{"basis"},
		rows: [][]driver.Value{{int64(GenesisTx)}},
	}
	coordinateRunner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{
		basis,
		{contains: "SELECT EXISTS(SELECT 1 FROM fgraph_events", err: failure},
	}})
	coordinateDB := &DB{store: &store{sql: coordinateRunner, names: map[string]int64{}}, exec: coordinateRunner}
	if _, _, _, _, _, err := coordinateDB.mcpEventCoordinates(ctx, uri); !errors.Is(err, ErrFormat) {
		t.Fatalf("event coordinate SQL fault = %v, want FormatError", err)
	}

	payloadRunner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{{
		contains: "SELECT ev.tx,ev.event_hash,ev.event_data", err: failure,
	}}})
	payloadDB := &DB{store: &store{sql: payloadRunner, names: map[string]int64{}}, exec: payloadRunner}
	if _, err := payloadDB.mcpEventPayload(ctx, event, eventUUID, GenesisTx, digest); !errors.Is(err, ErrFormat) {
		t.Fatalf("event payload SQL fault = %v, want FormatError", err)
	}
}

func TestMCPChangesResourcePropagatesClosedStore(t *testing.T) {
	db, openErr := Open(":memory:")
	if openErr != nil {
		t.Fatal(openErr)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	client, ctx := connectMCPTestClient(t, db)
	if _, err := client.ReadResource(ctx, &mcp.ReadResourceParams{URI: "fgraph://changes?since=64"}); err == nil {
		t.Fatal("changes resource accepted a closed store")
	}
}

func TestMCPChangesStopsBeforeOversizedEventAndRejectsMalformedContinuationURI(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	small, smallErr := db.Transact(ctx, E{"id": "changes/small-first", "changes/value": "small"})
	if smallErr != nil {
		t.Fatal(smallErr)
	}
	large, largeErr := db.Transact(ctx, E{"id": "changes/large-second", "changes/value": strings.Repeat("x", 300<<10)})
	if largeErr != nil {
		t.Fatal(largeErr)
	}
	client, ctx := connectMCPTestClient(t, db)
	var first mcpChangesResourcePage
	readMCPResourceJSON(t, client, ctx, "fgraph://changes?since=64", &first)
	if len(first.Events) != 1 || first.Events[0]["event"] != small.EventID || first.OversizedEvent != nil || first.NextURI == "" {
		t.Fatalf("page before oversized event = %#v", first)
	}
	var second mcpChangesResourcePage
	readMCPResourceJSON(t, client, ctx, first.NextURI, &second)
	if len(second.Events) != 0 || second.OversizedEvent == nil || second.OversizedEvent.Event != large.EventID || second.NextURI != "" {
		t.Fatalf("oversized continuation page = %#v", second)
	}

	budgetDB := fixedDB(t, ":memory:")
	for index := range 2 {
		if _, err := budgetDB.Transact(ctx, E{
			"id": fmt.Sprintf("malformed-uri/%d", index), "malformed-uri/value": strings.Repeat("x", 110<<10),
		}); err != nil {
			t.Fatal(err)
		}
	}
	basis, basisErr := budgetDB.latestTx(ctx)
	if basisErr != nil {
		t.Fatal(basisErr)
	}
	if _, err := budgetDB.mcpChanges(ctx, basis, GenesisTx, "64", "%"); !errors.Is(err, ErrType) {
		t.Fatalf("malformed continuation URI error = %v, want TypeError", err)
	}
}
