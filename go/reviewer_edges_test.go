package fgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestReviewerQueryEdges(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, []any{
		E{"id": "disabled", "feature/enabled": false},
		E{"id": "enabled", "feature/enabled": true},
	}); err != nil {
		t.Fatal(err)
	}

	ordered, err := db.QueryJSON(ctx, map[string]any{
		"find":  []any{"?enabled"},
		"where": []any{[]any{"?e", "feature/enabled", "?enabled"}},
		"order": []any{[]any{"?enabled", "asc"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantRows := [][]any{{false}, {true}}
	if !rowsEqual(ordered.Rows, wantRows) {
		t.Fatalf("boolean ascending rows = %#v, want %#v", ordered.Rows, wantRows)
	}

	limited, err := db.QueryJSON(ctx, map[string]any{
		"find":  []any{"?enabled"},
		"where": []any{[]any{"?e", "feature/enabled", "?enabled"}},
		"limit": 0,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited.Rows) != 0 {
		t.Fatalf("explicit limit zero returned %#v", limited.Rows)
	}

	closed := fixedDB(t, ":memory:")
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	unsafeNot := Q{
		Find: []any{"?e"},
		Where: []any{Object{Fields: []Field{{
			Name: "not",
			Value: []any{
				[]any{"?e", "feature/enabled", true},
			},
		}}}},
	}
	if _, err := closed.Query(ctx, unsafeNot, nil); !errors.Is(err, ErrQuery) {
		t.Fatalf("unsafe not static validation error = %v, want QueryError", err)
	}
}

func TestReviewerPullStarComposition(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "node/link", Ref()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "parent", "node/name": "Parent", "node/value": int64(7), "node/link": RefTo("child")},
		E{"id": "child", "node/name": "Child", "node/value": int64(9)},
	}); err != nil {
		t.Fatal(err)
	}

	result, err := db.Query(ctx, Q{
		Find: []any{[]any{"pull", "?e", []any{
			"*",
			map[string]any{"node/link": []any{"node/name"}},
		}}},
		Where: []any{[]any{"?e", "node/name", "Parent"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		t.Fatalf("pull rows = %#v", result.Rows)
	}
	pulled, ok := result.Rows[0][0].(map[string]any)
	if !ok {
		t.Fatalf("pull value type = %T", result.Rows[0][0])
	}
	if pulled["node/name"] != "Parent" || pulled["node/value"] != int64(7) {
		t.Fatalf("star fields were not retained: %#v", pulled)
	}
	child, ok := pulled["node/link"].(map[string]any)
	if !ok || child["node/name"] != "Child" || len(child) != 1 {
		t.Fatalf("nested pull did not compose with star: %#v", pulled["node/link"])
	}
}

func TestReviewerDeclarationAndJSONBoundaries(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	before, statsErr := db.Stats(ctx)
	if statsErr != nil {
		t.Fatal(statsErr)
	}
	if _, err := db.Declare(ctx, "empty/declaration"); !errors.Is(err, ErrSchema) {
		t.Fatalf("empty declaration error = %v, want SchemaError", err)
	}
	after, statsErr := db.Stats(ctx)
	if statsErr != nil {
		t.Fatal(statsErr)
	}
	if after.Entities != before.Entities || after.Facts != before.Facts {
		t.Fatalf("empty declaration mutated stats: before=%+v after=%+v", before, after)
	}
	if _, err := db.Entity(ctx, "empty/declaration"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty declaration allocated an identity: %v", err)
	}

	for name, input := range map[string][]byte{
		"invalid UTF-8":    {'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'},
		"integer overflow": []byte(`9223372036854775808`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeJSON(bytes.NewReader(input)); !errors.Is(err, ErrType) {
				t.Fatalf("DecodeJSON error = %v, want TypeError", err)
			}
		})
	}
	if _, err := canonicalJSON(map[string]any{"too-large": uint64(math.MaxInt64) + 1}); !errors.Is(err, ErrType) {
		t.Fatalf("canonical JSON integer overflow error = %v, want TypeError", err)
	}
}

type reviewerShortWriter struct{}

func (reviewerShortWriter) Write(data []byte) (int, error) {
	return len(data) / 2, nil
}

func TestReviewerEventStreamAndApplyFailures(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{"id": "exported", "item/value": "value"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Tail(ctx, reviewerShortWriter{}, GenesisTx); !errors.Is(err, ErrFormat) || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short export error = %v, want FormatError wrapping io.ErrShortWrite", err)
	}

	invalid := strings.NewReader("\n" + `{"fgraph":"event/1","event":"11111111-1111-4111-8111-111111111111","at":1,"created":["entity"],"asserted":[["entity","item/value",{"bytes":"AQ=="},"text"]],"retracted":[]}` + "\n")
	_, err := db.Apply(ctx, invalid)
	if !errors.Is(err, ErrType) {
		t.Fatalf("wrapper/tag mismatch error = %v, want TypeError", err)
	}
	if !strings.Contains(err.Error(), "event line 2") {
		t.Fatalf("wrapper/tag mismatch lacks line context: %v", err)
	}

	txFactMismatch := strings.NewReader(`{"fgraph":"event/1","event":"22222222-2222-4222-8222-222222222222","at":2,"created":[],"asserted":[],"retracted":[],"tx_facts":[["audit/value",{"bytes":"AQ=="},"text"]]}` + "\n")
	if _, err := db.Apply(ctx, txFactMismatch); !errors.Is(err, ErrType) || !strings.Contains(err.Error(), "event line 1") {
		t.Fatalf("tx-fact wrapper/tag mismatch = %v", err)
	}
	conflicting := strings.NewReader(`{"fgraph":"event/1","event":"33333333-3333-4333-8333-333333333333","at":3,"created":["conflict"],"asserted":[["conflict","item/value","one","text"],["conflict","item/value","two","text"]],"retracted":[]}` + "\n")
	if _, err := db.Apply(ctx, conflicting); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "event line 1 event 33333333-3333-4333-8333-333333333333 failed") {
		t.Fatalf("apply transaction failure context = %v", err)
	}
}

func TestReviewerMCPNilAndExplicitDepthZero(t *testing.T) {
	ctx := context.Background()
	if err := RunMCP(ctx, nil, MCPOptions{}); !errors.Is(err, ErrType) {
		t.Fatalf("nil MCP database error = %v, want TypeError", err)
	}
	var omitted, explicitZero aboutInput
	if err := json.Unmarshal([]byte(`{"entity":"parent-agent"}`), &omitted); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"entity":"parent-agent","depth":0}`), &explicitZero); err != nil {
		t.Fatal(err)
	}
	if omitted.Depth != nil || explicitZero.Depth == nil || *explicitZero.Depth != 0 {
		t.Fatalf("about depth presence was lost: omitted=%#v explicit=%#v", omitted.Depth, explicitZero.Depth)
	}

	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "agent/peer", Ref()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "parent-agent", "agent/name": "Parent", "agent/peer": RefTo("child-agent")},
		E{"id": "child-agent", "agent/name": "Child"},
	}); err != nil {
		t.Fatal(err)
	}

	server := NewMCPServer(db, MCPOptions{Write: true})
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, serverConnectErr := server.Connect(ctx, serverTransport, nil)
	if serverConnectErr != nil {
		t.Fatal(serverConnectErr)
	}
	defer closeTest(t, serverSession)
	client := mcp.NewClient(&mcp.Implementation{Name: "reviewer", Version: "1"}, nil)
	clientSession, clientConnectErr := client.Connect(ctx, clientTransport, nil)
	if clientConnectErr != nil {
		t.Fatal(clientConnectErr)
	}
	defer closeTest(t, clientSession)

	callAbout := func(arguments map[string]any) map[string]any {
		t.Helper()
		result, callErr := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: "about", Arguments: arguments})
		if callErr != nil {
			t.Fatal(callErr)
		}
		if result.IsError {
			t.Fatalf("about failed: %#v", result.Content)
		}
		var decoded map[string]any
		decodeMCPData(t, result, &decoded)
		return decoded
	}

	defaultDepth := callAbout(map[string]any{"entity": "parent-agent"})
	defaultPeer, ok := defaultDepth["agent/peer"].(map[string]any)
	if !ok || defaultPeer["ref"] != "child-agent" || len(defaultPeer) != 1 {
		t.Fatalf("omitted depth did not default to one: %#v", defaultDepth)
	}
	depthTwo := callAbout(map[string]any{"entity": "parent-agent", "depth": 2})
	expandedPeer, ok := depthTwo["agent/peer"].(map[string]any)
	if !ok || expandedPeer["agent/name"] != "Child" {
		t.Fatalf("depth two did not expand the direct reference: %#v", depthTwo)
	}
	zeroDepth := callAbout(map[string]any{"entity": "parent-agent", "depth": 0})
	zeroPeer, ok := zeroDepth["agent/peer"].(map[string]any)
	if !ok || zeroPeer["ref"] != "child-agent" || len(zeroPeer) != 1 {
		t.Fatalf("explicit depth zero was not preserved: %#v", zeroDepth)
	}

	for name, arguments := range map[string]map[string]any{
		"empty remember":  {},
		"empty recall":    {"query": ""},
		"negative about":  {"entity": "parent-agent", "depth": -1},
		"unknown undo":    {"tx": 999999},
		"unknown history": {"entity": "parent-agent", "attribute": "missing/attribute"},
	} {
		t.Run(name, func(t *testing.T) {
			tool := strings.TrimPrefix(name, "empty ")
			tool = strings.TrimPrefix(tool, "negative ")
			tool = strings.TrimPrefix(tool, "unknown ")
			result, callErr := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: arguments})
			if callErr != nil {
				t.Fatal(callErr)
			}
			if !result.IsError {
				t.Fatalf("%s unexpectedly succeeded: %#v", tool, result.StructuredContent)
			}
		})
	}

	if _, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: "remember", Arguments: map[string]any{"operation_id": "reviewer-remember-forgotten", "facts": map[string]any{"id": "forgotten", "item/value": "x"}},
	}); err != nil {
		t.Fatal(err)
	}
	if result, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: "remember", Arguments: map[string]any{
			"operation_id": "reviewer-remember-empty-source",
			"facts":        map[string]any{"id": "empty-source", "item/value": int64(1)}, "source": "",
		},
	}); err != nil || result.IsError {
		t.Fatalf("remember explicit empty source = %#v, %v", result, err)
	}
	history, err := db.History(ctx, "empty-source", "item/value")
	if err != nil || len(history) != 1 {
		t.Fatalf("empty-source history = %#v, %v", history, err)
	}
	transaction, err := db.Entity(ctx, history[0].Tx, 1)
	if err != nil || transaction["fgraph/source"] != "" {
		t.Fatalf("explicit empty MCP source was lost: transaction=%#v err=%v", transaction, err)
	}
	if _, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: "remember", Arguments: map[string]any{"operation_id": "reviewer-remember-victim", "facts": map[string]any{"id": "victim", "item/a": int64(1), "item/b": int64(2)}},
	}); err != nil {
		t.Fatal(err)
	}
	emptyAttribute, emptyAttributeErr := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: "forget", Arguments: map[string]any{"entity": "victim", "attribute": ""},
	})
	if emptyAttributeErr != nil || !emptyAttribute.IsError {
		t.Fatalf("empty forget attribute = %#v, %v; want tool error", emptyAttribute, emptyAttributeErr)
	}
	victim, victimErr := db.Entity(ctx, "victim", 1)
	if victimErr != nil || victim["item/a"] != int64(1) || victim["item/b"] != int64(2) {
		t.Fatalf("empty forget attribute mutated victim: %#v, %v", victim, victimErr)
	}
	zeroRecall, zeroRecallErr := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: "recall", Arguments: map[string]any{"query": "anything", "k": 0},
	})
	if zeroRecallErr != nil || !zeroRecall.IsError {
		t.Fatalf("zero recall k = %#v, %v; want tool error", zeroRecall, zeroRecallErr)
	}
	for _, arguments := range []map[string]any{
		{"entity": "forgotten", "attribute": "item/value", "value": "x"},
		{"entity": "forgotten", "attribute": "item/value"},
		{"entity": "forgotten"},
	} {
		basis, basisErr := db.latestTx(ctx)
		if basisErr != nil {
			t.Fatal(basisErr)
		}
		arguments["operation_id"] = fmt.Sprintf("reviewer-forget:%d", basis)
		arguments["if_basis_tx"] = basis
		result, callErr := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: "forget", Arguments: arguments})
		if callErr != nil {
			t.Fatal(callErr)
		}
		if result.IsError {
			t.Fatalf("forget failed: %#v", result.Content)
		}
	}

	embedFailure := errors.New("reviewer embed failure")
	failingServer := NewMCPServer(db, MCPOptions{Write: true, Embed: func(context.Context, string) ([]float32, error) {
		return nil, embedFailure
	}})
	failingClientTransport, failingServerTransport := mcp.NewInMemoryTransports()
	failingServerSession, failingServerErr := failingServer.Connect(ctx, failingServerTransport, nil)
	if failingServerErr != nil {
		t.Fatal(failingServerErr)
	}
	defer closeTest(t, failingServerSession)
	failingClient := mcp.NewClient(&mcp.Implementation{Name: "reviewer-failure", Version: "1"}, nil)
	failingClientSession, failingClientErr := failingClient.Connect(ctx, failingClientTransport, nil)
	if failingClientErr != nil {
		t.Fatal(failingClientErr)
	}
	defer closeTest(t, failingClientSession)
	for tool, arguments := range map[string]map[string]any{
		"remember": {"text": "cannot embed", "operation_id": "reviewer-remember-embed-failure"},
		"recall":   {"query": "cannot embed"},
	} {
		result, callErr := failingClientSession.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: arguments})
		if callErr != nil {
			t.Fatal(callErr)
		}
		if !result.IsError {
			t.Fatalf("%s embed failure unexpectedly succeeded", tool)
		}
	}

	wideRemember, wideRememberErr := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: "remember", Arguments: map[string]any{"operation_id": "reviewer-remember-wide", "facts": map[string]any{"id": "wide-agent", "agent/score": int64(math.MaxInt64)}},
	})
	if wideRememberErr != nil || wideRemember.IsError {
		t.Fatalf("MCP wide integer remember failed: result=%#v err=%v", wideRemember, wideRememberErr)
	}
	wideEntity, wideEntityErr := db.Entity(ctx, "wide-agent", 1)
	if wideEntityErr != nil || wideEntity["agent/score"] != int64(math.MaxInt64) {
		t.Fatalf("MCP wide integer changed across the wire: entity=%#v err=%v", wideEntity, wideEntityErr)
	}
	wideQuery, wideQueryErr := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: "query", Arguments: map[string]any{
		"q": map[string]any{
			"find":  []any{"?entity"},
			"where": []any{[]any{"?entity", "agent/score", "?score"}},
			"in":    []any{"?score"},
		},
		"args": map[string]any{"?score": int64(math.MaxInt64)},
	}})
	if wideQueryErr != nil || wideQuery.IsError {
		t.Fatalf("MCP wide integer query failed: result=%#v err=%v", wideQuery, wideQueryErr)
	}
	wideBasis, wideBasisErr := db.latestTx(ctx)
	if wideBasisErr != nil {
		t.Fatal(wideBasisErr)
	}
	wideForget, wideForgetErr := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: "forget", Arguments: map[string]any{
		"entity": "wide-agent", "attribute": "agent/score", "value": int64(math.MaxInt64),
		"operation_id": "reviewer-forget-wide", "if_basis_tx": wideBasis,
	}})
	if wideForgetErr != nil || wideForget.IsError {
		t.Fatalf("MCP wide integer forget failed: result=%#v err=%v", wideForget, wideForgetErr)
	}
	if _, err := db.Transact(ctx, E{"id": "scored-agent", "agent/score": int64(2)}); err != nil {
		t.Fatal(err)
	}
	queryResult, queryErr := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: "query", Arguments: map[string]any{
		"q": map[string]any{
			"find":  []any{"?score"},
			"where": []any{[]any{"?e", "agent/score", "?score"}, []any{">=", "?score", "?min"}},
			"in":    []any{"?min"},
		},
		"args": map[string]any{"?min": 2.0},
	}})
	if queryErr != nil || queryResult.IsError {
		t.Fatalf("MCP query args failed: result=%#v err=%v", queryResult, queryErr)
	}

	schemaDB := fixedDB(t, ":memory:")
	if _, err := schemaDB.Declare(ctx, "memory/embedding", Type("text")); err != nil {
		t.Fatal(err)
	}
	if _, err := schemaDB.Transact(ctx, E{"id": "schema-seed", "memory/embedding": "text"}); err != nil {
		t.Fatal(err)
	}
	schemaServer := NewMCPServer(schemaDB, MCPOptions{Write: true, Embed: func(context.Context, string) ([]float32, error) {
		return []float32{1}, nil
	}})
	schemaClientTransport, schemaServerTransport := mcp.NewInMemoryTransports()
	schemaServerSession, schemaServerErr := schemaServer.Connect(ctx, schemaServerTransport, nil)
	if schemaServerErr != nil {
		t.Fatal(schemaServerErr)
	}
	defer closeTest(t, schemaServerSession)
	schemaClient := mcp.NewClient(&mcp.Implementation{Name: "reviewer-schema", Version: "1"}, nil)
	schemaClientSession, schemaClientErr := schemaClient.Connect(ctx, schemaClientTransport, nil)
	if schemaClientErr != nil {
		t.Fatal(schemaClientErr)
	}
	defer closeTest(t, schemaClientSession)
	schemaResult, schemaCallErr := schemaClientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: "remember", Arguments: map[string]any{"operation_id": "reviewer-remember-schema-mismatch", "text": "schema mismatch"},
	})
	if schemaCallErr != nil || !schemaResult.IsError {
		t.Fatalf("MCP embedding declaration conflict = %#v, %v", schemaResult, schemaCallErr)
	}

	atomicBefore := physicalState(t, db)
	atomicServer := NewMCPServer(db, MCPOptions{Write: true, Embed: func(context.Context, string) ([]float32, error) {
		return []float32{1, 0}, nil
	}})
	atomicClientTransport, atomicServerTransport := mcp.NewInMemoryTransports()
	atomicServerSession, atomicServerErr := atomicServer.Connect(ctx, atomicServerTransport, nil)
	if atomicServerErr != nil {
		t.Fatal(atomicServerErr)
	}
	defer closeTest(t, atomicServerSession)
	atomicClient := mcp.NewClient(&mcp.Implementation{Name: "reviewer-atomic", Version: "1"}, nil)
	atomicClientSession, atomicClientErr := atomicClient.Connect(ctx, atomicClientTransport, nil)
	if atomicClientErr != nil {
		t.Fatal(atomicClientErr)
	}
	defer closeTest(t, atomicClientSession)
	atomicResult, atomicCallErr := atomicClientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: "remember", Arguments: map[string]any{"operation_id": "reviewer-remember-atomic", "key": "fgraph/bad", "text": "must fail atomically"},
	})
	if atomicCallErr != nil || !atomicResult.IsError {
		t.Fatalf("failed embedded remember = %#v, %v; want tool error", atomicResult, atomicCallErr)
	}
	atomicAfter := physicalState(t, db)
	beforeJSON, beforeErr := json.Marshal(atomicBefore)
	if beforeErr != nil {
		t.Fatal(beforeErr)
	}
	afterJSON, afterErr := json.Marshal(atomicAfter)
	if afterErr != nil {
		t.Fatal(afterErr)
	}
	if !bytes.Equal(afterJSON, beforeJSON) {
		t.Fatalf("failed embedded remember mutated physical state: before=%s after=%s", beforeJSON, afterJSON)
	}
}

func TestReviewerExpandedSearchJSONShape(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "doc/link", Ref()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "search-seed", "doc/text": "distinctive needle", "doc/link": RefTo("search-neighbor")},
		E{"id": "search-neighbor", "doc/text": "unrelated neighbor"},
	}); err != nil {
		t.Fatal(err)
	}

	result, err := db.Search(ctx, SearchOpts{Text: "distinctive needle", K: 1, Expand: 1})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var shape struct {
		Hits     []map[string]any `json:"hits"`
		Expanded []map[string]any `json:"expanded"`
	}
	if err := json.Unmarshal(encoded, &shape); err != nil {
		t.Fatal(err)
	}
	if len(shape.Hits) != 1 || len(shape.Expanded) != 1 {
		t.Fatalf("search shape = %s", encoded)
	}
	if _, ok := shape.Hits[0]["matched"]; !ok {
		t.Fatalf("ranked hit omitted matched facts: %s", encoded)
	}
	if _, ok := shape.Hits[0]["score"]; !ok {
		t.Fatalf("ranked hit omitted score: %s", encoded)
	}
	if _, ok := shape.Expanded[0]["matched"]; ok {
		t.Fatalf("expanded hit emitted matched: %s", encoded)
	}
	if _, ok := shape.Expanded[0]["score"]; ok {
		t.Fatalf("expanded hit emitted score: %s", encoded)
	}
}

func TestReviewerConcurrentClose(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{"id": "close-race", "item/value": int64(1)}); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errs := make(chan error, 33)
	var workers sync.WaitGroup
	for range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 16 {
				if _, err := db.Stats(ctx); err != nil && !errors.Is(err, ErrFormat) {
					errs <- err
					return
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		if err := db.Close(); err != nil {
			errs <- err
		}
	}()
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent close operation returned %v", err)
	}
	if _, err := db.Stats(ctx); !errors.Is(err, ErrFormat) {
		t.Fatalf("post-close stats error = %v, want FormatError", err)
	}
}

func TestReviewerTemporalMaintenanceAndInternalEdges(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	first, firstErr := db.Transact(ctx, E{"id": "temporal", "item/value": "first"})
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	second, secondErr := db.Transact(ctx, E{"id": "temporal", "item/value": "second"})
	if secondErr != nil {
		t.Fatal(secondErr)
	}

	for name, at := range map[string]any{
		"transaction": first.Tx,
		"instant":     Instant(first.At),
		"RFC3339":     time.UnixMicro(first.At).UTC().Format(time.RFC3339Nano),
	} {
		t.Run(name, func(t *testing.T) {
			view, viewErr := db.At(ctx, at)
			if viewErr != nil {
				t.Fatal(viewErr)
			}
			entity, entityErr := view.Entity(ctx, "temporal")
			if entityErr != nil || entity["item/value"] != "first" {
				t.Fatalf("At(%v) entity = %#v, %v", at, entity, entityErr)
			}
		})
	}
	for _, at := range []any{true, "not-an-instant", Instant(minInstantMicros - 1)} {
		if _, err := db.At(ctx, at); !errors.Is(err, ErrType) {
			t.Errorf("At(%#v) error = %v, want TypeError", at, err)
		}
	}
	changes, changesErr := db.Changes(ctx, first.Tx, second.Tx)
	if changesErr != nil || len(changes.Asserted) == 0 || len(changes.Retracted) == 0 {
		t.Fatalf("bounded changes = %#v, %v", changes, changesErr)
	}
	if _, err := db.History(ctx, "temporal", "missing/attribute"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("history unknown attribute = %v", err)
	}
	if _, err := db.Why(ctx, "temporal", "missing/attribute"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("why unknown attribute = %v", err)
	}
	if _, err := db.History(ctx, "temporal", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("history empty attribute = %v", err)
	}
	if _, err := db.Why(ctx, "temporal", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("why empty attribute = %v", err)
	}
	if err := db.Speculate(ctx, nil); !errors.Is(err, ErrType) {
		t.Fatalf("nil speculation callback = %v", err)
	}

	for _, destination := range []string{"", ":memory:"} {
		if err := db.Backup(ctx, destination); !errors.Is(err, ErrType) {
			t.Errorf("Backup(%q) error = %v", destination, err)
		}
	}
	if _, err := db.Excise(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("excise missing entity = %v", err)
	}

	if _, err := db.Declare(ctx, "all/ref", Ref()); err != nil {
		t.Fatal(err)
	}
	allTags, allTagsErr := db.Transact(ctx, E{
		"id": "undo-all", "all/ref": RefTo("undo-target"), "all/instant": Instant(first.At),
		"all/bytes": Bytes([]byte{1, 2}), "all/vector": Vector([]float32{1, 2}),
		"all/json": JSON(E{"ok": true}), "all/text": "undo me",
	})
	if allTagsErr != nil {
		t.Fatal(allTagsErr)
	}
	undoAll, undoAllErr := db.Undo(ctx, allTags.Tx)
	if undoAllErr != nil || undoAll.Tx == 0 || len(undoAll.Retracted) < 6 {
		t.Fatalf("undo all logical tags = %+v, %v", undoAll, undoAllErr)
	}
	metadataOnly, metadataErr := db.Transact(ctx, E{"id": "temporal", "item/value": "second"}, WithMeta(E{"reason": "receipt"}))
	if metadataErr != nil || metadataOnly.Tx == 0 {
		t.Fatalf("metadata-only transaction = %+v, %v", metadataOnly, metadataErr)
	}
	noOpUndo, noOpUndoErr := db.Undo(ctx, metadataOnly.Tx)
	if noOpUndoErr != nil || noOpUndo.Tx == 0 || noOpUndo.Status != "applied" {
		t.Fatalf("metadata-only undo = %+v, %v", noOpUndo, noOpUndoErr)
	}

	filePath := filepath.Join(t.TempDir(), "source.db")
	fileDB := fixedDB(t, filePath)
	if err := fileDB.Backup(ctx, filePath); !errors.Is(err, ErrConflict) {
		t.Fatalf("backup over source = %v", err)
	}
	nonEmpty := filepath.Join(t.TempDir(), "non-empty.db")
	if err := os.WriteFile(nonEmpty, []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := fileDB.Backup(ctx, nonEmpty); !errors.Is(err, ErrConflict) {
		t.Fatalf("backup over non-empty file = %v", err)
	}
	if err := fileDB.Backup(ctx, filepath.Join(t.TempDir(), "missing", "backup.db")); !errors.Is(err, ErrFormat) {
		t.Fatalf("backup to missing parent = %v", err)
	}
	readOnlyPath := filepath.Join(t.TempDir(), "read-only.db")
	if err := fileDB.Backup(ctx, readOnlyPath); err != nil {
		t.Fatal(err)
	}
	readOnly, readOnlyErr := Open(readOnlyPath, WithReadOnly())
	if readOnlyErr != nil {
		t.Fatal(readOnlyErr)
	}
	defer closeTest(t, readOnly)
	if report, err := readOnly.Doctor(ctx); err != nil || !report.OK {
		t.Fatalf("read-only doctor = %+v, %v", report, err)
	}
	if _, err := readOnly.Doctor(ctx, true); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only doctor repair = %v", err)
	}
	if _, err := readOnly.Excise(ctx, "anything"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only excise = %v", err)
	}
	if _, err := readOnly.Apply(ctx, strings.NewReader("")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only apply = %v", err)
	}

	corrupt := fixedDB(t, ":memory:")
	if _, err := corrupt.store.sql.ExecContext(ctx, "INSERT INTO fgraph_blobs(hash,data) VALUES (?,?)", bytes.Repeat([]byte{1}, 32), []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	// Simulate on-disk corruption that a valid writer cannot create so doctor
	// exercises its interval invariant instead of only the schema CHECK.
	if _, err := corrupt.store.sql.ExecContext(ctx, "PRAGMA ignore_check_constraints=ON"); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (1,10,x'00',7,64,NULL)",
		"INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (1,10,'invalid interval',4,64,64)",
		"INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (1,999,'dangling attribute',4,64,NULL)",
	} {
		if _, err := corrupt.store.sql.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	report, doctorErr := corrupt.Doctor(ctx)
	if doctorErr != nil || report.OK || report.OrphanedBlobs != 1 || len(report.Problems) != 9 ||
		report.Problems[0] != "next_id: expected 1000, found 65" || report.Problems[1] != "invalid genesis facts: 3" {
		t.Fatalf("doctor corrupt report = %+v, %v", report, doctorErr)
	}

	closed := fixedDB(t, ":memory:")
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := closed.Doctor(ctx); !errors.Is(err, ErrFormat) {
		t.Fatalf("closed doctor = %v", err)
	}
	if _, err := closed.Excise(ctx, "anything"); !errors.Is(err, ErrFormat) {
		t.Fatalf("closed excise = %v", err)
	}
	if err := closed.Tail(ctx, io.Discard, GenesisTx); !errors.Is(err, ErrFormat) {
		t.Fatalf("closed export = %v", err)
	}
	if _, err := closed.Apply(ctx, strings.NewReader("")); !errors.Is(err, ErrFormat) {
		t.Fatalf("closed apply = %v", err)
	}
	followed := closed.Follow(ctx, FollowOptions{})
	event, ok := <-followed
	if !ok || !errors.Is(event.Err, ErrFormat) {
		t.Fatalf("closed follow event = %+v, open=%t", event, ok)
	}

	one := errors.New("one")
	two := errors.New("two")
	if joinErrors(nil, nil) != nil || !errors.Is(joinErrors(nil, one), one) || !errors.Is(joinErrors(one, nil), one) {
		t.Fatal("joinErrors nil identity failed")
	}
	joined := joinErrors(one, two)
	if !errors.Is(joined, one) || !errors.Is(joined, two) {
		t.Fatalf("joined errors = %v", joined)
	}
	if wrapClose(nil, "resource") != nil || !errors.Is(wrapClose(one, "resource"), ErrFormat) {
		t.Fatal("wrapClose taxonomy failed")
	}

	for _, value := range []any{int(1), int64(1), float32(1.5), float64(1.5)} {
		if _, ok := numericRat(value); !ok {
			t.Errorf("numericRat rejected %T", value)
		}
	}
	for _, value := range []any{math.NaN(), "1"} {
		if _, ok := numericRat(value); ok {
			t.Errorf("numericRat accepted %#v", value)
		}
	}
	if _, err := vectorDimensions(storedValue{logical: "not-a-vector"}, "item/vector"); !errors.Is(err, ErrType) {
		t.Fatalf("malformed vector dimensions = %v", err)
	}
	plan := &transactionPlan{}
	if err := db.ensureVectorDims(plan, 99, "item/vector", 2); err != nil {
		t.Fatal(err)
	}
	if err := db.ensureVectorDims(plan, 99, "item/vector", 2); err != nil {
		t.Fatal(err)
	}
	if err := db.ensureVectorDims(plan, 99, "item/vector", 3); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting inferred dimensions = %v", err)
	}
}

func TestReviewerExportCorruptIdentityBoundary(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{"id": "corrupt-export", "item/value": "value"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Entity(ctx, "corrupt-export"); err != nil {
		t.Fatal(err)
	}
	attributeID := db.store.names["item/value"]
	if _, err := db.store.sql.ExecContext(ctx, "DELETE FROM fgraph_ids WHERE id=?", attributeID); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := db.Tail(ctx, &output, GenesisTx); !errors.Is(err, ErrFormat) {
		t.Fatalf("export with missing attribute identity = %v", err)
	}
}

type reviewerCancelWriter struct {
	cancel context.CancelFunc
	data   bytes.Buffer
	once   sync.Once
	mu     sync.Mutex
}

func (writer *reviewerCancelWriter) Write(data []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	written, err := writer.data.Write(data)
	writer.once.Do(writer.cancel)
	return written, err
}

func (writer *reviewerCancelWriter) String() string {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.data.String()
}

func TestReviewerTailFollowCancellation(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	path := filepath.Join(t.TempDir(), "follow.db")
	db := fixedDB(t, path)
	if _, err := db.Transact(context.Background(), E{"id": "followed", "item/value": "value"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer := &reviewerCancelWriter{cancel: cancel}
	err := RunCLI(ctx, []string{
		"fgraph", "--db", path, "tail", "--since", "64", "--follow",
	}, strings.NewReader(""), writer, io.Discard)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("tail follow cancellation = %v", err)
	}
	if !strings.Contains(writer.String(), `"event"`) {
		t.Fatalf("tail follow emitted no transaction: %q", writer.String())
	}
}

func TestReviewerFollowDefaultStartsAfterGenesis(t *testing.T) {
	db := fixedDB(t, ":memory:")
	report, err := db.Transact(context.Background(), E{"id": "first-user-event", "item/value": "value"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	events := db.Follow(ctx, FollowOptions{})
	first, ok := <-events
	cancel()
	for range events {
	}
	if !ok {
		t.Fatal("default follower closed before the first user transaction")
	}
	if first.Err != nil {
		t.Fatalf("default follower error = %v", first.Err)
	}
	if first.Tx != report.Tx {
		t.Fatalf("default follower first tx = %d, want first user tx %d (genesis must stay internal)", first.Tx, report.Tx)
	}
}

func TestReviewerSubsetMismatchContract(t *testing.T) {
	exact := Object{Fields: []Field{{Name: "value", Value: int64(1)}}}
	withEllipsis := Object{Fields: []Field{
		{Name: "value", Value: int64(1)},
		{Name: "...", Value: true},
	}}
	actualExact := map[string]any{"value": int64(1)}
	actualExtra := map[string]any{"value": int64(1), "extra": true}
	if mismatch := subsetMismatch(exact, actualExact, "$", false); mismatch != "" {
		t.Fatalf("exact object rejected exact value: %s", mismatch)
	}
	if mismatch := subsetMismatch(exact, actualExtra, "$", false); mismatch == "" {
		t.Fatal("exact object accepted an extra key")
	}
	if mismatch := subsetMismatch(withEllipsis, actualExtra, "$", false); mismatch != "" {
		t.Fatalf("ellipsis object rejected an extra key: %s", mismatch)
	}

	ordered := []any{[]any{int64(1)}, []any{int64(2)}}
	reordered := []any{[]any{int64(2)}, []any{int64(1)}}
	if mismatch := subsetMismatch(ordered, reordered, "$.rows", false); mismatch == "" {
		t.Fatal("ordered rows accepted a reorder")
	}
	if mismatch := subsetMismatch(ordered, reordered, "$.rows", true); mismatch != "" {
		t.Fatalf("unordered rows rejected a reorder: %s", mismatch)
	}
	withExtra := append(append([]any(nil), reordered...), []any{int64(3)})
	for _, unordered := range []bool{false, true} {
		if mismatch := subsetMismatch(ordered, withExtra, "$.rows", unordered); mismatch == "" {
			t.Fatalf("unordered=%t accepted an extra array item", unordered)
		}
	}
}

func TestReviewerTransactionBoundaryMatrix(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, declareErr := db.Declare(ctx, "item/int", Type("int")); declareErr != nil {
		t.Fatal(declareErr)
	}
	if _, declareErr := db.Declare(ctx, "item/ref", Ref()); declareErr != nil {
		t.Fatal(declareErr)
	}
	if _, seedErr := db.Transact(ctx, E{"id": "seed", "item/int": int64(1)}); seedErr != nil {
		t.Fatal(seedErr)
	}

	cases := []struct {
		data any
		kind error
		name string
	}{
		{data: []any{"assert", "entity", "item/value"}, kind: ErrType, name: "short assert"},
		{data: []any{"assert", 1.5, "item/value", true}, kind: ErrType, name: "fractional entity"},
		{data: []any{"assert", "entity", true, "value"}, kind: ErrType, name: "non-text attribute"},
		{data: []any{"assert", "entity", "invalid", "value"}, kind: ErrSchema, name: "invalid attribute name"},
		{data: []any{"assert", int64(999999), "item/value", true}, kind: ErrNotFound, name: "unknown numeric entity"},
		{data: []any{"assert", "entity", "item/ref", RefTo(true)}, kind: ErrType, name: "invalid reference target"},
		{data: []any{"assert", "entity", "item/ref", RefTo(int64(999999))}, kind: ErrNotFound, name: "unknown numeric reference"},
		{data: []any{"retract"}, kind: ErrType, name: "short retract"},
		{data: []any{"retract", 1.5}, kind: ErrType, name: "fractional retract entity"},
		{data: []any{"retract", "seed", true}, kind: ErrType, name: "non-text retract attribute"},
		{data: []any{"retract", "seed", "invalid"}, kind: ErrSchema, name: "invalid retract attribute"},
		{data: []any{"retract", "seed", "item/int", "wrong"}, kind: ErrType, name: "wrong exact retract type"},
		{data: []any{"assert", Object{Fields: []Field{{Name: "tmp", Value: true}}}, "item/value", true}, kind: ErrType, name: "invalid tempid"},
		{data: []any{"assert", true, "item/value", true}, kind: ErrType, name: "invalid selector type"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, transactErr := db.Transact(ctx, test.data); !errors.Is(transactErr, test.kind) {
				t.Fatalf("Transact(%#v) error = %v, want %v", test.data, transactErr, test.kind)
			}
		})
	}
	for name, data := range map[string]any{
		"missing attribute": []any{"retract", "seed", "missing/attribute"},
		"missing entity":    []any{"retract", "missing", "item/int"},
		"numeric int":       []any{"retract", int(db.store.names["seed"]), "missing/attribute"},
	} {
		t.Run(name, func(t *testing.T) {
			report, transactErr := db.Transact(ctx, data)
			if transactErr != nil || report.Tx != 0 {
				t.Fatalf("missing retract = %+v, %v", report, transactErr)
			}
		})
	}

	if _, declareErr := db.Declare(ctx, "unique/email", Type("text"), Unique()); declareErr != nil {
		t.Fatal(declareErr)
	}
	if _, declareErr := db.Declare(ctx, "unique/code", Type("text"), Unique()); declareErr != nil {
		t.Fatal(declareErr)
	}
	if _, seedErr := db.Transact(ctx, []any{
		E{"id": "email-owner", "unique/email": "owner@example.test"},
		E{"id": "code-owner", "unique/code": "owner-code"},
	}); seedErr != nil {
		t.Fatal(seedErr)
	}
	if _, transactErr := db.Transact(ctx, E{
		"unique/email": "owner@example.test",
		"unique/code":  "owner-code",
	}); !errors.Is(transactErr, ErrConflict) {
		t.Fatalf("split unique-owner map error = %v", transactErr)
	}
}

func TestReviewerQueryRuntimeShapeErrors(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, seedErr := db.Transact(ctx, E{"id": "query-shape", "item/value": true}); seedErr != nil {
		t.Fatal(seedErr)
	}
	clauses := map[string]any{
		"non-object":          true,
		"invalid not body":    Object{Fields: []Field{{Name: "not", Value: true}}},
		"invalid or body":     Object{Fields: []Field{{Name: "or", Value: true}}},
		"invalid rule body":   Object{Fields: []Field{{Name: "rule", Value: true}}},
		"unknown clause kind": Object{Fields: []Field{{Name: "unknown", Value: []any{}}}},
	}
	for name, clause := range clauses {
		t.Run(name, func(t *testing.T) {
			_, queryErr := db.Query(ctx, Q{
				Find:  []any{"?e"},
				Where: []any{[]any{"?e", "item/value", true}, clause},
			}, nil)
			if !errors.Is(queryErr, ErrQuery) {
				t.Fatalf("runtime clause %#v error = %v", clause, queryErr)
			}
		})
	}
}

func TestReviewerCLIOutputAndTailWriterFailures(t *testing.T) {
	human := NewCLI(strings.NewReader(""), io.Discard, io.Discard)
	if outputErr := outputResult(human, make(chan int), nil); !errors.Is(outputErr, ErrFormat) {
		t.Fatalf("human output encoding error = %v", outputErr)
	}
	machine := NewCLI(strings.NewReader(""), io.Discard, io.Discard)
	if setErr := machine.Set("json", "true"); setErr != nil {
		t.Fatal(setErr)
	}
	if outputErr := outputResult(machine, math.NaN(), nil); !errors.Is(outputErr, ErrFormat) {
		t.Fatalf("machine output encoding error = %v", outputErr)
	}
	sentinel := fail(ErrQuery, "reviewer sentinel")
	if outputErr := outputResult(machine, nil, sentinel); !errors.Is(outputErr, sentinel) {
		t.Fatalf("output error propagation = %v", outputErr)
	}

	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	path := filepath.Join(t.TempDir(), "tail-writer.db")
	db := fixedDB(t, path)
	if _, seedErr := db.Transact(context.Background(), E{"id": "tail-writer", "item/value": true}); seedErr != nil {
		t.Fatal(seedErr)
	}
	closeTest(t, db)
	for name, args := range map[string][]string{
		"snapshot": {"fgraph", "--db", path, "tail", "--since", "64"},
		"follow":   {"fgraph", "--db", path, "tail", "--since", "64", "--follow"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cancel := func() {}
			if name == "follow" {
				ctx, cancel = context.WithCancel(ctx)
			}
			cliErr := RunCLI(ctx, args, strings.NewReader(""), errorWriter{}, io.Discard)
			cancel()
			if cliErr == nil {
				t.Fatal("tail with failing writer unexpectedly succeeded")
			}
		})
	}
	emptyPath := filepath.Join(t.TempDir(), "empty-tail.db")
	emptyDB := fixedDB(t, emptyPath)
	closeTest(t, emptyDB)
	if cliErr := RunCLI(context.Background(), []string{"fgraph", "--db", emptyPath, "tail"}, strings.NewReader(""), io.Discard, io.Discard); cliErr != nil {
		t.Fatalf("empty tail = %v", cliErr)
	}
}

func TestReviewerCanceledMCPCLI(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(t.TempDir(), "mcp.db")
	err := RunCLI(ctx, []string{"fgraph", "--db", path, "mcp", "--embed-cmd", "unused"}, strings.NewReader(""), io.Discard, io.Discard)
	if err == nil {
		t.Fatal("canceled MCP command unexpectedly succeeded")
	}
}

func rowsEqual(left, right [][]any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}
