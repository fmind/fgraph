package fgraph

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestCoverageCLIV1ValidationBranches(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	path := filepath.Join(t.TempDir(), "coverage-cli.db")
	run := func(stdin string, args ...string) (string, error) {
		base := make([]string, 0, 3+len(args))
		base = append(base, "--db", path, "--json")
		return runCLIForTest(t, stdin, append(base, args...)...)
	}
	if _, err := run("", "init"); err != nil {
		t.Fatal(err)
	}
	if _, err := run("", "add", `{"id":"cli/covered","cli/value":1}`); err != nil {
		t.Fatal(err)
	}

	for name, args := range map[string][]string{
		"explain arity":            {"explain"},
		"explain json":             {"explain", "{"},
		"explain args json":        {"explain", `{"find":[],"where":[]}`, "--args", "{"},
		"explain args object":      {"explain", `{"find":[],"where":[]}`, "--args", "[]"},
		"datoms arity":             {"datoms", "eavt", "avet"},
		"datoms json":              {"datoms", "--components", "{"},
		"datoms components":        {"datoms", "--components", "{}"},
		"shape arity":              {"shape"},
		"shape flags":              {"shape", "shape/cli", "--closed", "--open"},
		"validate arity":           {"validate"},
		"schema arity":             {"schema", "one", "two"},
		"apply arity":              {"apply", "one", "two"},
		"restore arity":            {"restore", "one", "two"},
		"snapshot arity":           {"snapshot", "extra"},
		"tx arity":                 {"tx"},
		"tx low":                   {"tx", "63"},
		"tx text":                  {"tx", "nope"},
		"doctor arity":             {"doctor", "extra"},
		"declare many conflict":    {"declare", "cli/many", "--many", "--one"},
		"declare unique conflict":  {"declare", "cli/unique", "--unique", "--not-unique"},
		"declare history conflict": {"declare", "cli/history", "--nohistory", "--history"},
		"historical overflow":      {"get", "cli/covered", "--at", "9223372036854775808"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := run("", args...); err == nil {
				t.Fatalf("invalid CLI invocation succeeded: %v", args)
			}
		})
	}

	if _, err := run("", "history", "cli/covered"); err != nil {
		t.Fatalf("history without attribute: %v", err)
	}
	if _, err := run("", "why", "cli/covered"); err != nil {
		t.Fatalf("why without attribute: %v", err)
	}

	t.Setenv("FGRAPH_CLOCK", "not-an-instant")
	if _, err := run("", "info"); !errors.Is(err, ErrType) {
		t.Fatalf("invalid CLI clock error = %v", err)
	}
}

func TestCoverageMCPV1ValidationBranches(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{"id": "mcp/entity", "mcp/value": int64(1), "mcp/text": "needle"}); err != nil {
		t.Fatal(err)
	}
	server := NewMCPServer(db, MCPOptions{Write: true})
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, serverErr := server.Connect(ctx, serverTransport, nil)
	if serverErr != nil {
		t.Fatal(serverErr)
	}
	t.Cleanup(func() {
		if closeErr := serverSession.Close(); closeErr != nil {
			t.Errorf("close MCP server session: %v", closeErr)
		}
	})
	clientSession, clientErr := mcp.NewClient(&mcp.Implementation{Name: "coverage-v1", Version: "1"}, nil).Connect(ctx, clientTransport, nil)
	if clientErr != nil {
		t.Fatal(clientErr)
	}
	t.Cleanup(func() {
		if closeErr := clientSession.Close(); closeErr != nil {
			t.Errorf("close MCP client session: %v", closeErr)
		}
	})

	callError := func(name string, arguments map[string]any) {
		t.Helper()
		result, callErr := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: arguments})
		if callErr != nil {
			t.Fatalf("%s transport error: %v", name, callErr)
		}
		if !result.IsError {
			t.Fatalf("%s unexpectedly succeeded with %#v", name, arguments)
		}
	}
	for name, test := range map[string]struct {
		args map[string]any
		tool string
	}{
		"remember operation": {tool: "remember", args: map[string]any{"facts": map[string]any{"id": "x"}}},
		"remember key":       {tool: "remember", args: map[string]any{"operation_id": "mcp-key", "key": "x"}},
		"remember empty":     {tool: "remember", args: map[string]any{"operation_id": "mcp-empty"}},
		"remember blank":     {tool: "remember", args: map[string]any{"operation_id": "mcp-blank", "text": "  "}},
		"remember facts":     {tool: "remember", args: map[string]any{"operation_id": "mcp-facts-empty", "facts": []any{}}},
		"forget value":       {tool: "forget", args: map[string]any{"entity": "mcp/entity", "value": int64(1)}},
		"forget attribute":   {tool: "forget", args: map[string]any{"entity": "mcp/entity", "attribute": ""}},
		"forget receipt":     {tool: "forget", args: map[string]any{"entity": "mcp/entity", "attribute": "mcp/value"}},
		"undo receipt":       {tool: "undo", args: map[string]any{"tx": GenesisTx}},
		"recall k low":       {tool: "recall", args: map[string]any{"query": "needle", "k": -1}},
		"recall k high":      {tool: "recall", args: map[string]any{"query": "needle", "k": 21}},
		"recall expand low":  {tool: "recall", args: map[string]any{"query": "needle", "expand": -1}},
		"recall expand high": {tool: "recall", args: map[string]any{"query": "needle", "expand": 3}},
		"about depth":        {tool: "about", args: map[string]any{"entity": "mcp/entity", "depth": 3}},
		"why limit low":      {tool: "why", args: map[string]any{"entity": "mcp/entity", "limit": 0}},
		"why limit high":     {tool: "why", args: map[string]any{"entity": "mcp/entity", "limit": 101}},
		"why attribute":      {tool: "why", args: map[string]any{"entity": "mcp/entity", "attribute": ""}},
		"history limit low":  {tool: "history", args: map[string]any{"entity": "mcp/entity", "limit": 0}},
		"history limit high": {tool: "history", args: map[string]any{"entity": "mcp/entity", "limit": 101}},
		"history attribute":  {tool: "history", args: map[string]any{"entity": "mcp/entity", "attribute": ""}},
		"query args":         {tool: "query", args: map[string]any{"q": map[string]any{"find": []any{}, "where": []any{}}, "args": []any{}}},
		"query limit low":    {tool: "query", args: map[string]any{"q": map[string]any{"find": []any{}, "where": []any{}, "limit": -1}}},
		"query limit high":   {tool: "query", args: map[string]any{"q": map[string]any{"find": []any{}, "where": []any{}, "limit": 1001}}},
		"schema limit low":   {tool: "schema", args: map[string]any{"limit": 0}},
		"schema limit high":  {tool: "schema", args: map[string]any{"limit": 101}},
		"datoms limit low":   {tool: "datoms", args: map[string]any{"limit": 0}},
		"datoms limit high":  {tool: "datoms", args: map[string]any{"limit": 101}},
		"explain args":       {tool: "explain", args: map[string]any{"q": map[string]any{"find": []any{}, "where": []any{}}, "args": []any{}}},
	} {
		t.Run(name, func(t *testing.T) { callError(test.tool, test.args) })
	}

	for name, test := range map[string]struct {
		args map[string]any
		tool string
	}{
		"remember list": {tool: "remember", args: map[string]any{"operation_id": "mcp-list", "facts": []any{map[string]any{"id": "mcp/list", "mcp/value": int64(2)}}}},
		"remember note": {tool: "remember", args: map[string]any{"operation_id": "mcp-note", "key": "mcp/note", "text": "remembered", "source": "coverage"}},
		"recall":        {tool: "recall", args: map[string]any{"query": "needle", "k": 1}},
		"why":           {tool: "why", args: map[string]any{"entity": "mcp/entity", "limit": 1}},
		"history":       {tool: "history", args: map[string]any{"entity": "mcp/entity", "limit": 1}},
		"query":         {tool: "query", args: map[string]any{"q": map[string]any{"find": []any{"?e"}, "where": []any{[]any{"?e", "mcp/value", int64(1)}}}}},
		"explain":       {tool: "explain", args: map[string]any{"q": map[string]any{"find": []any{"?e"}, "where": []any{[]any{"?e", "mcp/value", int64(1)}}}}},
	} {
		t.Run("valid "+name, func(t *testing.T) {
			result, callErr := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: test.tool, Arguments: test.args})
			if callErr != nil || result.IsError {
				t.Fatalf("valid %s = %#v, %v", name, result, callErr)
			}
		})
	}

	first, callErr := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: "schema", Arguments: map[string]any{"limit": 1}})
	if callErr != nil || first.IsError {
		t.Fatalf("schema first page = %#v, %v", first, callErr)
	}
	var firstEnvelope struct {
		Data struct {
			NextCursor *string `json:"next_cursor"`
		} `json:"data"`
	}
	encoded, marshalErr := json.Marshal(first.StructuredContent)
	if marshalErr != nil || json.Unmarshal(encoded, &firstEnvelope) != nil || firstEnvelope.Data.NextCursor == nil {
		t.Fatalf("schema cursor envelope = %s, %v", encoded, marshalErr)
	}
	callError("schema", map[string]any{"cursor": *firstEnvelope.Data.NextCursor, "prefix": "other/"})
	callError("schema", map[string]any{"cursor": *firstEnvelope.Data.NextCursor, "include_system": true})
}

func TestCoverageMCPAndEventWireHelpers(t *testing.T) {
	ctx := context.Background()
	if _, err := canonicalMCPBytes(make(chan int)); !errors.Is(err, ErrFormat) {
		t.Fatalf("unsupported MCP value error = %v", err)
	}
	for _, value := range []*int{nil, func() *int { value := 0; return &value }(), func() *int { value := 101; return &value }(), func() *int { value := 12; return &value }()} {
		limit, err := mcpAuditLimit(value)
		if value == nil || *value == 12 {
			if err != nil || limit == 0 {
				t.Fatalf("audit limit %#v = %d, %v", value, limit, err)
			}
		} else if err == nil {
			t.Fatalf("invalid audit limit %#v succeeded", value)
		}
	}
	if page := mcpFactPage(make([]Fact, 3), 2); !page.Truncated || len(page.Items) != 2 {
		t.Fatalf("MCP fact page = %#v", page)
	}
	if _, _, err := boundedMCPOutput(GenesisTx, nil, errors.New("sentinel")); err == nil {
		t.Fatal("bounded MCP output swallowed its error")
	}

	validCursor := mcpSchemaCursor{Version: 1, Basis: GenesisTx, Offset: 1, Digest: "sha256:digest"}
	raw, err := encodeMCPSchemaCursor(validCursor)
	if err != nil {
		t.Fatal(err)
	}
	unknown := base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"basis":64,"offset":1,"digest":"x","unknown":true}`))
	trailing := base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"basis":64,"offset":1,"digest":"x"}{}`))
	for name, candidate := range map[string]string{
		"large": strings.Repeat("x", maxMCPResourceCursor+1), "base64": "!!", "noncanonical": raw + "=", "unknown": unknown, "trailing": trailing,
	} {
		t.Run("schema cursor "+name, func(t *testing.T) {
			if _, err := decodeMCPSchemaCursor(candidate); err == nil {
				t.Fatalf("invalid schema cursor accepted: %q", candidate)
			}
		})
	}
	for name, cursor := range map[string]mcpSchemaCursor{
		"version": {Version: 2, Basis: GenesisTx, Digest: "x"},
		"basis":   {Version: 1, Basis: 0, Digest: "x"},
		"offset":  {Version: 1, Basis: GenesisTx, Offset: -1, Digest: "x"},
		"digest":  {Version: 1, Basis: GenesisTx},
	} {
		t.Run("schema cursor "+name, func(t *testing.T) {
			candidate, encodeErr := encodeMCPSchemaCursor(cursor)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			if _, err := decodeMCPSchemaCursor(candidate); err == nil {
				t.Fatalf("invalid schema cursor accepted: %#v", cursor)
			}
		})
	}

	if _, _, err := genesisEventData(1_767_225_600_000_000); err != nil {
		t.Fatal(err)
	}
	if _, _, err := canonicalEventData(map[string]any{"bad": math.NaN()}); !errors.Is(err, ErrFormat) {
		t.Fatalf("uncanonical event error = %v", err)
	}
	if _, _, err := canonicalEventData(map[string]any{"fgraph": "event/1", "value": strings.Repeat("x", maxPortableLineBytes)}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized event error = %v", err)
	}
	if _, err := parseUUID("00000000-0000-4000-8000-0000000000zz"); !errors.Is(err, ErrType) {
		t.Fatalf("invalid hex UUID error = %v", err)
	}

	db := fixedDB(t, ":memory:")
	if report, err := db.eventReceipt(ctx, db.store.sql, GenesisTx); err != nil || report.BasisTx != GenesisTx {
		t.Fatalf("genesis event receipt = %#v, %v", report, err)
	}
	if _, err := db.mcpResourceView(ctx, GenesisTx-1); !errors.Is(err, ErrConflict) {
		t.Fatalf("low MCP view basis error = %v", err)
	}
	if _, err := db.mcpResourceView(ctx, GenesisTx+1); !errors.Is(err, ErrConflict) {
		t.Fatalf("future MCP view basis error = %v", err)
	}
	historical := db.atTx(GenesisTx)
	if basis, err := historical.mcpVisibleBasis(ctx); err != nil || basis != GenesisTx {
		t.Fatalf("historical MCP basis = %d, %v", basis, err)
	}
	if _, err := mcpJSONResource("fgraph://invalid", make(chan int)); !errors.Is(err, ErrFormat) {
		t.Fatalf("invalid MCP resource error = %v", err)
	}
}

func TestCoverageSearchHelperBranches(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "search/tags", Many(true)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "search/one", "search/text": "needle alpha", "search/group": "a", "search/tags": []any{"x", "y"}},
		E{"id": "search/two", "search/text": "needle beta", "search/group": "b", "search/tags": []any{"x"}},
	}); err != nil {
		t.Fatal(err)
	}
	if result, err := db.Search(ctx, SearchOpts{Text: "needle", Filters: [][]any{{"search/tags", "x"}, {"search/group", "a"}}}); err != nil || len(result.Hits) != 1 {
		t.Fatalf("intersected search = %#v, %v", result, err)
	}
	if result, err := db.Search(ctx, SearchOpts{Text: "needle", Filters: [][]any{{"search/tags", "missing"}, {"search/group", "a"}}}); err != nil || len(result.Hits) != 0 {
		t.Fatalf("empty intersected search = %#v, %v", result, err)
	}
	if _, err := db.Search(ctx, SearchOpts{Text: "needle", TextAttributes: []string{"missing/attribute"}}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing text attribute error = %v", err)
	}

	for name, fact := range map[string]Fact{
		"vector any":     {Tag: TagVector, V: map[string]any{"vector": []any{1.0, 2.0}}},
		"vector float32": {Tag: TagVector, V: map[string]any{"vector": []float32{1, 2, 3}}},
		"vector float64": {Tag: TagVector, V: map[string]any{"vector": []float64{1}}},
		"unicode text":   {Tag: TagText, V: strings.Repeat("é", maxMatchedValueBytes)},
		"short text":     {Tag: TagText, V: "short"},
	} {
		t.Run(name, func(t *testing.T) {
			bounded := boundMatchedFact(fact)
			if strings.HasPrefix(name, "vector") && !bounded.ValueTruncated {
				t.Fatalf("vector was not bounded: %#v", bounded)
			}
		})
	}
	if !math.IsInf(cosine([]float32{0}, []float32{1}), -1) {
		t.Fatal("zero cosine vector did not produce negative infinity")
	}
	if score := cosine([]float32{1, 0}, []float32{1, 0}); score != 1 {
		t.Fatalf("cosine score = %v", score)
	}

	if page, _ := rankRawEntityCandidatesBounded([]rankedRawFact{{entity: 1, score: 1}, {entity: 1, score: 2}, {entity: 2, score: 1}}, 1); len(page) != 1 || page[0].score != 2 {
		t.Fatalf("ranked entity candidates = %#v", page)
	}
	result := SearchResult{Expanded: []SearchHit{{Pull: map[string]any{"large": strings.Repeat("x", maxSearchOutputBytes)}}}, Hits: []SearchHit{{Pull: map[string]any{"large": strings.Repeat("x", maxSearchOutputBytes)}}}}
	boundSearchOutput(&result)
	if !result.Truncated || len(result.Expanded) != 0 || len(result.Hits) != 0 {
		t.Fatalf("bounded search output = %#v", result)
	}
	invalidJSON := SearchResult{Hits: []SearchHit{{Score: math.NaN()}}}
	boundSearchOutput(&invalidJSON)
}

func TestCoverageReceiptCorruptionBranches(t *testing.T) {
	ctx := context.Background()
	for name, corrupt := range map[string]func(*testing.T, *DB, TxReport){
		"event hash": func(t *testing.T, db *DB, report TxReport) {
			_, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_events SET event_hash=x'00' WHERE tx=?", report.Tx)
			if err != nil {
				t.Fatal(err)
			}
		},
		"event gid": func(t *testing.T, db *DB, report TxReport) {
			_, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_ids SET gid=x'00' WHERE id=?", report.Tx)
			if err != nil {
				t.Fatal(err)
			}
		},
		"request hash": func(t *testing.T, db *DB, report TxReport) {
			_, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_events SET operation_id='coverage',request_hash=x'00' WHERE tx=?", report.Tx)
			if err != nil {
				t.Fatal(err)
			}
		},
		"by type": func(t *testing.T, db *DB, report TxReport) {
			_, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_facts SET v=1,t=2 WHERE e=? AND a=2", report.Tx)
			if err != nil {
				t.Fatal(err)
			}
		},
		"source type": func(t *testing.T, db *DB, report TxReport) {
			_, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_facts SET v=1,t=2 WHERE e=? AND a=3", report.Tx)
			if err != nil {
				t.Fatal(err)
			}
		},
		"missing at": func(t *testing.T, db *DB, report TxReport) {
			_, err := db.store.sql.ExecContext(ctx, "DELETE FROM fgraph_facts WHERE e=? AND a=1", report.Tx)
			if err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			report, err := db.Transact(ctx, E{"id": "receipt/entity", "receipt/value": int64(1)}, WithBy("by"), WithSource("source"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.store.sql.ExecContext(ctx, "PRAGMA ignore_check_constraints=ON"); err != nil {
				t.Fatal(err)
			}
			corrupt(t, db, report)
			if _, err := db.Receipt(ctx, report.Tx); !errors.Is(err, ErrFormat) {
				t.Fatalf("corrupt receipt error = %v", err)
			}
		})
	}
}

func TestCoverageStoreAndSchemaHelpers(t *testing.T) {
	ctx := context.Background()
	if _, err := Open(":memory:", WithEventIDFactory(nil)); !errors.Is(err, ErrType) {
		t.Fatalf("nil event factory error = %v", err)
	}
	if dsn, err := sqliteDSN(":memory:", true); err != nil || dsn != ":memory:" {
		t.Fatalf("memory read-only DSN = %q, %v", dsn, err)
	}
	path := filepath.Join(t.TempDir(), "readonly.db")
	db := fixedDB(t, path)
	if dsn, err := sqliteDSN(path, true); err != nil || !strings.HasPrefix(dsn, "file:") {
		t.Fatalf("file read-only DSN = %q, %v", dsn, err)
	}
	if normalizeSchemaSQL(" CREATE  TABLE\nX ( A INT ) ") != "create table x ( a int )" {
		t.Fatal("schema SQL normalization failed")
	}
	if err := (&DB{store: db.store}).Close(); err != nil {
		t.Fatalf("non-owner close = %v", err)
	}

	declaration := DeclaredAttribute{}
	for _, test := range []struct {
		value     any
		attribute int64
	}{
		{attribute: 5, value: true},
		{attribute: 6, value: false},
		{attribute: 7, value: true},
		{attribute: 8, value: "text"},
		{attribute: 10, value: "doc"},
		{attribute: 14, value: "model"},
		{attribute: 9, value: int64(3)},
	} {
		if err := applyDeclaredAttribute(&declaration, test.attribute, test.value, "schema/test"); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		value     any
		attribute int64
	}{{attribute: 5, value: "yes"}, {attribute: 8, value: true}, {attribute: 9, value: "three"}} {
		if err := applyDeclaredAttribute(&DeclaredAttribute{}, test.attribute, test.value, "schema/bad"); !errors.Is(err, ErrFormat) {
			t.Fatalf("invalid declaration %#v error = %v", test, err)
		}
	}
	if values, err := normalizeShapeAttributes("required", []string{"z/name", "a/name", "z/name"}); err != nil || fmt.Sprint(values) != "[a/name z/name]" {
		t.Fatalf("normalized shape attributes = %#v, %v", values, err)
	}
	if _, err := normalizeShapeAttributes("required", []string{"invalid"}); !errors.Is(err, ErrSchema) {
		t.Fatalf("invalid shape attribute error = %v", err)
	}
	if value := pointerValue((*string)(nil)); value != nil {
		t.Fatalf("nil pointer wire value = %#v", value)
	}
	value := "present"
	if pointerValue(&value) != value {
		t.Fatal("present pointer wire value changed")
	}
	if _, err := db.Schema(ctx, "", true); err != nil {
		t.Fatal(err)
	}
}
