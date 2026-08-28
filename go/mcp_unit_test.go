package fgraph

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestExactMCPArguments(t *testing.T) {
	for _, request := range []*mcp.CallToolRequest{
		nil,
		{},
		{Params: &mcp.CallToolParamsRaw{Arguments: json.RawMessage(" \n ")}},
	} {
		arguments, err := exactMCPArguments(request)
		if err != nil || len(arguments) != 0 {
			t.Fatalf("empty arguments = %#v, %v", arguments, err)
		}
	}
	request := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
		Arguments: json.RawMessage(`{"entity":9223372036854775807}`),
	}}
	arguments, err := exactMCPArguments(request)
	if err != nil || arguments["entity"] != int64(math.MaxInt64) {
		t.Fatalf("lossless arguments = %#v, %v", arguments, err)
	}
	for _, raw := range []string{"[1]", "{"} {
		request.Params.Arguments = json.RawMessage(raw)
		if _, err := exactMCPArguments(request); !errors.Is(err, ErrType) {
			t.Fatalf("arguments %q error = %v, want TypeError", raw, err)
		}
	}
}

func TestMCPDatomsPreservesInt64Components(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	for entity, value := range map[string]int64{
		"mcp/min-int": math.MinInt64,
		"mcp/max-int": math.MaxInt64,
	} {
		if _, err := db.Transact(ctx, E{"id": entity, "mcp/integer": value}); err != nil {
			t.Fatal(err)
		}
	}

	server := NewMCPServer(db, MCPOptions{ReadOnly: true})
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, serverSession)
	client := mcp.NewClient(&mcp.Implementation{Name: "integer-boundary", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, clientSession)

	for entity, value := range map[string]int64{
		"mcp/min-int": math.MinInt64,
		"mcp/max-int": math.MaxInt64,
	} {
		result, callErr := clientSession.CallTool(ctx, &mcp.CallToolParams{
			Name: "datoms", Arguments: map[string]any{
				"index": "avet", "components": []any{"mcp/integer", value},
			},
		})
		if callErr != nil || result.IsError {
			t.Fatalf("datoms(%d) = %#v, %v", value, result, callErr)
		}
		var page struct {
			Items []struct {
				Entity string `json:"e"`
			} `json:"items"`
		}
		decodeMCPData(t, result, &page)
		if len(page.Items) != 1 || page.Items[0].Entity != entity {
			t.Fatalf("datoms(%d) items = %#v, want %s", value, page.Items, entity)
		}
	}
}

func TestMCPStructuredOutputCap(t *testing.T) {
	within := strings.Repeat("x", MaxMCPOutputBytes-100)
	if _, value, err := boundedMCPOutput(GenesisTx, within, nil); err != nil {
		t.Fatalf("boundary output = %T, %v", value, err)
	} else if envelope, ok := value.(mcpEnvelope); !ok || !envelope.OK || envelope.BasisTx != GenesisTx || envelope.Data != within {
		t.Fatalf("boundary envelope = %#v", value)
	}
	over := strings.Repeat("x", MaxMCPOutputBytes)
	if _, _, err := boundedMCPOutput(GenesisTx, over, nil); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized output error = %v, want TooLarge", err)
	}
	sentinel := errors.New("tool failed")
	if _, _, err := boundedMCPOutput(GenesisTx, over, sentinel); !errors.Is(err, sentinel) {
		t.Fatalf("tool error = %v, want original", err)
	}
}

func decodeMCPData(t *testing.T, result *mcp.CallToolResult, destination any) int64 {
	t.Helper()
	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Data    json.RawMessage `json:"data"`
		BasisTx int64           `json:"basis_tx"`
		OK      bool            `json:"ok"`
	}
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.OK || envelope.BasisTx < GenesisTx || len(envelope.Data) == 0 {
		t.Fatalf("invalid MCP envelope: %s", encoded)
	}
	if err := json.Unmarshal(envelope.Data, destination); err != nil {
		t.Fatal(err)
	}
	return envelope.BasisTx
}
