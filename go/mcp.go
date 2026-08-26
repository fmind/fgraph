package fgraph

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type MCPOptions struct {
	Embed    Embedder
	ReadOnly bool
	Write    bool
}

type rememberInput struct {
	Facts       any     `json:"facts,omitempty" jsonschema:"Structured fact map or transaction operations. Example: {\"id\":\"ada\",\"person/city\":\"Lyon\"}."`
	Key         *string `json:"key,omitempty" jsonschema:"Stable entity name for a text note. Example: project/build-language."`
	Source      *string `json:"source,omitempty" jsonschema:"Provenance source. Example: architecture.md."`
	OperationID *string `json:"operation_id" jsonschema:"Required idempotency key for a retryable write."`
	IfBasisTx   *int64  `json:"if_basis_tx,omitempty" jsonschema:"Optional expected local basis transaction."`
	Text        string  `json:"text,omitempty" jsonschema:"Unstructured note text. Example: The build uses Go 1.27."`
}

type recallInput struct {
	K      *int   `json:"k,omitempty" jsonschema:"Maximum hits, default 10. Example: 5."`
	Query  string `json:"query" jsonschema:"Text to recall. Example: which language does the build use?"`
	Expand int    `json:"expand,omitempty" jsonschema:"Reference hops to expand. Example: 1."`
}

type aboutInput struct {
	Entity any  `json:"entity" jsonschema:"Entity name or id. Example: ada."`
	Depth  *int `json:"depth,omitempty" jsonschema:"Reference expansion depth, default 1. Example: 2."`
}

type auditInput struct {
	Entity    any     `json:"entity" jsonschema:"Entity name or id. Example: ada."`
	Attribute *string `json:"attribute,omitempty" jsonschema:"Optional attribute. Example: person/city."`
	Limit     *int    `json:"limit,omitempty" jsonschema:"Maximum items, default and maximum 100."`
}

type forgetInput struct {
	Entity      any     `json:"entity" jsonschema:"Entity name or id. Example: ada."`
	Value       any     `json:"value,omitempty" jsonschema:"Optional exact value. Example: Lyon."`
	Attribute   *string `json:"attribute,omitempty" jsonschema:"Optional attribute. Example: person/city."`
	OperationID *string `json:"operation_id" jsonschema:"Required idempotency key for a destructive write."`
	IfBasisTx   *int64  `json:"if_basis_tx" jsonschema:"Required expected local basis transaction."`
}

type undoInput struct {
	OperationID *string `json:"operation_id" jsonschema:"Required idempotency key for a destructive write."`
	IfBasisTx   *int64  `json:"if_basis_tx" jsonschema:"Required expected local basis transaction."`
	Tx          int64   `json:"tx" jsonschema:"Transaction id to compensate. Example: 70."`
}

type queryInput struct {
	Q    any            `json:"q" jsonschema:"Canonical query object. Example: {\"find\":[\"?n\"],\"where\":[[\"ada\",\"person/name\",\"?n\"]]}."`
	Args map[string]any `json:"args,omitempty" jsonschema:"Bindings for query in variables. Example: {\"?min\":30}."`
}

type schemaInput struct {
	Prefix        *string `json:"prefix,omitempty" jsonschema:"Optional attribute prefix. Example: person/."`
	IncludeSystem *bool   `json:"include_system,omitempty" jsonschema:"Include fgraph system attributes. Example: true."`
	Limit         *int    `json:"limit,omitempty" jsonschema:"Page size, 1 through 100."`
	Cursor        string  `json:"cursor,omitempty" jsonschema:"Opaque cursor returned by a previous schema call."`
}

type datomsInput struct {
	Limit      *int   `json:"limit,omitempty" jsonschema:"Page size, 1 through 100."`
	Index      string `json:"index,omitempty" jsonschema:"Index order: eavt, avet, or vaet."`
	Source     string `json:"source,omitempty" jsonschema:"Datom source: current or history."`
	Cursor     string `json:"cursor,omitempty" jsonschema:"Opaque cursor returned by a previous call."`
	Components []any  `json:"components,omitempty" jsonschema:"Leading index components."`
}

type receiptInput struct {
	Tx int64 `json:"tx" jsonschema:"Local transaction id. Example: 70."`
}

type mcpEnvelope struct {
	Data    any   `json:"data"`
	BasisTx int64 `json:"basis_tx"`
	OK      bool  `json:"ok"`
}

type mcpItemsResult struct {
	Items     []Fact `json:"items"`
	Truncated bool   `json:"truncated"`
}

type mcpSchemaResult struct {
	NextCursor *string `json:"next_cursor"`
	SchemaSnapshot
	Truncated bool `json:"truncated"`
}

func (result mcpSchemaResult) MarshalJSON() ([]byte, error) {
	return marshalOrderedObject([]Field{
		{Name: "digest", Value: result.Digest},
		{Name: "attributes", Value: result.Attributes},
		{Name: "shapes", Value: result.Shapes},
		{Name: "basis_tx", Value: result.BasisTx},
		{Name: "next_cursor", Value: result.NextCursor},
		{Name: "truncated", Value: result.Truncated},
	})
}

type mcpSchemaCursor struct {
	Prefix        *string `json:"prefix"`
	Digest        string  `json:"digest"`
	Basis         int64   `json:"basis"`
	Offset        int     `json:"offset"`
	Version       int     `json:"v"`
	IncludeSystem bool    `json:"include_system"`
}

func paginateMCPSchema(snapshot SchemaSnapshot, offset, limit int) ([]SchemaAttribute, []ShapeInfo, int) {
	attributeOffset := min(offset, len(snapshot.Attributes))
	attributeEnd := min(len(snapshot.Attributes), attributeOffset+limit)
	attributes := snapshot.Attributes[attributeOffset:attributeEnd]
	remaining := limit - len(attributes)
	shapeOffset := max(0, offset-len(snapshot.Attributes))
	shapeEnd := min(len(snapshot.Shapes), shapeOffset+remaining)
	shapes := snapshot.Shapes[shapeOffset:shapeEnd]
	return attributes, shapes, offset + len(attributes) + len(shapes)
}

func mcpBool(value bool) *bool { return &value }

func mcpClientProvenance(request *mcp.CallToolRequest) string {
	client := "unknown"
	if request != nil {
		if info := request.ClientInfo(); info != nil && info.Name != "" {
			client = info.Name
		}
	}
	return "mcp:" + client
}

func mcpReportBasis(report TxReport) int64 {
	if report.Tx != 0 {
		return report.Tx
	}
	return report.BasisTx
}

func mcpPinnedView(ctx context.Context, db *DB) (*DB, int64, error) {
	latest, err := db.latestTx(ctx)
	var view *DB
	var basis int64
	if err == nil {
		view = db.atTx(latest)
		basis = latest
	}
	return view, basis, err
}

func NewMCPServer(db *DB, options MCPOptions) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "fgraph", Version: Version}, &mcp.ServerOptions{
		Instructions: "Use fgraph as an auditable temporal fact store. Discover schema first, prefer bounded query/datoms pages, preserve returned basis_tx for follow-up reads, and supply stable operation_id plus if_basis_tx for retry-safe writes. The server is read-only unless explicitly started with write access.",
	})
	readAnnotations := &mcp.ToolAnnotations{
		ReadOnlyHint: true, IdempotentHint: true,
		DestructiveHint: mcpBool(false), OpenWorldHint: mcpBool(false),
	}
	writeAnnotations := &mcp.ToolAnnotations{
		IdempotentHint: true, DestructiveHint: mcpBool(false), OpenWorldHint: mcpBool(false),
	}
	destructiveAnnotations := &mcp.ToolAnnotations{
		IdempotentHint: true, DestructiveHint: mcpBool(true), OpenWorldHint: mcpBool(false),
	}
	outputSchema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"ok":       map[string]any{"const": true},
			"basis_tx": map[string]any{"type": "integer"},
			"data":     map[string]any{},
		},
		"required": []string{"ok", "basis_tx", "data"}, "additionalProperties": false,
	}
	registerMCPResources(server, db)
	if options.Write && !options.ReadOnly {
		mcp.AddTool(server, &mcp.Tool{
			Name: "remember", Description: "Store structured facts or a text note with audit provenance. Example: {\"text\":\"Use Go 1.27\",\"source\":\"AGENTS.md\"}.",
			Annotations: writeAnnotations, OutputSchema: outputSchema,
		}, func(ctx context.Context, request *mcp.CallToolRequest, input rememberInput) (*mcp.CallToolResult, any, error) {
			arguments, err := exactMCPArguments(request)
			if err != nil {
				return nil, nil, err
			}
			input.Facts = arguments["facts"]
			if input.OperationID == nil {
				return nil, nil, fail(ErrType, "remember requires operation_id; retry writes with one stable idempotency key")
			}
			hasText := strings.TrimSpace(input.Text) != ""
			if input.Key != nil && !hasText {
				return nil, nil, fail(ErrType, "remember key requires text; provide a non-blank text note")
			}
			if input.Facts == nil && !hasText {
				return nil, nil, fail(ErrType, "remember needs facts or text; provide at least one")
			}
			if input.Text != "" && !hasText {
				return nil, nil, fail(ErrType, "remember text is blank; provide meaningful text or structured facts")
			}
			items := []any{}
			if input.Facts != nil {
				if transaction, ok := input.Facts.([]any); ok && !isOperation(transaction) {
					items = append(items, transaction...)
				} else {
					items = append(items, input.Facts)
				}
			}
			if input.Text != "" {
				note := E{"memory/text": input.Text}
				if input.Key != nil {
					note["id"] = *input.Key
				}
				if options.Embed != nil {
					vector, embedErr := options.Embed(ctx, input.Text)
					if embedErr != nil {
						return nil, nil, wrap(ErrType, embedErr, "embedding command failed for remembered text; correct --embed-cmd")
					}
					note["memory/embedding"] = Vector(vector)
				}
				items = append(items, note)
			}
			if len(items) == 0 {
				return nil, nil, fail(ErrType, "remember facts is empty; provide at least one fact or non-empty text")
			}
			var data any = items
			if len(items) == 1 {
				data = items[0]
			}
			options := []TxOption{WithBy(mcpClientProvenance(request))}
			if input.Source != nil {
				options = append(options, WithSource(*input.Source))
			}
			options = append(options, WithOperationID(*input.OperationID))
			if input.IfBasisTx != nil {
				options = append(options, IfBasis(*input.IfBasisTx))
			}
			report, err := db.Transact(ctx, data, options...)
			return toolMCPOutput(ctx, db, report, err, mcpReportBasis(report))
		})
		mcp.AddTool(server, &mcp.Tool{
			Name: "forget", Description: "Retract a fact, attribute, or entity while preserving history. Example: {\"entity\":\"ada\",\"attribute\":\"person/city\",\"value\":\"Lyon\"}.",
			Annotations: destructiveAnnotations, OutputSchema: outputSchema,
		}, func(ctx context.Context, request *mcp.CallToolRequest, input forgetInput) (*mcp.CallToolResult, any, error) {
			arguments, err := exactMCPArguments(request)
			if err != nil {
				return nil, nil, err
			}
			entity := arguments["entity"]
			value, hasValue := arguments["value"]
			if input.Attribute == nil && hasValue {
				return nil, nil, fail(ErrType, "forget value requires an attribute; provide attribute for exact retraction or omit value for whole-entity retraction")
			}
			if input.Attribute != nil && *input.Attribute == "" {
				return nil, nil, fail(ErrSchema, "forget attribute is empty; provide namespace/name or omit it for whole-entity retraction")
			}
			if input.OperationID == nil || input.IfBasisTx == nil {
				return nil, nil, fail(ErrType, "forget requires operation_id and if_basis_tx; reread the basis and retry with a stable key")
			}
			operation := []any{"retract", entity}
			if input.Attribute != nil {
				operation = append(operation, *input.Attribute)
				if hasValue {
					operation = append(operation, value)
				}
			}
			report, err := db.Transact(ctx, operation, WithBy(mcpClientProvenance(request)), WithOperationID(*input.OperationID), IfBasis(*input.IfBasisTx))
			return toolMCPOutput(ctx, db, report, err, mcpReportBasis(report))
		})
		mcp.AddTool(server, &mcp.Tool{
			Name: "undo", Description: "Create an audited compensating transaction. Example: {\"tx\":70} restores what transaction 70 changed.",
			Annotations: destructiveAnnotations, OutputSchema: outputSchema,
		}, func(ctx context.Context, request *mcp.CallToolRequest, input undoInput) (*mcp.CallToolResult, any, error) {
			if input.OperationID == nil || input.IfBasisTx == nil {
				return nil, nil, fail(ErrType, "undo requires operation_id and if_basis_tx; reread the basis and retry with a stable key")
			}
			report, err := db.Undo(ctx, input.Tx, WithBy(mcpClientProvenance(request)), WithOperationID(*input.OperationID), IfBasis(*input.IfBasisTx))
			return toolMCPOutput(ctx, db, report, err, mcpReportBasis(report))
		})
	}
	mcp.AddTool(server, &mcp.Tool{
		Name: "recall", Description: "Recall notes with keyword and optional semantic search. Example: {\"query\":\"build language\",\"k\":5,\"expand\":1}.",
		Annotations: readAnnotations, OutputSchema: outputSchema,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input recallInput) (*mcp.CallToolResult, any, error) {
		if strings.TrimSpace(input.Query) == "" {
			return nil, nil, fail(ErrType, "recall query is blank; provide text to search")
		}
		k := 10
		if input.K != nil {
			if *input.K < 1 {
				return nil, nil, fail(ErrType, "recall k=%d is invalid; use 1..20", *input.K)
			}
			if *input.K > 20 {
				return nil, nil, fail(ErrTooLarge, "recall k=%d exceeds 20; request at most 20 hits", *input.K)
			}
			k = *input.K
		}
		if input.Expand < 0 {
			return nil, nil, fail(ErrType, "recall expand=%d is invalid; use 0..2", input.Expand)
		}
		if input.Expand > 2 {
			return nil, nil, fail(ErrTooLarge, "recall expand=%d exceeds 2; use at most two reference hops", input.Expand)
		}
		optionsSearch := SearchOpts{Text: input.Query, K: k, Expand: input.Expand}
		if options.Embed != nil {
			vector, err := options.Embed(ctx, input.Query)
			if err != nil {
				return nil, nil, wrap(ErrType, err, "embedding command failed for recall query; correct --embed-cmd")
			}
			optionsSearch.Vector = vector
			optionsSearch.VectorAttribute = "memory/embedding"
		}
		result, err := db.Search(ctx, optionsSearch)
		return toolMCPOutput(ctx, db, result, err, result.BasisTx)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name: "about", Description: "Pull the current facts about an entity. Example: {\"entity\":\"ada\",\"depth\":1}.",
		Annotations: readAnnotations, OutputSchema: outputSchema,
	}, func(ctx context.Context, request *mcp.CallToolRequest, input aboutInput) (*mcp.CallToolResult, any, error) {
		arguments, err := exactMCPArguments(request)
		if err != nil {
			return nil, nil, err
		}
		depth := 1
		if input.Depth != nil {
			depth = *input.Depth
		}
		if depth < 0 || depth > 2 {
			return nil, nil, fail(ErrTooLarge, "about depth %d is invalid; use 0..2", depth)
		}
		view, basis, err := mcpPinnedView(ctx, db)
		if err != nil {
			return nil, nil, err
		}
		entity, err := view.Entity(ctx, arguments["entity"], depth)
		return toolMCPOutput(ctx, db, entity, err, basis)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name: "why", Description: "Explain current facts with full transaction provenance. Example: {\"entity\":\"ada\",\"attribute\":\"person/city\"}.",
		Annotations: readAnnotations, OutputSchema: outputSchema,
	}, func(ctx context.Context, request *mcp.CallToolRequest, input auditInput) (*mcp.CallToolResult, any, error) {
		arguments, err := exactMCPArguments(request)
		if err != nil {
			return nil, nil, err
		}
		limit, err := mcpAuditLimit(input.Limit)
		if err != nil {
			return nil, nil, err
		}
		view, basis, err := mcpPinnedView(ctx, db)
		if err != nil {
			return nil, nil, err
		}
		if input.Attribute != nil {
			if *input.Attribute == "" {
				return nil, nil, fail(ErrSchema, "why attribute is empty; provide namespace/name or omit it")
			}
			facts, readErr := view.Why(ctx, arguments["entity"], *input.Attribute)
			return toolMCPOutput(ctx, db, mcpFactPage(facts, limit), readErr, basis)
		}
		facts, err := view.Why(ctx, arguments["entity"])
		return toolMCPOutput(ctx, db, mcpFactPage(facts, limit), err, basis)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name: "history", Description: "Read a fact timeline including asserting and retracting provenance. Example: {\"entity\":\"ada\",\"attribute\":\"person/city\"}.",
		Annotations: readAnnotations, OutputSchema: outputSchema,
	}, func(ctx context.Context, request *mcp.CallToolRequest, input auditInput) (*mcp.CallToolResult, any, error) {
		arguments, err := exactMCPArguments(request)
		if err != nil {
			return nil, nil, err
		}
		limit, err := mcpAuditLimit(input.Limit)
		if err != nil {
			return nil, nil, err
		}
		view, basis, err := mcpPinnedView(ctx, db)
		if err != nil {
			return nil, nil, err
		}
		if input.Attribute != nil {
			if *input.Attribute == "" {
				return nil, nil, fail(ErrSchema, "history attribute is empty; provide namespace/name or omit it")
			}
			facts, readErr := view.History(ctx, arguments["entity"], *input.Attribute)
			return toolMCPOutput(ctx, db, mcpFactPage(facts, limit), readErr, basis)
		}
		facts, err := view.History(ctx, arguments["entity"])
		return toolMCPOutput(ctx, db, mcpFactPage(facts, limit), err, basis)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name: "query", Description: "Run canonical read-only Datalog. Example: {\"q\":{\"find\":[\"?n\"],\"where\":[[\"ada\",\"person/name\",\"?n\"]]}}.",
		Annotations: readAnnotations, OutputSchema: outputSchema,
	}, func(ctx context.Context, request *mcp.CallToolRequest, _ queryInput) (*mcp.CallToolResult, any, error) {
		arguments, err := exactMCPArguments(request)
		if err != nil {
			return nil, nil, err
		}
		args := map[string]any{}
		if rawArgs, ok := arguments["args"]; ok {
			var valid bool
			args, valid = objectMap(rawArgs)
			if !valid {
				return nil, nil, fail(ErrType, "query args must be a JSON object; provide variable bindings by name")
			}
		}
		query, err := ParseQuery(arguments["q"])
		if err != nil {
			return nil, nil, err
		}
		if query.Limit == nil {
			limit := 100
			query.Limit = &limit
		} else if *query.Limit < 0 {
			return nil, nil, fail(ErrType, "MCP query limit %d is invalid; use 0..1000", *query.Limit)
		} else if *query.Limit > 1000 {
			return nil, nil, fail(ErrTooLarge, "MCP query limit %d exceeds 1000", *query.Limit)
		}
		view, basis, err := mcpPinnedView(ctx, db)
		if err != nil {
			return nil, nil, err
		}
		result, err := view.Query(ctx, query, args)
		return toolMCPOutput(ctx, db, result, err, basis)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name: "receipt", Description: "Read a durable transaction and operation receipt. Example: {\"tx\":70}.",
		Annotations: readAnnotations, OutputSchema: outputSchema,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input receiptInput) (*mcp.CallToolResult, any, error) {
		view, basis, err := mcpPinnedView(ctx, db)
		if err != nil {
			return nil, nil, err
		}
		receipt, err := view.Receipt(ctx, input.Tx)
		return toolMCPOutput(ctx, db, receipt, err, basis)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name: "schema", Description: "List effective application attribute schemas and observed logical types. Example: {\"prefix\":\"person/\"}.",
		Annotations: readAnnotations, OutputSchema: outputSchema,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input schemaInput) (*mcp.CallToolResult, any, error) {
		limit := mcpResourcePage
		if input.Limit != nil {
			if *input.Limit < 1 {
				return nil, nil, fail(ErrType, "MCP schema limit %d is invalid; use 1..100", *input.Limit)
			}
			if *input.Limit > mcpResourcePage {
				return nil, nil, fail(ErrTooLarge, "MCP schema limit %d exceeds 100", *input.Limit)
			}
			limit = *input.Limit
		}
		view := db
		prefix := input.Prefix
		includeSystem := false
		if input.IncludeSystem != nil {
			includeSystem = *input.IncludeSystem
		}
		offset := 0
		expectedDigest := ""
		if input.Cursor != "" {
			cursor, err := decodeMCPSchemaCursor(input.Cursor)
			if err != nil {
				return nil, nil, err
			}
			if input.Prefix != nil && (cursor.Prefix == nil || *input.Prefix != *cursor.Prefix) {
				return nil, nil, fail(ErrConflict, "schema cursor does not match prefix; restart pagination")
			}
			if input.IncludeSystem != nil && *input.IncludeSystem != cursor.IncludeSystem {
				return nil, nil, fail(ErrConflict, "schema cursor does not match include_system; restart pagination")
			}
			view, err = db.mcpResourceView(ctx, cursor.Basis)
			if err != nil {
				return nil, nil, err
			}
			prefix, includeSystem = cursor.Prefix, cursor.IncludeSystem
			offset, expectedDigest = cursor.Offset, cursor.Digest
		}
		prefixValue := ""
		if prefix != nil {
			prefixValue = *prefix
		}
		snapshot, err := view.Schema(ctx, prefixValue, includeSystem)
		if err != nil {
			return nil, nil, err
		}
		if expectedDigest != "" && snapshot.Digest != expectedDigest {
			return nil, nil, fail(ErrConflict, "schema cursor digest no longer matches its basis; restart pagination")
		}
		total := len(snapshot.Attributes) + len(snapshot.Shapes)
		if offset > total {
			return nil, nil, fail(ErrConflict, "schema cursor is outside its snapshot; restart pagination")
		}
		attributes, shapes, end := paginateMCPSchema(snapshot, offset, limit)
		snapshot.Attributes, snapshot.Shapes = attributes, shapes
		var next *string
		if end < total {
			encoded, err := encodeMCPSchemaCursor(mcpSchemaCursor{
				Version: 1, Basis: snapshot.BasisTx, Offset: end, Prefix: prefix,
				IncludeSystem: includeSystem, Digest: snapshot.Digest,
			})
			if err != nil {
				return nil, nil, err
			}
			next = &encoded
		}
		return toolMCPOutput(ctx, db, mcpSchemaResult{SchemaSnapshot: snapshot, NextCursor: next, Truncated: next != nil}, nil, snapshot.BasisTx)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name: "datoms", Description: "Read a bounded, basis-pinned index page; continue with next_cursor.",
		Annotations: readAnnotations, OutputSchema: outputSchema,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input datomsInput) (*mcp.CallToolResult, any, error) {
		limit := 100
		if input.Limit != nil {
			if *input.Limit < 1 {
				return nil, nil, fail(ErrType, "MCP datom limit %d is invalid; use 1..100", *input.Limit)
			}
			if *input.Limit > 100 {
				return nil, nil, fail(ErrTooLarge, "MCP datom limit %d exceeds 100", *input.Limit)
			}
			limit = *input.Limit
		}
		page, err := db.Datoms(ctx, DatomOptions{
			Index: input.Index, Source: input.Source, Components: input.Components,
			Cursor: input.Cursor, Limit: limit,
		})
		return toolMCPOutput(ctx, db, page, err, page.BasisTx)
	})
	mcp.AddTool(server, &mcp.Tool{
		Name: "explain", Description: "Return the deterministic indexed plan for a canonical query without executing it.",
		Annotations: readAnnotations, OutputSchema: outputSchema,
	}, func(ctx context.Context, request *mcp.CallToolRequest, _ queryInput) (*mcp.CallToolResult, any, error) {
		arguments, err := exactMCPArguments(request)
		if err != nil {
			return nil, nil, err
		}
		args := map[string]any{}
		if raw, ok := arguments["args"]; ok {
			args, ok = objectMap(raw)
			if !ok {
				return nil, nil, fail(ErrType, "explain args must be an object")
			}
		}
		plan, err := db.ExplainJSON(ctx, arguments["q"], args)
		return toolMCPOutput(ctx, db, plan, err, plan.BasisTx)
	})
	return server
}

func encodeMCPSchemaCursor(cursor mcpSchemaCursor) (string, error) {
	return encodeMCPCursor(cursor)
}

func encodeMCPCursor(cursor any) (string, error) {
	encoded, err := canonicalMCPBytes(cursor)
	value := ""
	if err == nil {
		value = base64.RawURLEncoding.EncodeToString(encoded)
	}
	return value, err
}

func decodeMCPSchemaCursor(value string) (mcpSchemaCursor, error) {
	if len(value) > maxMCPResourceCursor {
		return mcpSchemaCursor{}, fail(ErrTooLarge, "schema cursor is too large; restart pagination")
	}
	encoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || base64.RawURLEncoding.EncodeToString(encoded) != value {
		return mcpSchemaCursor{}, fail(ErrType, "schema cursor is malformed; restart pagination")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var cursor mcpSchemaCursor
	if err := decoder.Decode(&cursor); err != nil {
		return mcpSchemaCursor{}, wrap(ErrType, err, "schema cursor is malformed; restart pagination")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return mcpSchemaCursor{}, fail(ErrType, "schema cursor has trailing data; restart pagination")
	}
	if cursor.Version != 1 || cursor.Basis < GenesisTx || cursor.Offset < 0 || cursor.Digest == "" {
		return mcpSchemaCursor{}, fail(ErrType, "schema cursor is invalid; restart pagination")
	}
	return cursor, nil
}

func exactMCPArguments(request *mcp.CallToolRequest) (map[string]any, error) {
	if request == nil || request.Params == nil || len(bytes.TrimSpace(request.Params.Arguments)) == 0 {
		return map[string]any{}, nil
	}
	decoded, err := DecodeJSON(bytes.NewReader(request.Params.Arguments))
	if err != nil {
		return nil, err
	}
	arguments, ok := objectMap(decoded)
	if !ok {
		return nil, fail(ErrType, "MCP tool arguments must be a JSON object; provide named tool arguments")
	}
	return arguments, nil
}

func canonicalMCPBytes(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot encode MCP structured output")
	}
	decoded, err := decodeInternalDocumentJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot normalize MCP structured output")
	}
	encoded, err := canonicalJSON(decoded)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot canonicalize MCP structured output")
	}
	return encoded, nil
}

func mcpAuditLimit(value *int) (int, error) {
	if value == nil {
		return 100, nil
	}
	if *value < 1 {
		return 0, fail(ErrType, "audit limit %d is invalid; use 1..100", *value)
	}
	if *value > 100 {
		return 0, fail(ErrTooLarge, "audit limit %d exceeds 100", *value)
	}
	return *value, nil
}

func mcpFactPage(facts []Fact, limit int) mcpItemsResult {
	truncated := len(facts) > limit
	if truncated {
		facts = facts[:limit]
	}
	return mcpItemsResult{Items: facts, Truncated: truncated}
}

func toolMCPOutput(
	_ context.Context,
	_ *DB,
	value any,
	resultErr error,
	basis int64,
) (*mcp.CallToolResult, any, error) {
	if resultErr != nil {
		return nil, nil, resultErr
	}
	return boundedMCPOutput(basis, value, nil)
}

func boundedMCPOutput(basis int64, value any, resultErr error) (*mcp.CallToolResult, any, error) {
	if resultErr != nil {
		return nil, nil, resultErr
	}
	envelope := mcpEnvelope{OK: true, BasisTx: basis, Data: value}
	encoded, err := canonicalMCPBytes(envelope)
	if err != nil {
		return nil, nil, err
	}
	if len(encoded) > MaxMCPOutputBytes {
		return nil, nil, fail(ErrTooLarge, "MCP structured output is %d bytes; narrow the request to at most %d canonical JSON bytes", len(encoded), MaxMCPOutputBytes)
	}
	return nil, envelope, nil
}

func RunMCP(ctx context.Context, db *DB, options MCPOptions) error {
	if db == nil {
		return fail(ErrType, "MCP database is nil; open an fgraph database before serving")
	}
	return NewMCPServer(db, options).Run(ctx, &mcp.StdioTransport{})
}
