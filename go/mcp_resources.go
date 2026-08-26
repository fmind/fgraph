package fgraph

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	mcpResourcePage      = 100
	maxMCPResourceCursor = 4096
)

type mcpReceiptResource struct {
	EventReceipt
	Truncated bool `json:"truncated"`
}

func (resource mcpReceiptResource) MarshalJSON() ([]byte, error) {
	fields := eventReceiptFields(resource.EventReceipt)
	fields = append(fields, Field{Name: "truncated", Value: resource.Truncated})
	return marshalOrderedObject(fields)
}

type mcpResourceCursor struct {
	Resource string `json:"resource"`
	Argument string `json:"argument"`
	Digest   string `json:"digest,omitempty"`
	Basis    int64  `json:"basis"`
	Position int64  `json:"position,omitempty"`
	Offset   int    `json:"offset,omitempty"`
	Version  int    `json:"version"`
}

func registerMCPResources(server *mcp.Server, db *DB) {
	annotations := &mcp.Annotations{Audience: []mcp.Role{"assistant"}, Priority: 0.9}
	server.AddResourceTemplate(&mcp.ResourceTemplate{
		Name: "fgraph-schema", Title: "fgraph schema", MIMEType: "application/json",
		Description: "Bounded, basis-pinned attribute and shape introspection.",
		URITemplate: "fgraph://schema{?prefix,cursor}", Annotations: annotations,
	}, func(ctx context.Context, request *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		uri, uriErr := parseMCPResourceURI(request)
		if uriErr != nil {
			return nil, uriErr
		}
		prefix := uri.Query().Get("prefix")
		view := db
		offset := 0
		expectedDigest := ""
		if raw := uri.Query().Get("cursor"); raw != "" {
			cursor, cursorErr := decodeMCPResourceCursor(raw, "schema", prefix)
			if cursorErr != nil {
				return nil, cursorErr
			}
			var viewErr error
			view, viewErr = db.mcpResourceView(ctx, cursor.Basis)
			if viewErr != nil {
				return nil, viewErr
			}
			offset, expectedDigest = cursor.Offset, cursor.Digest
		}
		snapshot, schemaErr := view.Schema(ctx, prefix, false)
		if schemaErr != nil {
			return nil, schemaErr
		}
		if expectedDigest != "" && expectedDigest != snapshot.Digest {
			return nil, fail(ErrConflict, "schema cursor digest changed at basis %d; restart without a cursor", snapshot.BasisTx)
		}
		total := len(snapshot.Attributes) + len(snapshot.Shapes)
		if offset > total {
			return nil, fail(ErrConflict, "schema cursor is outside this snapshot; restart without a cursor")
		}
		attributes, shapes, nextOffset := paginateMCPSchema(snapshot, offset, mcpResourcePage)
		body := map[string]any{
			"basis_tx": snapshot.BasisTx, "digest": snapshot.Digest,
			"attributes": attributes, "shapes": shapes,
		}
		if nextOffset < total {
			cursor, cursorErr := encodeMCPResourceCursor(mcpResourceCursor{
				Version: 1, Resource: "schema", Argument: prefix, Basis: snapshot.BasisTx,
				Offset: nextOffset, Digest: snapshot.Digest,
			})
			if cursorErr != nil {
				return nil, cursorErr
			}
			next := *uri
			query := next.Query()
			query.Set("cursor", cursor)
			next.RawQuery = query.Encode()
			body["next_uri"] = next.String()
		}
		return mcpJSONResource(request.Params.URI, body)
	})

	server.AddResourceTemplate(&mcp.ResourceTemplate{
		Name: "fgraph-entity", Title: "fgraph entity datoms", MIMEType: "application/json",
		Description: "Current entity datoms, bounded to 100 per opaque cursor.",
		URITemplate: "fgraph://entity/{entity}{?at,cursor}", Annotations: annotations,
	}, func(ctx context.Context, request *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		uri, err := parseMCPResourceURI(request)
		if err != nil {
			return nil, err
		}
		selectorText, err := url.PathUnescape(strings.TrimPrefix(uri.Path, "/"))
		if err != nil || selectorText == "" {
			return nil, mcp.ResourceNotFoundError(request.Params.URI)
		}
		selector := mcpEntitySelector(selectorText)
		view := db
		if at := uri.Query().Get("at"); at != "" {
			value, parseErr := strconv.ParseInt(at, 10, 64)
			if parseErr != nil {
				return nil, fail(ErrType, "entity resource at=%q is invalid; use a transaction id", at)
			}
			view, err = db.At(ctx, value)
			if err != nil {
				return nil, err
			}
		}
		page, err := view.Datoms(ctx, DatomOptions{
			Index: "eavt", Source: "current", Components: []any{selector},
			Cursor: uri.Query().Get("cursor"), Limit: mcpResourcePage,
		})
		if err != nil {
			return nil, err
		}
		body := map[string]any{"basis_tx": page.BasisTx, "items": page.Items}
		if page.NextCursor != "" {
			next := *uri
			query := next.Query()
			query.Set("cursor", page.NextCursor)
			next.RawQuery = query.Encode()
			body["next_uri"] = next.String()
		}
		return mcpJSONResource(request.Params.URI, body)
	})

	server.AddResourceTemplate(&mcp.ResourceTemplate{
		Name: "fgraph-transaction", Title: "fgraph transaction", MIMEType: "application/json",
		Description: "One immutable event receipt with bounded fact evidence.",
		URITemplate: "fgraph://tx/{tx}", Annotations: annotations,
	}, func(ctx context.Context, request *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		uri, err := parseMCPResourceURI(request)
		if err != nil {
			return nil, err
		}
		text, err := url.PathUnescape(strings.TrimPrefix(uri.Path, "/"))
		if err != nil || text == "" {
			return nil, mcp.ResourceNotFoundError(request.Params.URI)
		}
		id, parseErr := strconv.ParseInt(text, 10, 64)
		if parseErr != nil {
			err = db.withRead(ctx, func(runner sqlRunner) error {
				var found bool
				id, found, err = db.resolveReadEntity(ctx, runner, mcpEntitySelector(text))
				if err == nil && !found {
					return mcp.ResourceNotFoundError(request.Params.URI)
				}
				return err
			})
		}
		if err != nil {
			return nil, err
		}
		receipt, err := db.Receipt(ctx, id)
		if err != nil {
			return nil, err
		}
		truncated := len(receipt.Facts) > mcpResourcePage
		if truncated {
			receipt.Facts = receipt.Facts[:mcpResourcePage]
		}
		return mcpJSONResource(request.Params.URI, mcpReceiptResource{EventReceipt: receipt, Truncated: truncated})
	})

	server.AddResourceTemplate(&mcp.ResourceTemplate{
		Name: "fgraph-changes", Title: "fgraph change feed", MIMEType: "application/json",
		Description: "Portable committed event/1 records after a transaction, 100 events per page.",
		URITemplate: "fgraph://changes{?since,cursor}", Annotations: annotations,
	}, func(ctx context.Context, request *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		uri, err := parseMCPResourceURI(request)
		if err != nil {
			return nil, err
		}
		sinceArgument := uri.Query().Get("since")
		if sinceArgument == "" {
			sinceArgument = strconv.FormatInt(GenesisTx, 10)
		}
		since, parseErr := strconv.ParseInt(sinceArgument, 10, 64)
		if parseErr != nil || since < GenesisTx {
			return nil, fail(ErrType, "changes since=%q is invalid; use a transaction at or after genesis", sinceArgument)
		}
		basis, basisErr := db.mcpVisibleBasis(ctx)
		if basisErr != nil {
			return nil, basisErr
		}
		position := since
		if raw := uri.Query().Get("cursor"); raw != "" {
			cursor, cursorErr := decodeMCPResourceCursor(raw, "changes", sinceArgument)
			if cursorErr != nil {
				return nil, cursorErr
			}
			if _, viewErr := db.mcpResourceView(ctx, cursor.Basis); viewErr != nil {
				return nil, viewErr
			}
			basis, position = cursor.Basis, cursor.Position
		}
		body, changesErr := db.mcpChanges(ctx, basis, position, sinceArgument, request.Params.URI)
		if changesErr != nil {
			return nil, changesErr
		}
		return mcpJSONResource(request.Params.URI, body)
	})
}

func parseMCPResourceURI(request *mcp.ReadResourceRequest) (*url.URL, error) {
	if request == nil || request.Params == nil {
		return nil, fail(ErrType, "MCP resource request is missing parameters")
	}
	uri, err := url.Parse(request.Params.URI)
	if err != nil || uri.Scheme != "fgraph" {
		return nil, mcp.ResourceNotFoundError(request.Params.URI)
	}
	return uri, nil
}

func mcpEntitySelector(value string) any {
	if _, err := parseUUID(value); err == nil {
		return map[string]any{"eid": value}
	}
	return value
}

func mcpJSONResource(uri string, value any) (*mcp.ReadResourceResult, error) {
	encoded, err := canonicalMCPBytes(value)
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaxMCPOutputBytes {
		return nil, fail(ErrTooLarge, "MCP resource %q exceeds %d canonical JSON bytes; narrow the prefix or use its cursor", uri, MaxMCPOutputBytes)
	}
	return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{
		URI: uri, MIMEType: "application/json", Text: string(encoded),
	}}}, nil
}

func encodeMCPResourceCursor(cursor mcpResourceCursor) (string, error) {
	return encodeMCPCursor(cursor)
}

func decodeMCPResourceCursor(value, resource, argument string) (mcpResourceCursor, error) {
	if len(value) > maxMCPResourceCursor {
		return mcpResourceCursor{}, fail(ErrTooLarge, "%s resource cursor is too large; restart without a cursor", resource)
	}
	encoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || base64.RawURLEncoding.EncodeToString(encoded) != value {
		return mcpResourceCursor{}, fail(ErrType, "%s resource cursor is malformed; restart without a cursor", resource)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var cursor mcpResourceCursor
	if err := decoder.Decode(&cursor); err != nil {
		return mcpResourceCursor{}, wrap(ErrType, err, "%s resource cursor is malformed; restart without a cursor", resource)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return mcpResourceCursor{}, fail(ErrType, "%s resource cursor has trailing data; restart without a cursor", resource)
	}
	if cursor.Version != 1 || cursor.Resource != resource || cursor.Argument != argument || cursor.Basis < GenesisTx || cursor.Offset < 0 || cursor.Position < 0 {
		return mcpResourceCursor{}, fail(ErrConflict, "%s resource cursor belongs to another request; restart without a cursor", resource)
	}
	if resource == "changes" && (cursor.Position < GenesisTx || cursor.Position > cursor.Basis) {
		return mcpResourceCursor{}, fail(ErrConflict, "changes resource cursor position is outside its pinned basis; restart without a cursor")
	}
	return cursor, nil
}

func (db *DB) mcpVisibleBasis(ctx context.Context) (basis int64, resultErr error) {
	resultErr = db.withRead(ctx, func(runner sqlRunner) error {
		var err error
		basis, err = db.basisOn(ctx, runner)
		if err == nil && db.asOf != nil && *db.asOf < basis {
			basis = *db.asOf
		}
		return err
	})
	return basis, resultErr
}

func (db *DB) mcpResourceView(ctx context.Context, basis int64) (*DB, error) {
	visible, err := db.mcpVisibleBasis(ctx)
	if err != nil {
		return nil, err
	}
	if basis < GenesisTx || basis > visible {
		return nil, fail(ErrConflict, "resource cursor basis %d is not visible; restart without a cursor", basis)
	}
	return db.atTx(basis), nil
}

func (db *DB) mcpChanges(ctx context.Context, basis, since int64, sinceArgument, rawURI string) (map[string]any, error) {
	result := map[string]any{"basis_tx": basis, "events": []map[string]any{}}
	ids := []int64{}
	err := db.withRead(ctx, func(runner sqlRunner) (resultErr error) {
		rows, queryErr := runner.QueryContext(ctx, "SELECT tx FROM fgraph_events WHERE tx>? AND tx<=? ORDER BY tx LIMIT ?", since, basis, mcpResourcePage+1)
		if queryErr != nil {
			return wrap(ErrFormat, queryErr, "cannot read MCP change feed")
		}
		defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "MCP change rows")) }()
		for rows.Next() {
			var tx int64
			if scanErr := rows.Scan(&tx); scanErr != nil {
				return wrap(ErrFormat, scanErr, "cannot decode MCP change event")
			}
			ids = append(ids, tx)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return wrap(ErrFormat, rowsErr, "cannot finish MCP change feed")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	hasMore := len(ids) > mcpResourcePage
	if hasMore {
		ids = ids[:mcpResourcePage]
	}
	if len(ids) == 0 {
		return result, nil
	}
	events, err := db.EventRecords(ctx, since, ids[len(ids)-1])
	if err != nil {
		return nil, err
	}
	result["events"] = events
	if !hasMore {
		return result, nil
	}
	uri, err := url.Parse(rawURI)
	if err != nil {
		return nil, wrap(ErrType, err, "cannot continue malformed MCP changes resource URI")
	}
	cursor, err := encodeMCPResourceCursor(mcpResourceCursor{
		Version: 1, Resource: "changes", Argument: sinceArgument,
		Basis: basis, Position: ids[len(ids)-1],
	})
	if err != nil {
		return nil, err
	}
	query := uri.Query()
	query.Set("cursor", cursor)
	uri.RawQuery = query.Encode()
	result["next_uri"] = uri.String()
	return result, nil
}
