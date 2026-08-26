package fgraph

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func indirectBlobKey(tag Tag, data []byte) []byte {
	digest := indirectDigest(tag, data)
	return append([]byte(nil), digest[:]...)
}

func TestValidIndirectBlobPhysicalDomain(t *testing.T) {
	validText := strings.Repeat("t", BlobThreshold+1)
	validBytes := []byte(strings.Repeat("b", BlobThreshold+1))
	validVector := []byte{0, 0, 0, 0}
	invalidUTF8 := strings.Repeat("\xff", BlobThreshold+1)
	inlineText := strings.Repeat("t", BlobThreshold)
	inlineBytes := []byte(strings.Repeat("b", BlobThreshold))
	oversizedText := strings.Repeat("t", MaxValueBytes+1)
	oversizedBytes := make([]byte, MaxValueBytes+1)
	oversizedVector := make([]byte, MaxValueBytes+4)

	tests := []struct {
		key  any
		data any
		name string
		tag  Tag
		want bool
	}{
		{name: "text", tag: TagTextRef, key: indirectBlobKey(TagTextRef, []byte(validText)), data: validText, want: true},
		{name: "bytes", tag: TagBytesRef, key: indirectBlobKey(TagBytesRef, validBytes), data: validBytes, want: true},
		{name: "vector", tag: TagVector, key: indirectBlobKey(TagVector, validVector), data: validVector, want: true},
		{name: "key domain", tag: TagTextRef, key: "not-a-blob", data: validText},
		{name: "key length", tag: TagTextRef, key: []byte("short"), data: validText},
		{name: "text storage domain", tag: TagTextRef, key: indirectBlobKey(TagTextRef, []byte(validText)), data: []byte(validText)},
		{name: "text UTF-8", tag: TagTextRef, key: indirectBlobKey(TagTextRef, []byte(invalidUTF8)), data: invalidUTF8},
		{name: "inline text", tag: TagTextRef, key: indirectBlobKey(TagTextRef, []byte(inlineText)), data: inlineText},
		{name: "oversized text", tag: TagTextRef, key: indirectBlobKey(TagTextRef, []byte(oversizedText)), data: oversizedText},
		{name: "bytes storage domain", tag: TagBytesRef, key: indirectBlobKey(TagBytesRef, validBytes), data: string(validBytes)},
		{name: "inline bytes", tag: TagBytesRef, key: indirectBlobKey(TagBytesRef, inlineBytes), data: inlineBytes},
		{name: "oversized bytes", tag: TagBytesRef, key: indirectBlobKey(TagBytesRef, oversizedBytes), data: oversizedBytes},
		{name: "vector storage domain", tag: TagVector, key: indirectBlobKey(TagVector, validVector), data: "not-a-blob"},
		{name: "empty vector", tag: TagVector, key: indirectBlobKey(TagVector, nil), data: []byte{}},
		{name: "oversized vector", tag: TagVector, key: indirectBlobKey(TagVector, oversizedVector), data: oversizedVector},
		{name: "unaligned vector", tag: TagVector, key: indirectBlobKey(TagVector, []byte{0, 0, 0}), data: []byte{0, 0, 0}},
		{name: "unknown tag", tag: TagJSON, key: indirectBlobKey(TagJSON, []byte(validText)), data: validText},
		{name: "tag-separated digest", tag: TagBytesRef, key: indirectBlobKey(TagTextRef, validBytes), data: validBytes},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := validIndirectBlob(test.tag, test.key, test.data); got != test.want {
				t.Fatalf("validIndirectBlob(%d, %T, %T) = %t, want %t", test.tag, test.key, test.data, got, test.want)
			}
		})
	}
}

func TestInstantTextRejectsMalformedRFC3339(t *testing.T) {
	for _, value := range []string{
		"2026-08-24T10:00:00",
		"2026/08-24T10:00:00Z",
		"2026-08-24T10:00:00,5Z",
		"202x-08-24T10:00:00Z",
		"2026-08-24T10:00:00.Z",
		"2026-08-24T10:00:00+0x:00",
		"2026-02-30T10:00:00Z",
	} {
		t.Run(value, func(t *testing.T) {
			_, err := scalarValue(Object{Fields: []Field{{Name: "instant", Value: value}}})
			if !errors.Is(err, ErrType) {
				t.Fatalf("instant %q error = %v, want TypeError", value, err)
			}
		})
	}
}

func TestIndirectBlobRowFailuresAreTyped(t *testing.T) {
	rowFailure := errors.New("injected indirect blob row failure")
	for _, test := range []struct {
		name string
		rule scriptedQuery
	}{
		{
			name: "scan",
			rule: scriptedQuery{
				columns: []string{"rowid", "hash", "data", "t"},
				rows:    [][]driver.Value{{int64(1)}},
			},
		},
		{
			name: "iteration",
			rule: scriptedQuery{
				columns: []string{"rowid", "hash", "data", "t"},
				nextErr: rowFailure,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.rule.contains = "SELECT b.rowid,b.hash,b.data,f.t"
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			if _, err := countInvalidIndirectBlobs(context.Background(), runner); !errors.Is(err, ErrFormat) {
				t.Fatalf("count invalid blobs error = %v, want FormatError", err)
			}
		})
	}
}

func scriptedReadDB(runner *sql.DB, names map[string]int64) *DB {
	return &DB{
		store: &store{sql: runner, path: ":memory:", names: names},
		exec:  runner,
	}
}

func TestPublicReadRowFailuresAreTyped(t *testing.T) {
	ctx := context.Background()
	rowFailure := errors.New("injected read row failure")

	t.Run("entity name lookup", func(t *testing.T) {
		runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{{
			contains: "SELECT id FROM fgraph_ids",
			err:      rowFailure,
		}}})
		if _, err := scriptedReadDB(runner, map[string]int64{}).Entity(ctx, "entity"); !errors.Is(err, ErrFormat) {
			t.Fatalf("entity name lookup error = %v, want FormatError", err)
		}
	})

	for _, test := range []struct {
		name    string
		queries []scriptedQuery
	}{
		{
			name: "entity scan",
			queries: []scriptedQuery{{
				contains: "SELECT f.id,f.e,f.a,f.v,f.t,f.tx,f.rx,i.name",
				columns:  []string{"id", "e", "a", "v", "t", "tx", "rx", "name"},
				rows:     [][]driver.Value{{int64(1)}},
			}},
		},
		{
			name: "entity iteration",
			queries: []scriptedQuery{{
				contains: "SELECT f.id,f.e,f.a,f.v,f.t,f.tx,f.rx,i.name",
				columns:  []string{"id", "e", "a", "v", "t", "tx", "rx", "name"},
				nextErr:  rowFailure,
			}},
		},
		{
			name: "empty entity existence",
			queries: []scriptedQuery{
				{
					contains: "SELECT f.id,f.e,f.a,f.v,f.t,f.tx,f.rx,i.name",
					columns:  []string{"id", "e", "a", "v", "t", "tx", "rx", "name"},
				},
				{contains: "SELECT EXISTS(SELECT 1 FROM fgraph_ids", err: rowFailure},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: test.queries})
			if _, err := scriptedReadDB(runner, map[string]int64{"entity": 65}).Entity(ctx, "entity"); !errors.Is(err, ErrFormat) {
				t.Fatalf("entity read error = %v, want FormatError", err)
			}
		})
	}

	t.Run("raw fact iteration", func(t *testing.T) {
		runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{{
			contains: "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts",
			columns:  []string{"id", "e", "a", "v", "t", "tx", "rx"},
			nextErr:  rowFailure,
		}}})
		if _, err := scriptedReadDB(runner, map[string]int64{}).RawFacts(ctx, true); !errors.Is(err, ErrFormat) {
			t.Fatalf("raw fact iteration error = %v, want FormatError", err)
		}
	})

	t.Run("reference constant resolution", func(t *testing.T) {
		runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{{
			contains: "SELECT EXISTS",
			err:      rowFailure,
		}}})
		db := scriptedReadDB(runner, map[string]int64{})
		evaluator := &queryEvaluator{ctx: ctx, runner: runner, db: db}
		_, _, _, err := evaluator.preparePatternTerm(RefTo(int64(65)), false)
		if !errors.Is(err, ErrFormat) {
			t.Fatalf("reference constant resolution error = %v, want FormatError", err)
		}
	})
}

func TestNestedApplyReleaseFailureIsTyped(t *testing.T) {
	ctx := context.Background()
	releaseFailure := errors.New("injected savepoint release failure")
	runner := openScriptedSQL(t, scriptedSQL{execs: []scriptedExec{
		{contains: "SAVEPOINT fgraph_apply"},
		{contains: "RELEASE fgraph_apply", err: releaseFailure},
	}})
	db := scriptedReadDB(runner, map[string]int64{})
	if _, err := db.Apply(ctx, strings.NewReader("")); !errors.Is(err, ErrFormat) {
		t.Fatalf("nested apply release error = %v, want FormatError", err)
	}
}

func TestQueryOrderingPropagatesComparisonErrors(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	evaluator := &queryEvaluator{ctx: ctx, db: db, rules: map[string][]ruleDef{}, relations: map[string][][]cell{}}
	rows := []binding{
		{"?name": {tag: TagText, value: "a"}, "?sort": {tag: TagJSON, value: make(chan int)}},
		{"?name": {tag: TagText, value: "b"}, "?sort": {tag: TagJSON, value: make(chan int)}},
	}
	_, err := evaluator.project(Q{
		Find:  []any{"?name"},
		In:    []string{"?sort"},
		Order: []any{[]any{"?sort", "asc"}},
	}, rows)
	if !errors.Is(err, ErrQuery) {
		t.Fatalf("non-canonical order comparison error = %v, want QueryError", err)
	}
}

func TestQueryReferenceConstantMissingEntityDoesNotMatch(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	err := db.withRead(ctx, func(runner sqlRunner) error {
		evaluator := &queryEvaluator{ctx: ctx, runner: runner, db: db}
		_, exact, matched, prepareErr := evaluator.preparePatternTerm(RefTo(int64(999_999)), false)
		if prepareErr != nil {
			return prepareErr
		}
		if !exact || matched {
			t.Fatalf("missing reference constant = exact %t, matched %t; want true, false", exact, matched)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueryEvaluatorRejectsMalformedClauseObjects(t *testing.T) {
	evaluator := &queryEvaluator{
		ctx: context.Background(), db: fixedDB(t, ":memory:"),
		rules: map[string][]ruleDef{}, relations: map[string][][]cell{},
	}
	tests := []struct {
		clause any
		name   string
	}{
		{clause: true, name: "non-object"},
		{clause: Object{Fields: []Field{{Name: "not", Value: true}}}, name: "not body"},
		{clause: Object{Fields: []Field{{Name: "or", Value: true}}}, name: "or body"},
		{clause: Object{Fields: []Field{{Name: "rule", Value: true}}}, name: "rule invocation"},
		{clause: Object{Fields: []Field{{Name: "unknown", Value: []any{}}}}, name: "unknown object"},
		{clause: []any{int64(1), "item/value"}, name: "short pattern"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := evaluator.evalClauses([]any{test.clause}, []binding{{}}); !errors.Is(err, ErrQuery) {
				t.Fatalf("malformed clause error = %v, want QueryError", err)
			}
		})
	}
}

func TestMCPAuditToolsAcceptEntityWithoutAttribute(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, E{"id": "audit-target", "item/value": int64(1)}); err != nil {
		t.Fatal(err)
	}

	server := NewMCPServer(db, MCPOptions{})
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, serverSession)
	client := mcp.NewClient(&mcp.Implementation{Name: "audit-contract", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, clientSession)

	for _, tool := range []string{"why", "history"} {
		t.Run(tool, func(t *testing.T) {
			result, callErr := clientSession.CallTool(ctx, &mcp.CallToolParams{
				Name: tool, Arguments: map[string]any{"entity": "audit-target"},
			})
			if callErr != nil || result.IsError {
				t.Fatalf("%s without attribute = %#v, %v", tool, result, callErr)
			}
		})
	}
}

func statsQueries(attributeRule scriptedQuery) []scriptedQuery {
	count := func(contains string) scriptedQuery {
		return scriptedQuery{contains: contains, columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}}
	}
	return []scriptedQuery{
		count("SELECT COUNT(*) FROM fgraph_ids"),
		count("SELECT COUNT(*) FROM fgraph_facts"),
		count("SELECT COUNT(*) FROM fgraph_facts WHERE rx IS NULL"),
		count("SELECT COUNT(*) FROM fgraph_facts WHERE a=1"),
		count("SELECT COUNT(*) FROM fgraph_blobs"),
		attributeRule,
	}
}

func TestStatisticsAttributeRowFailuresAreTyped(t *testing.T) {
	ctx := context.Background()
	rowFailure := errors.New("injected attribute row failure")
	for _, test := range []struct {
		name string
		rule scriptedQuery
	}{
		{name: "query", rule: scriptedQuery{contains: "SELECT name FROM fgraph_ids", err: rowFailure}},
		{name: "scan", rule: scriptedQuery{
			contains: "SELECT name FROM fgraph_ids", columns: []string{"name"}, rows: [][]driver.Value{{}},
		}},
		{name: "iteration", rule: scriptedQuery{
			contains: "SELECT name FROM fgraph_ids", columns: []string{"name"}, nextErr: rowFailure,
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: statsQueries(test.rule)})
			if _, err := scriptedReadDB(runner, map[string]int64{}).Stats(ctx); !errors.Is(err, ErrFormat) {
				t.Fatalf("statistics error = %v, want FormatError", err)
			}
		})
	}
}

func TestDoctorSupportingRowFailuresAreTyped(t *testing.T) {
	ctx := context.Background()
	rowFailure := errors.New("injected doctor row failure")
	for _, test := range []struct {
		name string
		run  func(*sql.DB) error
		rule scriptedQuery
	}{
		{
			name: "genesis scan",
			run: func(runner *sql.DB) error {
				_, _, _, err := readGenesisReceipt(ctx, runner)
				return err
			},
			rule: scriptedQuery{contains: "SELECT v,t,tx,rx", columns: []string{"v", "t", "tx", "rx"}, rows: [][]driver.Value{{int64(1)}}},
		},
		{
			name: "genesis iteration",
			run: func(runner *sql.DB) error {
				_, _, _, err := readGenesisReceipt(ctx, runner)
				return err
			},
			rule: scriptedQuery{contains: "SELECT v,t,tx,rx", columns: []string{"v", "t", "tx", "rx"}, nextErr: rowFailure},
		},
		{
			name: "expected FTS scan",
			run: func(runner *sql.DB) error {
				_, err := scriptedReadDB(runner, map[string]int64{}).readExpectedFTSRows(ctx, runner)
				return err
			},
			rule: scriptedQuery{contains: "SELECT f.id,CASE", columns: []string{"id", "text"}, rows: [][]driver.Value{{"not-an-id", "text"}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := openScriptedSQL(t, scriptedSQL{queries: []scriptedQuery{test.rule}})
			if err := test.run(runner); !errors.Is(err, ErrFormat) {
				t.Fatalf("doctor row error = %v, want FormatError", err)
			}
		})
	}
}

func TestAttributesRejectCorruptDocumentationDomain(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	_, err := db.Declare(ctx, "profile/name", Doc("Human-readable name"))
	if err != nil {
		t.Fatal(err)
	}
	var attribute int64
	if err := db.store.sql.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE name=?", "profile/name").Scan(&attribute); err != nil {
		t.Fatal(err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_facts SET t=? WHERE e=? AND a=10 AND rx IS NULL", TagBytes, attribute); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Attributes(ctx, "profile/", false); !errors.Is(err, ErrFormat) {
		t.Fatalf("corrupt attribute documentation error = %v, want FormatError", err)
	}
}
