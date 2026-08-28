package fgraph

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func deterministicEventIDs(t *testing.T) (EventIDFactory, *int) {
	t.Helper()
	calls := 0
	return func() (string, error) {
		calls++
		return fmt.Sprintf("00000000-0000-4000-8000-%012x", 0x40+calls), nil
	}, &calls
}

func openV2DB(t *testing.T) (*DB, *int) {
	t.Helper()
	factory, calls := deterministicEventIDs(t)
	db, err := Open(":memory:", WithClock(func() int64 { return 1_767_225_600_000_000 }), WithEventIDFactory(factory))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTest(t, db) })
	return db, calls
}

type schemaResourcePage struct {
	NextURI    string            `json:"next_uri"`
	Attributes []SchemaAttribute `json:"attributes"`
	BasisTx    int64             `json:"basis_tx"`
}

func TestV2RegistryEventsBasisOperationAndCAS(t *testing.T) {
	t.Setenv("FGRAPH_EVENT_SEED", "injected-factory-must-win")
	ctx := context.Background()
	db, eventCalls := openV2DB(t)
	var version, facts, identities, events int64
	if err := db.store.sql.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	for query, destination := range map[string]*int64{
		"SELECT COUNT(*) FROM fgraph_facts WHERE tx=64": &facts,
		"SELECT COUNT(*) FROM fgraph_ids":               &identities,
		"SELECT COUNT(*) FROM fgraph_events":            &events,
	} {
		if err := db.store.sql.QueryRow(query).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	if version != 2 || facts != GenesisFactCount || identities != 19 || events != 1 {
		t.Fatalf("genesis version=%d facts=%d identities=%d events=%d", version, facts, identities, events)
	}

	seed, seedErr := db.Transact(ctx, E{"id": "ada", "person/name": "Ada"}, WithOperationID("person:ada:create"), IfBasis(GenesisTx))
	if seedErr != nil {
		t.Fatal(seedErr)
	}
	if seed.BasisTx != GenesisTx || seed.EventID != "00000000-0000-4000-8000-000000000041" || seed.Tx == 0 {
		t.Fatalf("seed receipt = %#v", seed)
	}
	retry, retryErr := db.Transact(ctx, E{"person/name": "Ada", "id": "ada"}, WithOperationID("person:ada:create"), IfBasis(GenesisTx))
	if retryErr != nil || retry.Tx != seed.Tx || retry.EventID != seed.EventID || *eventCalls != 1 {
		t.Fatalf("idempotent retry = %#v, %v calls=%d", retry, retryErr, *eventCalls)
	}
	if _, err := db.Transact(ctx, E{"id": "ada", "person/name": "Augusta"}, WithOperationID("person:ada:create")); !errors.Is(err, ErrConflict) {
		t.Fatalf("operation mismatch = %v", err)
	}
	if _, err := db.Transact(ctx, E{"id": "ada", "person/name": "Augusta"}, IfBasis(GenesisTx)); !errors.Is(err, ErrConflict) || *eventCalls != 1 {
		t.Fatalf("stale basis = %v calls=%d", err, *eventCalls)
	}

	changed, changeErr := db.Transact(ctx, []any{"cas", "ada", "person/name", "Ada", "Augusta"})
	if changeErr != nil || changed.BasisTx != seed.Tx {
		t.Fatalf("cas = %#v, %v", changed, changeErr)
	}
	if _, err := db.Transact(ctx, []any{"cas", "ada", "person/name", "Ada", "Wrong"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("cas mismatch = %v", err)
	}
	if _, err := db.Transact(ctx, []any{
		[]any{"cas", "ada", "person/name", "Augusta", "Ada"},
		[]any{"assert", "ada", "person/name", "Other"},
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("cas isolation = %v", err)
	}
	if report, err := db.Doctor(ctx); err != nil || !report.OK {
		t.Fatalf("doctor = %#v, %v", report, err)
	}
}

func TestV2ExciseIsIdempotentBeforeBasisCheck(t *testing.T) {
	ctx := context.Background()
	db, calls := openV2DB(t)
	seed, seedErr := db.Transact(ctx, []any{
		E{"id": "victim", "item/value": "secret"},
		E{"id": "survivor", "item/value": "public"},
	})
	if seedErr != nil {
		t.Fatal(seedErr)
	}

	report, exciseErr := db.Excise(ctx, "victim", WithOperationID("erase:victim"), IfBasis(seed.Tx))
	if exciseErr != nil {
		t.Fatal(exciseErr)
	}
	if report.Status != "applied" || report.BasisTx != seed.Tx || report.EventID == "" || len(report.Retracted) == 0 {
		t.Fatalf("excision receipt = %#v", report)
	}
	retry, retryErr := db.Excise(ctx, "victim", WithOperationID("erase:victim"), IfBasis(seed.Tx))
	if retryErr != nil || retry.Status != "already_applied" || retry.Tx != report.Tx || retry.BasisTx != seed.Tx || *calls != 2 {
		t.Fatalf("stale-basis retry = %#v err=%v event_calls=%d", retry, retryErr, *calls)
	}
	if _, err := db.Excise(ctx, "survivor", WithOperationID("erase:victim"), IfBasis(report.Tx)); !errors.Is(err, ErrConflict) {
		t.Fatalf("operation id reuse = %v", err)
	}
	if _, err := db.Excise(ctx, "survivor", WithOperationID("erase:survivor"), IfBasis(seed.Tx)); !errors.Is(err, ErrConflict) {
		t.Fatalf("new operation with stale basis = %v", err)
	}
	receipt, receiptErr := db.Receipt(ctx, report.Tx)
	if receiptErr != nil || receipt.OperationID == nil || *receipt.OperationID != "erase:victim" || receipt.RequestHash == nil {
		t.Fatalf("durable excision receipt = %#v err=%v", receipt, receiptErr)
	}
	if doctor, err := db.Doctor(ctx); err != nil || !doctor.OK {
		t.Fatalf("doctor after excision = %#v err=%v", doctor, err)
	}
}

func TestV2ExciseApplicationAttributeRemovesItsDatoms(t *testing.T) {
	ctx := context.Background()
	db, _ := openV2DB(t)
	declaration, declareErr := db.Declare(ctx, "private/value", Type("text"))
	if declareErr != nil {
		t.Fatal(declareErr)
	}
	write, writeErr := db.Transact(ctx, []any{
		E{"id": "first", "private/value": "one", "public/value": 1},
		E{"id": "second", "private/value": "two", "public/value": 2},
	})
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	excision, exciseErr := db.Excise(ctx, "private/value", WithOperationID("erase:private-value"), IfBasis(write.Tx))
	if exciseErr != nil {
		t.Fatal(exciseErr)
	}
	if len(excision.Retracted) < 3 {
		t.Fatalf("attribute excision retracted = %#v", excision.Retracted)
	}
	var attributeID int64
	if err := db.store.sql.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE name='private/value'").Scan(&attributeID); err != nil {
		t.Fatal(err)
	}
	var remaining int64
	if err := db.store.sql.QueryRowContext(ctx, "SELECT COUNT(*) FROM fgraph_facts WHERE a=?", attributeID).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("attribute datoms remain = %d, %v", remaining, err)
	}
	for _, entity := range []string{"first", "second"} {
		facts, err := db.Entity(ctx, entity)
		if err != nil || facts["private/value"] != nil || facts["public/value"] == nil {
			t.Fatalf("entity %s after attribute excision = %#v, %v", entity, facts, err)
		}
	}
	for _, tx := range []int64{declaration.Tx, write.Tx} {
		var eventData sql.NullString
		if err := db.store.sql.QueryRowContext(ctx, "SELECT event_data FROM fgraph_events WHERE tx=?", tx).Scan(&eventData); err != nil || eventData.Valid {
			t.Fatalf("attribute event %d was not redacted: %#v, %v", tx, eventData, err)
		}
	}
	if doctor, err := db.Doctor(ctx); err != nil || !doctor.OK {
		t.Fatalf("doctor after attribute excision = %#v, %v", doctor, err)
	}
}

func TestV2OperationIDBoundary(t *testing.T) {
	ctx := context.Background()
	db, _ := openV2DB(t)
	if _, err := db.Transact(ctx, E{"id": "boundary", "item/value": 1}, WithOperationID(strings.Repeat("x", 512))); err != nil {
		t.Fatalf("512-byte operation id = %v", err)
	}
	for _, operationID := range []string{strings.Repeat("x", 513), "bad\noperation"} {
		if _, err := db.Transact(ctx, E{"id": "invalid", "item/value": 1}, WithOperationID(operationID)); !errors.Is(err, ErrType) {
			t.Fatalf("invalid operation id error = %v", err)
		}
	}
}

func TestV2DeterministicEnvironmentEventAndExactLayout(t *testing.T) {
	t.Setenv("FGRAPH_EVENT_SEED", "conformance")
	ctx := context.Background()
	db, openErr := Open(":memory:", WithClock(func() int64 { return 1_767_225_600_000_000 }))
	if openErr != nil {
		t.Fatal(openErr)
	}
	report, transactErr := db.Transact(ctx, E{"id": "seed", "item/value": 1})
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	if report.Tx != 67 || report.EventID != "b4e2a762-c76a-45c5-a2af-4d4e18bb3473" {
		t.Fatalf("seeded event = tx %d event %s", report.Tx, report.EventID)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "layout.db")
	db, openErr = Open(path, WithClock(func() int64 { return 1_767_225_600_000_000 }))
	if openErr != nil {
		t.Fatal(openErr)
	}
	if _, err := db.store.sql.ExecContext(ctx, "CREATE TABLE application_data(value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Doctor(ctx); !errors.Is(err, ErrFormat) {
		t.Fatalf("doctor extra-object error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(path); !errors.Is(err, ErrFormat) {
		if reopened != nil {
			closeTest(t, reopened)
		}
		t.Fatalf("open extra-object error = %v", err)
	}

	modified := filepath.Join(t.TempDir(), "modified.db")
	db, openErr = Open(modified, WithClock(func() int64 { return 1_767_225_600_000_000 }))
	if openErr != nil {
		t.Fatal(openErr)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, rawOpenErr := sql.Open("sqlite", modified)
	if rawOpenErr != nil {
		t.Fatal(rawOpenErr)
	}
	if _, err := raw.Exec("DROP INDEX fgraph_ids_created; CREATE INDEX fgraph_ids_created ON fgraph_ids(id,created_tx)"); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(modified); !errors.Is(err, ErrFormat) {
		if reopened != nil {
			closeTest(t, reopened)
		}
		t.Fatalf("open modified-DDL error = %v", err)
	}
}

func TestV2ReceiptAndPortableFollow(t *testing.T) {
	ctx := context.Background()
	db, _ := openV2DB(t)
	first, firstErr := db.Transact(ctx, E{"id": "ada", "person/name": "Ada"},
		WithOperationID("ada:create"), WithBy("agent"), WithSource("test"),
		WithMeta(E{"ticket": 7}), WithTxFacts(E{"audit/kind": "seed"}))
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	second, secondErr := db.Transact(ctx, E{"id": "ada", "person/name": "Augusta"})
	if secondErr != nil {
		t.Fatal(secondErr)
	}
	receipt, receiptErr := db.Receipt(ctx, first.Tx)
	if receiptErr != nil {
		t.Fatal(receiptErr)
	}
	if receipt.ReadBasisTx != second.Tx || receipt.BasisTx != GenesisTx || receipt.Tx != first.Tx || receipt.Event != first.EventID {
		t.Fatalf("receipt basis/identity = %#v", receipt)
	}
	if receipt.OperationID == nil || *receipt.OperationID != "ada:create" || receipt.RequestHash == nil || !strings.HasPrefix(*receipt.RequestHash, "sha256:") || !strings.HasPrefix(receipt.EventHash, "sha256:") {
		t.Fatalf("receipt operation hashes = %#v", receipt)
	}
	if receipt.By == nil || *receipt.By != "agent" || receipt.Source == nil || *receipt.Source != "test" || receipt.Meta == nil || len(receipt.Facts) != 1 || receipt.Facts[0].A != "audit/kind" {
		t.Fatalf("receipt metadata = %#v", receipt)
	}
	if _, err := db.atTx(first.Tx).Receipt(ctx, second.Tx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("future receipt through historical view = %v", err)
	}

	followCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	event := <-db.Follow(followCtx, FollowOptions{Since: first.Tx, Interval: time.Millisecond})
	if event.Err != nil || event.Tx != second.Tx || event.Record["fgraph"] != "event/1" || event.Record["event"] != second.EventID {
		t.Fatalf("follow event = %#v", event)
	}
	if _, localTx := event.Record["tx"]; localTx {
		t.Fatalf("portable follow record leaked local tx: %#v", event.Record)
	}
	if event.Record["redacted"] != nil {
		t.Fatalf("ordinary event unexpectedly redacted: %#v", event.Record)
	}
	secondReceipt, secondReceiptErr := db.Receipt(ctx, second.Tx)
	if secondReceiptErr != nil {
		t.Fatal(secondReceiptErr)
	}
	_, hash, canonicalErr := canonicalEventData(event.Record)
	if canonicalErr != nil || secondReceipt.EventHash != "sha256:"+hex.EncodeToString(hash[:]) {
		t.Fatalf("follow hash = %s receipt=%s err=%v", hex.EncodeToString(hash[:]), secondReceipt.EventHash, canonicalErr)
	}
}

func TestV2HistoricalIdentityQueryDatomsAndExplain(t *testing.T) {
	ctx := context.Background()
	db, _ := openV2DB(t)
	seed, seedErr := db.Transact(ctx, E{"id": "ada", "person/name": "Ada"})
	if seedErr != nil {
		t.Fatal(seedErr)
	}
	changed, changeErr := db.Transact(ctx, []any{"cas", "ada", "person/name", "Ada", "Augusta"})
	if changeErr != nil {
		t.Fatal(changeErr)
	}
	past := db.atTx(GenesisTx)
	if _, err := past.Entity(ctx, "ada"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("future identity leaked into genesis view: %v", err)
	}
	stats, statsErr := past.Stats(ctx)
	if statsErr != nil || stats.Entities != 18 || stats.Transactions != 1 || stats.Facts != GenesisFactCount {
		t.Fatalf("historical stats = %#v, %v", stats, statsErr)
	}

	current, currentErr := db.Query(ctx, Q{
		Find: []any{"?a", "?v"}, Where: []any{[]any{"ada", "?a", "?v"}},
	}, nil)
	if currentErr != nil || len(current.Rows) != 1 {
		t.Fatalf("variable attribute query = %#v, %v", current, currentErr)
	}
	history, historyErr := db.Query(ctx, Q{
		Source: "history", Find: []any{"?v", "?tx", "?added"},
		Where: []any{[]any{"ada", "person/name", "?v", "?tx", "?added"}},
		Order: []any{[]any{"?added", "desc"}},
	}, nil)
	if historyErr != nil || len(history.Rows) != 3 {
		t.Fatalf("five-position history = %#v, %v", history, historyErr)
	}
	added := 0
	removed := 0
	for _, row := range history.Rows {
		if row[2] == true {
			added++
		} else {
			removed++
		}
	}
	if added != 2 || removed != 1 {
		t.Fatalf("history added=%d removed=%d rows=%#v", added, removed, history.Rows)
	}
	currentEvent, currentEventErr := db.Query(ctx, Q{
		Find:  []any{"?v"},
		Where: []any{[]any{"ada", "person/name", "?v", changed.Tx, true}},
	}, nil)
	if currentEventErr != nil || len(currentEvent.Rows) != 1 || currentEvent.Rows[0][0] != "Augusta" {
		t.Fatalf("bound five-position current query = %#v, %v", currentEvent, currentEventErr)
	}
	removedCurrent, removedErr := db.Query(ctx, Q{
		Find:  []any{"?v"},
		Where: []any{[]any{"ada", "person/name", "?v", changed.Tx, false}},
	}, nil)
	if removedErr != nil || len(removedCurrent.Rows) != 0 {
		t.Fatalf("current query exposed removal datom = %#v, %v", removedCurrent, removedErr)
	}

	first, firstPageErr := db.Datoms(ctx, DatomOptions{Index: "eavt", Source: "history", Components: []any{"ada", "person/name"}, Limit: 1})
	if firstPageErr != nil || len(first.Items) != 1 || first.NextCursor == "" || first.BasisTx != changed.Tx {
		t.Fatalf("first datom page = %#v, %v", first, firstPageErr)
	}
	second, secondPageErr := db.Datoms(ctx, DatomOptions{Index: "eavt", Source: "history", Components: []any{"ada", "person/name"}, Limit: 1, Cursor: first.NextCursor})
	if secondPageErr != nil || len(second.Items) != 1 || second.BasisTx != first.BasisTx {
		t.Fatalf("second datom page = %#v, %v", second, secondPageErr)
	}
	plan, planErr := db.Explain(ctx, Q{Find: []any{"?v"}, Where: []any{[]any{"ada", "person/name", "?v"}}}, nil)
	if planErr != nil || len(plan.Clauses) != 1 || plan.Clauses[0].Access != "eavt/ea" || plan.BasisTx != changed.Tx {
		t.Fatalf("explain = %#v, %v", plan, planErr)
	}
	_ = seed
}

func TestV2SchemaShapesAndFilterCorrectSearch(t *testing.T) {
	ctx := context.Background()
	db, _ := openV2DB(t)
	if _, err := db.Declare(ctx, "person/name", Type("text")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "person/embedding", Type("vector"), Dims(2), VectorModel("test/model-v1")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{
		"id": "shape/person", "fgraph/shape-required": []any{RefTo("person/name")},
		"fgraph/shape-allowed": []any{RefTo("person/name")}, "fgraph/shape-closed": true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "ada", "fgraph/shape": RefTo("shape/person"), "person/name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "invalid", "fgraph/shape": RefTo("shape/person")}); !errors.Is(err, ErrSchema) {
		t.Fatalf("missing required shape value = %v", err)
	}
	snapshot, schemaErr := db.Schema(ctx, "person/", false)
	if schemaErr != nil || len(snapshot.Attributes) != 2 || snapshot.Digest == "" || len(snapshot.Shapes) != 1 {
		t.Fatalf("schema snapshot = %#v, %v", snapshot, schemaErr)
	}
	var model string
	for _, attribute := range snapshot.Attributes {
		if attribute.Name == "person/embedding" && attribute.Effective.VectorModel != nil {
			model = *attribute.Effective.VectorModel
		}
	}
	if model != "test/model-v1" {
		t.Fatalf("vector model = %q", model)
	}

	items := make([]any, 0, 61)
	for index := 0; index < 60; index++ {
		items = append(items, E{"id": fmt.Sprintf("decoy-%02d", index), "doc/text": "needle", "doc/enabled": false})
	}
	items = append(items, E{"id": "target", "doc/text": "needle", "doc/enabled": true})
	if _, err := db.Transact(ctx, items); err != nil {
		t.Fatal(err)
	}
	result, searchErr := db.Search(ctx, SearchOpts{Text: "needle", Filters: [][]any{{"doc/enabled", true}}, K: 1})
	if searchErr != nil || len(result.Hits) != 1 || result.Hits[0].Entity != "target" {
		t.Fatalf("filter-correct search = %#v, %v", result, searchErr)
	}
	if result.Truncated {
		t.Fatalf("single filtered result was marked truncated: %#v", result)
	}
	longText := strings.Repeat("needle ", 400)
	if _, err := db.Transact(ctx, E{"id": "large-match", "doc/text": longText}); err != nil {
		t.Fatal(err)
	}
	bounded, boundedErr := db.Search(ctx, SearchOpts{Text: "needle", K: 100})
	if boundedErr != nil {
		t.Fatal(boundedErr)
	}
	largeMatched := false
	for _, hit := range bounded.Hits {
		for _, fact := range hit.Matched {
			if hit.Entity == "large-match" && fact.ValueTruncated {
				text, textOK := fact.V.(string)
				if !textOK {
					t.Fatalf("truncated matched text has type %T", fact.V)
				}
				largeMatched = len(text) <= maxMatchedValueBytes+len("…")
			}
		}
	}
	if !largeMatched {
		t.Fatalf("large matched value was not safely summarized: %#v", bounded)
	}
	if _, err := db.Transact(ctx, E{"id": "vector-match", "person/embedding": Vector([]float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	semantic, semanticErr := db.Search(ctx, SearchOpts{Vector: []float32{1, 0}, VectorAttribute: "person/embedding", K: 1})
	if semanticErr != nil || len(semantic.Hits) != 1 || len(semantic.Hits[0].Matched) != 1 || !semantic.Hits[0].Matched[0].ValueTruncated {
		t.Fatalf("vector matched summary = %#v, %v", semantic, semanticErr)
	}
	if value, ok := semantic.Hits[0].Matched[0].V.(map[string]any); !ok || value["vector_dims"] != 2 {
		t.Fatalf("vector matched value leaked embedding: %#v", semantic.Hits[0].Matched[0].V)
	}

	if _, err := db.Transact(ctx, E{"id": "invalid-doctor"}); err != nil {
		t.Fatal(err)
	}
	var nameAttr, shapeID, invalidEntity, latestTx int64
	for query, destination := range map[string]*int64{
		"SELECT id FROM fgraph_ids WHERE name='person/name'":    &nameAttr,
		"SELECT id FROM fgraph_ids WHERE name='shape/person'":   &shapeID,
		"SELECT id FROM fgraph_ids WHERE name='invalid-doctor'": &invalidEntity,
		"SELECT MAX(tx) FROM fgraph_events":                     &latestTx,
	} {
		if err := db.store.sql.QueryRowContext(ctx, query).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_facts SET v='int' WHERE e=? AND a=8 AND rx IS NULL", nameAttr); err != nil {
		t.Fatal(err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (?,15,?,0,?,NULL)", invalidEntity, shapeID, latestTx); err != nil {
		t.Fatal(err)
	}
	doctor, doctorErr := db.Doctor(ctx)
	if doctorErr != nil || doctor.SchemaProblems == 0 || doctor.ShapeViolations == 0 || doctor.OK {
		t.Fatalf("global schema/shape doctor = %#v, %v", doctor, doctorErr)
	}
}

func TestV2MCPDefaultsReadOnlyAndExposesBoundedSurfaces(t *testing.T) {
	ctx := context.Background()
	db, _ := openV2DB(t)
	wide := E{"id": "schema-page"}
	for index := 0; index < 105; index++ {
		wide[fmt.Sprintf("page/a%03d", index)] = index
	}
	if _, err := db.Transact(ctx, wide); err != nil {
		t.Fatal(err)
	}
	server := NewMCPServer(db, MCPOptions{})
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
	client := mcp.NewClient(&mcp.Implementation{Name: "v2-test", Version: "1"}, nil)
	clientSession, clientErr := client.Connect(ctx, clientTransport, nil)
	if clientErr != nil {
		t.Fatal(clientErr)
	}
	t.Cleanup(func() {
		if closeErr := clientSession.Close(); closeErr != nil {
			t.Errorf("close MCP client session: %v", closeErr)
		}
	})
	if instructions := clientSession.InitializeResult().Instructions; !strings.Contains(instructions, "read-only") {
		t.Fatalf("MCP instructions = %q", instructions)
	}
	tools, toolsErr := clientSession.ListTools(ctx, nil)
	if toolsErr != nil {
		t.Fatal(toolsErr)
	}
	names := make([]string, len(tools.Tools))
	for index, tool := range tools.Tools {
		names[index] = tool.Name
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint || !tool.Annotations.IdempotentHint {
			t.Fatalf("read tool %s annotations = %#v", tool.Name, tool.Annotations)
		}
		if tool.OutputSchema == nil {
			t.Fatalf("read tool %s has no output schema", tool.Name)
		}
	}
	if slices.Contains(names, "remember") || !slices.Contains(names, "datoms") || !slices.Contains(names, "explain") {
		t.Fatalf("default MCP tools = %v", names)
	}
	resources, resourcesErr := clientSession.ListResourceTemplates(ctx, nil)
	if resourcesErr != nil {
		t.Fatal(resourcesErr)
	}
	if len(resources.ResourceTemplates) != 5 {
		encoded, marshalErr := json.Marshal(resources)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		t.Fatalf("resource templates = %s", encoded)
	}
	readSchema := func(uri string) schemaResourcePage {
		t.Helper()
		result, readErr := clientSession.ReadResource(ctx, &mcp.ReadResourceParams{URI: uri})
		if readErr != nil || len(result.Contents) != 1 {
			t.Fatalf("read schema resource %q = %#v, %v", uri, result, readErr)
		}
		var page schemaResourcePage
		if err := json.Unmarshal([]byte(result.Contents[0].Text), &page); err != nil {
			t.Fatal(err)
		}
		return page
	}
	firstPage := readSchema("fgraph://schema?prefix=page%2F")
	if len(firstPage.Attributes) != mcpResourcePage || firstPage.NextURI == "" {
		t.Fatalf("first schema page = %#v", firstPage)
	}
	if _, err := db.Transact(ctx, E{"id": "later", "page/after-cursor": true}); err != nil {
		t.Fatal(err)
	}
	secondPage := readSchema(firstPage.NextURI)
	if secondPage.BasisTx != firstPage.BasisTx || len(secondPage.Attributes) != 5 || secondPage.NextURI != "" {
		t.Fatalf("pinned second schema page = %#v", secondPage)
	}
	tampered, parseErr := url.Parse(firstPage.NextURI)
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	query := tampered.Query()
	query.Set("prefix", "other/")
	tampered.RawQuery = query.Encode()
	if _, err := clientSession.ReadResource(ctx, &mcp.ReadResourceParams{URI: tampered.String()}); err == nil {
		t.Fatal("schema cursor accepted a changed prefix")
	}
}
