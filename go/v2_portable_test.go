package fgraph

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestV2PortableSnapshotRestoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	source, _ := openV2DB(t)
	first, transactErr := source.Transact(ctx, E{"id": "snapshot/item", "item/value": 1}, WithOperationID("snapshot:create"))
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	if _, err := source.Transact(ctx, E{"id": "snapshot/item", "item/value": 2}); err != nil {
		t.Fatal(err)
	}
	var snapshot bytes.Buffer
	if err := source.Snapshot(ctx, &snapshot); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(snapshot.String()), "\n")
	if len(lines) < 4 || !strings.Contains(lines[0], `"fgraph":"snapshot/1"`) || !strings.Contains(lines[len(lines)-1], `"fgraph":"end"`) {
		t.Fatalf("snapshot stream = %s", snapshot.String())
	}

	target, _ := openV2DB(t)
	if err := target.Restore(ctx, strings.NewReader(snapshot.String())); err != nil {
		t.Fatal(err)
	}
	entity, entityErr := target.Entity(ctx, "snapshot/item")
	if entityErr != nil || entity["item/value"] != int64(2) {
		t.Fatalf("restored entity = %#v, %v", entity, entityErr)
	}
	history, historyErr := target.History(ctx, "snapshot/item", "item/value")
	if historyErr != nil || len(history) != 2 {
		t.Fatalf("restored history = %#v, %v", history, historyErr)
	}
	receipt, receiptErr := target.Receipt(ctx, first.Tx)
	if receiptErr != nil || receipt.OperationID == nil || *receipt.OperationID != "snapshot:create" {
		t.Fatalf("restored receipt = %#v, %v", receipt, receiptErr)
	}
	var roundTrip bytes.Buffer
	if err := target.Snapshot(ctx, &roundTrip); err != nil || roundTrip.String() != snapshot.String() {
		t.Fatalf("snapshot round trip equal=%t err=%v\n%s\n%s", roundTrip.String() == snapshot.String(), err, snapshot.String(), roundTrip.String())
	}
	if err := target.Restore(ctx, strings.NewReader(snapshot.String())); !errors.Is(err, ErrConflict) {
		t.Fatalf("restore into non-pristine database = %v", err)
	}

	truncated, _ := openV2DB(t)
	broken := strings.Join(lines[:len(lines)-1], "\n")
	if err := truncated.Restore(ctx, strings.NewReader(broken)); err == nil {
		t.Fatal("truncated snapshot unexpectedly restored")
	}
	stats, statsErr := truncated.Stats(ctx)
	if statsErr != nil || stats.Transactions != 1 || stats.Facts != GenesisFactCount {
		t.Fatalf("truncated restore was not atomic: %#v, %v", stats, statsErr)
	}
}

func TestV2PortableApplyIsAtomicIdempotentAndPreservesEvent(t *testing.T) {
	ctx := context.Background()
	source, _ := openV2DB(t)
	original, transactErr := source.Transact(ctx, E{"item/value": "portable"}, WithBy("agent"), WithMeta(E{"run": 1}))
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	var stream bytes.Buffer
	if err := source.Tail(ctx, &stream, GenesisTx); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stream.String(), "imported_at") {
		t.Fatalf("event stream leaked local import metadata: %s", stream.String())
	}

	target, _ := openV2DB(t)
	reports, applyErr := target.Apply(ctx, strings.NewReader(stream.String()))
	if applyErr != nil || len(reports) != 1 || reports[0].EventID == "" {
		t.Fatalf("apply reports = %#v, %v", reports, applyErr)
	}
	var replayed bytes.Buffer
	if err := target.Tail(ctx, &replayed, GenesisTx); err != nil || replayed.String() != stream.String() {
		t.Fatalf("applied event changed: equal=%t err=%v\n%s\n%s", replayed.String() == stream.String(), err, stream.String(), replayed.String())
	}
	if _, err := source.Transact(ctx, E{"id": "later/event", "local/value": true}); err != nil {
		t.Fatal(err)
	}
	var later bytes.Buffer
	if err := source.Tail(ctx, &later, original.Tx); err != nil {
		t.Fatal(err)
	}
	currentReports, currentApplyErr := target.Apply(ctx, strings.NewReader(later.String()))
	if currentApplyErr != nil || len(currentReports) != 1 {
		t.Fatalf("later apply reports = %#v, %v", currentReports, currentApplyErr)
	}
	current := currentReports[0]
	retry, retryErr := target.Apply(ctx, strings.NewReader(stream.String()))
	if retryErr != nil || len(retry) != 1 || retry[0].Status != "already_applied" || retry[0].Tx != reports[0].Tx || retry[0].BasisTx != GenesisTx || current.Tx <= retry[0].Tx || len(retry[0].IDs) != 0 || len(retry[0].Asserted) != 0 || len(retry[0].Retracted) != 0 {
		t.Fatalf("apply retry = %#v, %v", retry, retryErr)
	}

	tampered := strings.Replace(stream.String(), "portable", "tampered", 1)
	if _, err := target.Apply(ctx, strings.NewReader(tampered)); !errors.Is(err, ErrConflict) {
		t.Fatalf("event collision = %v", err)
	}
	before, beforeErr := target.Stats(ctx)
	if beforeErr != nil {
		t.Fatal(beforeErr)
	}
	combined := stream.String() + strings.Replace(stream.String(), reports[0].EventID, "00000000-0000-4000-8000-000000000099", 1)
	if _, err := target.Apply(ctx, strings.NewReader(combined)); err == nil {
		t.Fatal("invalid second event unexpectedly applied")
	}
	after, afterErr := target.Stats(ctx)
	if afterErr != nil || before != after {
		t.Fatalf("multi-event apply was not atomic: before=%#v after=%#v err=%v", before, after, afterErr)
	}
}

func TestV2SearchFailedFiltersSpendWorkBudget(t *testing.T) {
	ctx := context.Background()
	factory, _ := deterministicEventIDs(t)
	db, openErr := Open(":memory:", WithClock(func() int64 { return 1_767_225_600_000_000 }), WithEventIDFactory(factory), WithQueryBudget(10))
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { closeTest(t, db) })
	items := make([]any, 0, 12)
	for index := 0; index < 11; index++ {
		items = append(items, E{"id": "budget/decoy-" + string(rune('a'+index)), "doc/text": "needle", "doc/enabled": false})
	}
	items = append(items, E{"id": "budget/target", "doc/text": "needle", "doc/enabled": true})
	if _, err := db.Transact(ctx, items); err != nil {
		t.Fatal(err)
	}
	_, searchErr := db.Search(ctx, SearchOpts{Text: "needle", Filters: [][]any{{"doc/enabled", true}}, K: 1})
	if !errors.Is(searchErr, ErrTooLarge) {
		t.Fatalf("failed filters did not spend the search work budget: %v", searchErr)
	}
}

func TestV2QueryPlannerReordersPatternBlocksAndPreservesBarriers(t *testing.T) {
	ctx := context.Background()
	db, _ := openV2DB(t)
	if _, err := db.Transact(ctx, []any{
		E{"id": "ada", "person/name": "Ada", "person/city": "Paris"},
		E{"id": "bob", "person/name": "Bob", "person/city": "London"},
	}); err != nil {
		t.Fatal(err)
	}
	query := Q{
		Find: []any{"?name"},
		Where: []any{
			[]any{"?e", "person/name", "?name"},
			[]any{"?e", "person/city", "Paris"},
		},
	}
	plan, err := db.Explain(ctx, query, nil)
	if err != nil || len(plan.Clauses) != 2 || plan.Clauses[0].Ordinal != 1 || plan.Clauses[0].Access != "avet" || plan.Clauses[1].Ordinal != 0 || plan.Clauses[1].Access != "eavt/batch" {
		t.Fatalf("greedy query plan = %#v, %v", plan, err)
	}
	result, err := db.Query(ctx, query, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != "Ada" {
		t.Fatalf("planned query result = %#v, %v", result, err)
	}

	barrier := Q{
		Find: []any{"?name"},
		Where: []any{
			[]any{"?e", "person/name", "?name"},
			[]any{"starts-with", "?name", "A"},
			[]any{"?e", "person/city", "Paris"},
		},
	}
	barrierPlan, err := db.Explain(ctx, barrier, nil)
	if err != nil || len(barrierPlan.Clauses) != 3 || barrierPlan.Clauses[0].Ordinal != 0 || barrierPlan.Clauses[1].Kind != "barrier" || barrierPlan.Clauses[2].Ordinal != 2 {
		t.Fatalf("barrier query plan = %#v, %v", barrierPlan, err)
	}
}

func TestV2EventDataSurvivesNohistoryAndExcisionIsAuditable(t *testing.T) {
	ctx := context.Background()
	db, _ := openV2DB(t)
	if _, err := db.Declare(ctx, "secret/value", Type("text"), NoHistory()); err != nil {
		t.Fatal(err)
	}
	first, firstErr := db.Transact(ctx, E{"id": "secret", "secret/value": "old"})
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	second, secondErr := db.Transact(ctx, E{"id": "secret", "secret/value": "new"})
	if secondErr != nil {
		t.Fatal(secondErr)
	}
	for _, tx := range []int64{first.Tx, second.Tx} {
		var eventData sql.NullString
		if err := db.store.sql.QueryRowContext(ctx, "SELECT event_data FROM fgraph_events WHERE tx=?", tx).Scan(&eventData); err != nil || !eventData.Valid {
			t.Fatalf("nohistory event %d data = %#v, %v", tx, eventData, err)
		}
	}
	var stream bytes.Buffer
	if err := db.Tail(ctx, &stream, GenesisTx); err != nil || !strings.Contains(stream.String(), `"old"`) {
		t.Fatalf("nohistory event stream is not replayable: %v\n%s", err, stream.String())
	}

	excision, exciseErr := db.Excise(ctx, "secret", WithOperationID("erase:secret"), IfBasis(second.Tx))
	if exciseErr != nil {
		t.Fatal(exciseErr)
	}
	redacted := []string{first.EventID, second.EventID}
	slices.Sort(redacted)
	for _, tx := range []int64{first.Tx, second.Tx} {
		var eventData sql.NullString
		if err := db.store.sql.QueryRowContext(ctx, "SELECT event_data FROM fgraph_events WHERE tx=?", tx).Scan(&eventData); err != nil || eventData.Valid {
			t.Fatalf("excised event %d data = %#v, %v", tx, eventData, err)
		}
	}
	var leaked int64
	if err := db.store.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM fgraph_events
		WHERE event_data LIKE '%"old"%' OR event_data LIKE '%"new"%'`).Scan(&leaked); err != nil || leaked != 0 {
		t.Fatalf("excised values remain in event_data: count=%d err=%v", leaked, err)
	}
	var excisionData string
	var excisionHash []byte
	if err := db.store.sql.QueryRowContext(ctx, "SELECT event_data,event_hash FROM fgraph_events WHERE tx=?", excision.Tx).Scan(&excisionData, &excisionHash); err != nil {
		t.Fatal(err)
	}
	record, err := decodeStoredEventData(excisionData, excisionHash)
	if err != nil {
		t.Fatal(err)
	}
	rawTargets, ok := record["redacts"].([]any)
	if !ok || len(rawTargets) != len(redacted) {
		t.Fatalf("redaction targets = %#v", record["redacts"])
	}
	for index, target := range redacted {
		if rawTargets[index] != target {
			t.Fatalf("redaction targets = %#v, want %v", rawTargets, redacted)
		}
	}
	if doctor, err := db.Doctor(ctx); err != nil || !doctor.OK || doctor.UnverifiableEvents != int64(len(redacted)) {
		t.Fatalf("doctor after audited redaction = %#v, %v", doctor, err)
	}

	var snapshot bytes.Buffer
	if err := db.Snapshot(ctx, &snapshot); err != nil {
		t.Fatal(err)
	}
	restored, _ := openV2DB(t)
	if err := restored.Restore(ctx, strings.NewReader(snapshot.String())); err != nil {
		t.Fatal(err)
	}
	if doctor, err := restored.Doctor(ctx); err != nil || !doctor.OK || doctor.UnverifiableEvents != int64(len(redacted)) {
		t.Fatalf("restored redaction audit = %#v, %v", doctor, err)
	}

	var redactedStream bytes.Buffer
	if err := db.Tail(ctx, &redactedStream, GenesisTx); err != nil {
		t.Fatal(err)
	}
	target, _ := openV2DB(t)
	if _, err := target.Apply(ctx, strings.NewReader(redactedStream.String())); !errors.Is(err, ErrType) {
		t.Fatalf("redacted event apply error = %v, want TypeError", err)
	}
}

func TestV2EventPayloadSemanticSelectorPositions(t *testing.T) {
	target, ok := eventSelectorKey("target")
	if !ok {
		t.Fatal("cannot build selector key")
	}
	base := func() map[string]any {
		return map[string]any{"created": []any{}, "asserted": []any{}, "retracted": []any{}}
	}
	for _, test := range []struct {
		change func(map[string]any)
		name   string
	}{
		{name: "created", change: func(event map[string]any) { event["created"] = []any{"target"} }},
		{name: "asserted entity", change: func(event map[string]any) { event["asserted"] = []any{[]any{"target", "item/value", "x", "text"}} }},
		{name: "asserted attribute", change: func(event map[string]any) { event["asserted"] = []any{[]any{"item", "target", "x", "text"}} }},
		{name: "asserted ref", change: func(event map[string]any) {
			event["asserted"] = []any{[]any{"item", "item/link", map[string]any{"ref": "target"}, "ref"}}
		}},
		{name: "retracted entity", change: func(event map[string]any) { event["retracted"] = []any{[]any{"target", "item/value", "x", "text"}} }},
		{name: "transaction attribute", change: func(event map[string]any) { event["tx_facts"] = []any{[]any{"target", "x", "text"}} }},
		{name: "transaction ref", change: func(event map[string]any) {
			event["tx_facts"] = []any{[]any{"item/link", map[string]any{"ref": "target"}, "ref"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			event := base()
			test.change(event)
			matched, err := eventRecordReferencesSelector(event, target)
			if err != nil || !matched {
				t.Fatalf("match = %t, %v", matched, err)
			}
		})
	}
	event := base()
	event["meta"] = map[string]any{"note": "target"}
	event["asserted"] = []any{[]any{"item", "item/value", "target", "text"}}
	if matched, err := eventRecordReferencesSelector(event, target); err != nil || matched {
		t.Fatalf("arbitrary value matched identity selector: %t, %v", matched, err)
	}
}

func TestV2DoctorRejectsUnauditedOrTamperedEventData(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name   string
		mutate string
	}{
		{name: "unaudited null", mutate: "UPDATE fgraph_events SET event_data=NULL WHERE tx=?"},
		{name: "noncanonical payload", mutate: "UPDATE fgraph_events SET event_data=event_data||' ' WHERE tx=?"},
		{name: "hash mismatch", mutate: "UPDATE fgraph_events SET event_hash=zeroblob(32) WHERE tx=?"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, _ := openV2DB(t)
			report, transactErr := db.Transact(ctx, E{"id": "item", "item/value": "value"})
			if transactErr != nil {
				t.Fatal(transactErr)
			}
			if _, err := db.store.sql.ExecContext(ctx, test.mutate, report.Tx); err != nil {
				t.Fatal(err)
			}
			doctor, err := db.Doctor(ctx)
			if err != nil || doctor.OK || len(doctor.Problems) == 0 {
				t.Fatalf("doctor accepted invalid event data: %#v, %v", doctor, err)
			}
		})
	}
}

func TestV2IdentityOnlyExcisionHasValidEmptyRedactionSet(t *testing.T) {
	ctx := context.Background()
	db, _ := openV2DB(t)
	identity, transactErr := db.Transact(ctx, E{"id": "identity-only"})
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	if _, err := db.Excise(ctx, "identity-only", WithOperationID("erase:identity-only"), IfBasis(identity.Tx)); err != nil {
		t.Fatal(err)
	}
	if doctor, err := db.Doctor(ctx); err != nil || !doctor.OK {
		t.Fatalf("identity-only excision doctor = %#v, %v", doctor, err)
	}
}
