package fgraph

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"testing"
)

func TestTransactInputValidationMatrix(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	invalid := []any{
		42,
		[]any{42},
		[]any{"assert"},
		[]any{"retract"},
		E{"id": int64(0), "item/value": 1},
		E{"id": true, "item/value": 1},
		E{"id": Object{Fields: []Field{{Name: "tmp", Value: ""}}}, "item/value": 1},
		E{"id": Object{Fields: []Field{{Name: "tmp", Value: 1}}}, "item/value": 1},
		E{"id": "bad-attr", "INVALID": 1},
		E{"id": "nested", "item/value": E{"nested/value": 1}},
		E{"id": "array", "item/value": []any{1, 2}},
		[]any{"assert", "bad-attribute", 1, "x"},
		[]any{"assert", float64(1.5), "item/value", 1},
		[]any{"assert", true, "item/value", 1},
		[]any{"assert", Object{Fields: []Field{{Name: "tmp", Value: 1}}}, "item/value", 1},
		E{"id": "bad-ref-id", "item/ref": RefTo(int64(0))},
		E{"id": "bad-ref-type", "item/ref": RefTo(true)},
		E{"id": "bad-ref-tmp", "item/ref": RefTo(Object{Fields: []Field{{Name: "tmp", Value: 1}}})},
	}
	for _, value := range invalid {
		if _, err := db.Transact(ctx, value); err == nil {
			t.Errorf("invalid transaction %#v unexpectedly succeeded", value)
		}
	}
	for _, values := range [][]any{{}, {int64(1)}, {int64(1), int64(2)}} {
		if _, err := db.Transact(ctx, E{"id": "card-one-array", "item/value": values}); !errors.Is(err, ErrConflict) {
			t.Errorf("cardinality-one array %#v error = %v", values, err)
		}
	}
	for _, value := range []any{
		E{"id": int64(999), "item/value": 1},
		E{"id": "dangling-ref", "item/ref": RefTo(int64(999))},
		[]any{"assert", int64(999), "item/value", 1},
		[]any{"assert", float64(999), "item/value", 1},
	} {
		if _, err := db.Transact(ctx, value); !errors.Is(err, ErrNotFound) {
			t.Errorf("unknown numeric write %#v error = %v", value, err)
		}
	}
	if report, err := db.Transact(ctx, []any{"retract", int64(999)}); err != nil || report.Tx != 0 {
		t.Fatalf("unknown numeric retract = %+v, %v", report, err)
	}
	if report, err := db.Transact(ctx, []any{"retract", "missing"}); err != nil || report.Tx != 0 {
		t.Fatalf("missing entity retract = %+v, %v", report, err)
	}
	if report, err := db.Transact(ctx, []any{"retract", "bad-attribute", "missing/attr"}); err != nil || report.Tx != 0 {
		t.Fatalf("missing attribute retract = %+v, %v", report, err)
	}
}

func TestInternalRequestHashOptionsFailClosed(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	operationID := "invalid-request-hash-base"
	cases := []struct {
		option TxOption
		name   string
	}{
		{
			name: "wrong digest length",
			option: func(options *txOptions) {
				options.requestHash = []byte{1}
			},
		},
		{
			name: "override and base",
			option: func(options *txOptions) {
				options.requestHash = make([]byte, 32)
				options.requestHashBase = map[string]any{"operation": "invalid"}
			},
		},
		{
			name: "non canonical base",
			option: func(options *txOptions) {
				options.operationID = &operationID
				options.requestHashBase = map[string]any{"unsupported": make(chan struct{})}
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := db.Transact(ctx, []any{}, test.option); !errors.Is(err, ErrType) {
				t.Fatalf("request hash option error = %v, want TypeError", err)
			}
			if stats, err := db.Stats(ctx); err != nil || stats.Transactions != 1 {
				t.Fatalf("failed request hash validation changed transactions: %+v, %v", stats, err)
			}
		})
	}
}

func TestIdentityOnlyTempIDIsAnonymousNoOp(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	var before int64
	if err := db.store.sql.QueryRowContext(ctx, "SELECT value FROM fgraph_meta WHERE key='next_id'").Scan(&before); err != nil {
		t.Fatal(err)
	}
	for _, input := range []any{
		E{"id": Tmp("ephemeral")},
		[]any{E{"id": Object{Fields: []Field{{Name: "tmp", Value: "wire"}}}}},
	} {
		report, err := db.Transact(ctx, input)
		if err != nil {
			t.Fatalf("tempid-only transaction %#v = %v", input, err)
		}
		if report.Tx != 0 || len(report.IDs) != 0 || len(report.Asserted) != 0 || len(report.Retracted) != 0 {
			t.Fatalf("tempid-only report = %+v", report)
		}
	}
	var after int64
	if err := db.store.sql.QueryRowContext(ctx, "SELECT value FROM fgraph_meta WHERE key='next_id'").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("tempid-only transaction advanced next_id from %d to %d", before, after)
	}
}

func TestCanceledAnonymousAllocationGapsAreCompacted(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	report, err := db.Transact(ctx, []any{
		E{"id": Tmp("discard"), "gap/value": int64(1)},
		[]any{"retract", Tmp("discard"), "gap/value", int64(1)},
		E{"id": "holder", "gap/ref": RefTo(Tmp("target"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Tx != 69 || report.IDs["holder"] != 66 || report.IDs["target"] != 68 {
		t.Fatalf("compacted report = %+v", report)
	}
	if _, exists := report.IDs["discard"]; exists {
		t.Fatalf("canceled tempid leaked into report: %+v", report.IDs)
	}
	holder, err := db.Entity(ctx, "holder")
	if err != nil {
		t.Fatal(err)
	}
	if db.store.names["gap/value"] != 65 || db.store.names["gap/ref"] != 67 {
		t.Fatalf("compacted attribute ids = gap/value:%d gap/ref:%d", db.store.names["gap/value"], db.store.names["gap/ref"])
	}
	ref, ok := objectMap(holder["gap/ref"])
	if !ok || ref["ref"] != int64(68) {
		t.Fatalf("remapped reference = %#v", holder["gap/ref"])
	}
	var next int64
	if err := db.store.sql.QueryRowContext(ctx, "SELECT value FROM fgraph_meta WHERE key='next_id'").Scan(&next); err != nil {
		t.Fatal(err)
	}
	if next != 70 {
		t.Fatalf("next_id=%d want=70", next)
	}
}

func TestTransactionOperationsApplyInInputOrder(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")

	canceled, err := db.Transact(ctx, []any{
		[]any{"assert", "new-ordered", "item/value", int64(1)},
		[]any{"retract", "new-ordered", "item/value", int64(1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Tx == 0 || canceled.Status != "applied" || len(canceled.Asserted) != 1 || canceled.Asserted[0].A != "fgraph/at" || len(canceled.Retracted) != 0 {
		t.Fatalf("assert then retract new fact = %+v", canceled)
	}
	if entity, entityErr := db.Entity(ctx, "new-ordered"); entityErr != nil || len(entity) != 0 {
		t.Fatalf("identity after canceled fact = %#v, %v", entity, entityErr)
	}

	seed, err := db.Transact(ctx, E{"id": "existing-ordered", "item/value": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := db.Transact(ctx, []any{
		[]any{"retract", "existing-ordered", "item/value", int64(1)},
		[]any{"assert", "existing-ordered", "item/value", int64(1)},
	})
	if err != nil || restored.Tx != 0 {
		t.Fatalf("retract then restore existing fact = %+v, %v", restored, err)
	}
	history, err := db.History(ctx, "existing-ordered")
	if err != nil || len(history) != 1 || history[0].Tx != seed.Tx || history[0].Rx != nil {
		t.Fatalf("restored history = %+v, %v", history, err)
	}

	removed, err := db.Transact(ctx, []any{
		[]any{"assert", "existing-ordered", "item/value", int64(1)},
		[]any{"retract", "existing-ordered", "item/value", int64(1)},
	})
	if err != nil || removed.Tx == 0 || len(removed.Asserted) != 1 || len(removed.Retracted) != 1 {
		// The transaction receipt is asserted; the already-live domain fact is
		// removed because the preceding duplicate assert changes no state.
		t.Fatalf("duplicate assert then retract existing fact = %+v, %v", removed, err)
	}

	if _, replaceSeedErr := db.Transact(ctx, E{"id": "replace-ordered", "item/value": int64(1)}); replaceSeedErr != nil {
		t.Fatal(replaceSeedErr)
	}
	replaced, err := db.Transact(ctx, []any{
		[]any{"retract", "replace-ordered", "item/value"},
		[]any{"assert", "replace-ordered", "item/value", int64(2)},
	})
	if err != nil || len(replaced.Retracted) != 1 || len(replaced.Asserted) != 2 {
		t.Fatalf("retract then replacement = %+v, %v", replaced, err)
	}
	entity, err := db.Entity(ctx, "replace-ordered")
	if err != nil || entity["item/value"] != int64(2) {
		t.Fatalf("replacement entity = %#v, %v", entity, err)
	}

	if _, err = db.Declare(ctx, "identity/email", Type("text"), Unique()); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Transact(ctx, E{"id": "owner-a", "identity/email": "move@example.test"}); err != nil {
		t.Fatal(err)
	}
	moved, err := db.Transact(ctx, []any{
		[]any{"retract", "owner-a", "identity/email", "move@example.test"},
		[]any{"assert", "owner-b", "identity/email", "move@example.test"},
	})
	if err != nil || len(factsForAttribute(moved.Asserted, "identity/email")) != 1 || len(moved.Retracted) != 1 {
		t.Fatalf("ordered unique transfer = %+v, %v", moved, err)
	}
	if _, err := db.Transact(ctx, []any{
		[]any{"assert", "owner-c", "identity/email", "move@example.test"},
		[]any{"retract", "owner-b", "identity/email", "move@example.test"},
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("reversed unique transfer error = %v", err)
	}
	if _, err := db.Entity(ctx, "owner-c"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("reversed unique transfer left partial owner-c: %v", err)
	}
}

func TestLookupNestedRefAndOperationForms(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "person/email", Type("text"), Unique()); err != nil {
		t.Fatal(err)
	}
	first, firstErr := db.Transact(ctx, E{"id": []any{"person/email", "a@example.test"}, "person/email": "a@example.test", "person/name": "Ada"})
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	owner := factsForAttribute(first.Asserted, "person/email")[0].E
	second, secondErr := db.Transact(ctx, E{"id": []any{"person/email", "a@example.test"}, "person/city": "Lyon"})
	if secondErr != nil || factsForAttribute(second.Asserted, "person/city")[0].E != owner {
		t.Fatalf("lookup write = %+v, %v", second, secondErr)
	}
	if entity, err := db.Entity(ctx, []any{"person/email", "a@example.test"}); err != nil || entity["person/name"] != "Ada" {
		t.Fatalf("lookup entity = %#v, %v", entity, err)
	}
	for _, lookup := range []any{
		[]any{"missing/attr", "x"}, []any{"person/email"}, []any{1, "x"},
	} {
		if _, err := db.Transact(ctx, E{"id": lookup, "person/name": "bad"}); err == nil {
			t.Errorf("invalid lookup %#v accepted", lookup)
		}
	}
	if _, err := db.Transact(ctx, E{"id": "nonunique", "person/code": "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": []any{"person/code", "x"}, "person/name": "bad"}); !errors.Is(err, ErrSchema) {
		t.Fatalf("non-unique lookup error = %v", err)
	}

	if _, err := db.Declare(ctx, "node/child", Ref(), Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{
		"id": "parent", "node/child": []any{E{"id": "child", "node/name": "Child"}},
	}); err != nil {
		t.Fatal(err)
	}
	parent, parentErr := db.Entity(ctx, "parent")
	children, childrenOK := parent["node/child"].([]any)
	if parentErr != nil || !childrenOK || len(children) != 1 {
		t.Fatalf("nested ref = %#v, %v", parent, parentErr)
	}
	if _, err := db.Transact(ctx, []any{
		[]any{"assert", Tmp("t"), "node/name", "Temp"},
		[]any{"assert", Tmp("t"), "node/child", RefTo("child")},
		[]any{"assert", "holder", "node/child", RefTo(Tmp("t"))},
	}); err != nil {
		t.Fatal(err)
	}
	if report, err := db.Transact(ctx, []any{"retract", Tmp("unknown")}); err != nil || report.Tx != 0 {
		t.Fatalf("unknown temp retract = %+v, %v", report, err)
	}
	if _, err := db.Transact(ctx, []any{"assert", float64(10_000), "item/value", "float entity"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown float entity error = %v", err)
	}
	adaID, ok := owner.(int64)
	if !ok {
		t.Fatalf("lookup owner has type %T, want anonymous int64", owner)
	}
	if _, err := db.Transact(ctx, E{"id": int(adaID), "person/age": 37, "person/mentor": RefTo(int(adaID))}); err != nil {
		t.Fatalf("native int id/reference = %v", err)
	}
}

func TestTransactionDiffSchemaAndDeclarationValidation(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Transact(ctx, []any{
		[]any{"assert", "duplicate", "item/value", 1},
		[]any{"assert", "duplicate", "item/value", 1},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		[]any{"assert", "conflict", "item/value", 1},
		[]any{"assert", "conflict", "item/value", 2},
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("same transaction cardinality error = %v", err)
	}

	for _, typeName := range []string{"", "text_ref", "bytes_ref", "bogus"} {
		if _, err := db.Declare(ctx, "bad/type", Type(typeName)); !errors.Is(err, ErrSchema) {
			t.Errorf("Declare type %q error = %v", typeName, err)
		}
	}
	if _, err := db.Declare(ctx, "bad/dims", Dims(0)); !errors.Is(err, ErrSchema) {
		t.Fatalf("invalid dims error = %v", err)
	}
	for _, typeName := range []string{"json", "vector"} {
		if _, err := db.Declare(ctx, "bad/unique-"+typeName, Type(typeName), Unique()); !errors.Is(err, ErrSchema) {
			t.Errorf("unique %s error = %v", typeName, err)
		}
	}
	if _, err := db.Declare(ctx, "bad/untyped-unique", Unique()); !errors.Is(err, ErrSchema) {
		t.Fatalf("untyped unique error = %v", err)
	}

	if _, err := db.Transact(ctx, []any{
		E{"id": "dup-a", "dup/value": "same"}, E{"id": "dup-b", "dup/value": "same"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "dup/value", Type("text"), Unique()); !errors.Is(err, ErrSchema) {
		t.Fatalf("duplicate unique declaration error = %v", err)
	}
	if _, err := db.Declare(ctx, "many/value", Type("text"), Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "multi", "many/value": []any{"a", "b"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "many/value", Many(false)); !errors.Is(err, ErrSchema) {
		t.Fatalf("disable many with data error = %v", err)
	}
	if _, err := db.Retract(ctx, "multi", "many/value", "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "many/value", Many(false)); err != nil {
		t.Fatalf("disable many = %v", err)
	}
	if _, err := db.Declare(ctx, "unique/value", Type("text"), Unique()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "unique/value", Unique(false)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "typed", "typed/value": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "typed/value", Type("text")); !errors.Is(err, ErrSchema) {
		t.Fatalf("conflicting type declaration error = %v", err)
	}
	if _, err := db.Declare(ctx, "typed/value", NoHistory(false), Doc("number")); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Declare(ctx, "vec/value", Type("vector"), Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		[]any{"assert", "vectors", "vec/value", Vector([]float32{1})},
		[]any{"assert", "vectors", "vec/value", Vector([]float32{1, 2})},
	}); !errors.Is(err, ErrType) {
		t.Fatalf("mixed vector dimensions error = %v", err)
	}
}

func TestTransactionMetadataAndTimestampFailures(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	seed := E{"id": "seed", "item/value": 1}
	if _, err := db.Transact(ctx, seed); err != nil {
		t.Fatal(err)
	}
	for _, option := range []TxOption{
		WithBy(strings.Repeat("x", MaxValueBytes+1)),
		WithSource(strings.Repeat("x", MaxValueBytes+1)),
		WithMeta(math.NaN()),
		WithTxFacts(true),
		WithTxFacts([]any{true}),
		WithTxFacts([]any{[]any{"audit/kind"}}),
		WithTxFacts([]any{[]any{1, "x", "text"}}),
		WithTxFacts([]any{[]any{"audit/kind", "x", 1}}),
		WithTxFacts([]any{[]any{"audit/kind", "x", "bogus"}}),
	} {
		if _, err := db.Transact(ctx, seed, option); err == nil {
			t.Errorf("invalid transaction option unexpectedly succeeded")
		}
	}
	for _, attr := range []string{"fgraph/at", "fgraph/by", "fgraph/source", "fgraph/meta"} {
		if _, err := db.Transact(ctx, seed, WithTxFacts(E{attr: "forged"})); !errors.Is(err, ErrSchema) {
			t.Errorf("reserved tx fact %q error = %v", attr, err)
		}
		if _, err := db.Transact(ctx, seed, WithTxFacts([]any{[]any{attr, "forged", "text"}})); !errors.Is(err, ErrSchema) {
			t.Errorf("reserved tuple tx fact %q error = %v", attr, err)
		}
	}
	if _, err := db.Transact(ctx, seed, WithTxFacts(E{"id": "forged"})); !errors.Is(err, ErrSchema) {
		t.Fatalf("tx metadata id error = %v", err)
	}
	valid, validErr := db.Transact(ctx, seed, WithTxFacts([]any{
		[]any{"audit/kind", "tuple", "text"},
	}), WithMeta(map[string]any{"ok": true}))
	if validErr != nil || valid.Tx == 0 {
		t.Fatalf("valid tuple tx facts = %+v, %v", valid, validErr)
	}
	if _, err := db.Declare(ctx, "audit/count", Type("int")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, seed, WithTxFacts([]any{[]any{"audit/count", "wrong", "text"}})); !errors.Is(err, ErrType) {
		t.Fatalf("typed tuple tx fact error = %v", err)
	}
	if _, err := db.Transact(ctx, seed, WithTxFacts([]any{[]any{"INVALID", "wrong", "text"}})); !errors.Is(err, ErrSchema) {
		t.Fatalf("invalid tuple tx attribute error = %v", err)
	}
	if _, err := db.Transact(ctx, seed, WithTxFacts(E{"audit/count": "wrong"})); !errors.Is(err, ErrType) {
		t.Fatalf("typed tx fact error = %v", err)
	}
	for _, values := range [][]any{{}, {int64(1)}, {int64(1), int64(2)}} {
		if _, err := db.Transact(ctx, seed, WithTxFacts(E{"audit/count": values})); !errors.Is(err, ErrConflict) {
			t.Fatalf("cardinality-one tx array %#v error = %v", values, err)
		}
	}
	if _, err := db.Declare(ctx, "audit/tag", Type("text"), Many()); err != nil {
		t.Fatal(err)
	}
	manyReport, manyErr := db.Transact(ctx, seed, WithTxFacts(E{"audit/tag": []any{"a", "b"}}))
	if manyErr != nil {
		t.Fatalf("many tx facts = %v", manyErr)
	}
	if len(manyReport.Asserted) != 3 || manyReport.Asserted[1].V != "a" || manyReport.Asserted[2].V != "b" {
		t.Fatalf("many tx fact assertions = %+v", manyReport.Asserted)
	}
	jsonReport, jsonErr := db.Transact(ctx, seed, WithTxFacts(E{"audit/json": JSON([]any{int64(1), int64(2)})}))
	if jsonErr != nil || len(jsonReport.Asserted) != 2 {
		t.Fatalf("literal JSON tx array = %+v, %v", jsonReport, jsonErr)
	}
	vectorReport, vectorErr := db.Transact(ctx, seed, WithTxFacts(E{"audit/vector": Vector([]float32{1, 2})}))
	if vectorErr != nil {
		t.Fatalf("first vector tx fact = %v", vectorErr)
	}
	if len(vectorReport.Asserted) != 4 || vectorReport.Asserted[1].A != "fgraph/type" || vectorReport.Asserted[2].A != "fgraph/dims" || vectorReport.Asserted[3].A != "audit/vector" {
		t.Fatalf("vector tx fact assertions = %+v", vectorReport.Asserted)
	}
	if _, err := db.Transact(ctx, seed, WithTxFacts(E{"audit/vector": Vector([]float32{1})})); !errors.Is(err, ErrType) {
		t.Fatalf("wrong-dimension tx fact error = %v", err)
	}
	tupleVector, tupleVectorErr := db.Transact(ctx, seed, WithTxFacts([]any{[]any{"audit/tuple-vector", []any{float64(1), float64(2)}, "vector"}}))
	if tupleVectorErr != nil || len(tupleVector.Asserted) != 4 || tupleVector.Asserted[1].A != "fgraph/type" || tupleVector.Asserted[2].A != "fgraph/dims" || tupleVector.Asserted[3].A != "audit/tuple-vector" {
		t.Fatalf("tuple vector tx fact = %+v, %v", tupleVector, tupleVectorErr)
	}
	if _, err := db.Declare(ctx, "audit/unique", Type("text"), Unique()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "unique-owner", "audit/unique": "occupied"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, seed, WithTxFacts([]any{[]any{"audit/unique", "occupied", "text"}})); !errors.Is(err, ErrConflict) {
		t.Fatalf("unique tuple tx fact error = %v", err)
	}
	emptyTx, emptyTxErr := db.Transact(ctx, seed, WithTxFacts(E{}))
	if emptyTxErr != nil || emptyTx.Tx != 0 {
		t.Fatalf("empty tx map = %+v, %v", emptyTx, emptyTxErr)
	}
	emptyMany, emptyManyErr := db.Transact(ctx, seed, WithTxFacts(E{"audit/tag": []any{}}))
	if emptyManyErr != nil || emptyMany.Tx != 0 {
		t.Fatalf("empty many tx map = %+v, %v", emptyMany, emptyManyErr)
	}

	maxClock, openErr := Open(":memory:", WithClock(func() int64 { return maxInstantMicros }))
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer closeTest(t, maxClock)
	if _, err := maxClock.Transact(ctx, E{"id": "overflow", "item/value": 1}); !errors.Is(err, ErrType) {
		t.Fatalf("timestamp overflow error = %v", err)
	}
}

func TestFailedMetadataDoesNotSampleStatefulClock(t *testing.T) {
	ctx := context.Background()
	const base = int64(1767225600000000)
	calls := int64(0)
	db, openErr := Open(":memory:", WithClock(func() int64 {
		value := base + calls*10_000_000
		calls++
		return value
	}))
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer closeTest(t, db)
	if calls != 1 {
		t.Fatalf("genesis sampled clock %d times", calls)
	}
	if _, err := db.Transact(ctx, E{"id": "bad-clock", "item/value": int64(1)}, WithBy(strings.Repeat("x", MaxValueBytes+1))); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized metadata error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("failed metadata sampled clock; calls=%d", calls)
	}
	report, transactErr := db.Transact(ctx, E{"id": "good-clock", "item/value": int64(1)})
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	if calls != 2 || report.At != base+10_000_000 {
		t.Fatalf("successful timestamp=%d clock calls=%d", report.At, calls)
	}
}

func TestWholeEntityRetractionPreservesProtectedInboundProvenance(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "audit/subject", Ref()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Declare(ctx, "item/subject", Ref()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "subject", "item/value": int64(1)}); err != nil {
		t.Fatal(err)
	}
	audit, auditErr := db.Transact(ctx, E{}, WithTxFacts(E{"audit/subject": RefTo("subject")}))
	if auditErr != nil {
		t.Fatal(auditErr)
	}
	if _, err := db.Transact(ctx, E{"id": "application-owner", "item/subject": RefTo("subject")}); err != nil {
		t.Fatal(err)
	}
	retracted, retractErr := db.Transact(ctx, []any{"retract", "subject"})
	if retractErr != nil {
		t.Fatal(retractErr)
	}
	for _, fact := range retracted.Retracted {
		if fact.E == audit.Tx && fact.A == "audit/subject" {
			t.Fatalf("whole-entity retract removed protected provenance: %+v", fact)
		}
	}
	transaction, transactionErr := db.Entity(ctx, audit.Tx)
	if transactionErr != nil || transaction["audit/subject"] == nil {
		t.Fatalf("protected transaction after retract = %#v, %v", transaction, transactionErr)
	}
	application, applicationErr := db.Entity(ctx, "application-owner")
	if applicationErr != nil {
		t.Fatal(applicationErr)
	}
	if _, exists := application["item/subject"]; exists {
		t.Fatalf("application inbound reference survived retract: %#v", application)
	}
	if _, err := db.Excise(ctx, "subject"); err != nil {
		t.Fatal(err)
	}
	transaction, transactionErr = db.Entity(ctx, audit.Tx)
	if transactionErr != nil {
		t.Fatal(transactionErr)
	}
	if _, exists := transaction["audit/subject"]; exists {
		t.Fatalf("explicit excise preserved inbound provenance: %#v", transaction)
	}
}

func TestTransactionPrimitiveHelpers(t *testing.T) {
	if asInt64(int(2)) != 2 || asInt64(true) != 1 || asInt64(false) != 0 || asInt64("x") != 0 {
		t.Fatal("asInt64 matrix mismatch")
	}
	if !storageEqual([]byte{1}, []byte{1}) || storageEqual([]byte{1}, "x") || storageEqual("x", []byte{1}) {
		t.Fatal("byte storage equality mismatch")
	}
	if isValueWrapper("unknown") || !isValueWrapper("json") {
		t.Fatal("value wrapper classification mismatch")
	}
	attr := int64(71)
	ref := storedValue{logical: int64(80), storage: int64(80), tag: TagRef}
	assertion := plannedFact{e: 70, a: attr, value: ref}
	if !retractMatchesPlanned(retractRequest{e: 70}, assertion) || !retractMatchesPlanned(retractRequest{e: 80}, assertion) {
		t.Fatal("whole-entity planned retraction did not match own/inbound fact")
	}
	if !retractMatchesPlanned(retractRequest{e: 70, a: &attr, value: &ref}, assertion) || retractMatchesPlanned(retractRequest{missing: true}, assertion) {
		t.Fatal("exact/missing planned retraction classification mismatch")
	}
	raw := rawFact{e: 70, a: attr, v: int64(80), t: TagRef}
	if !rawRetracted([]retractRequest{{e: 80}}, raw) || !rawRetracted([]retractRequest{{e: 70, a: &attr, value: &ref}}, raw) || rawRetracted([]retractRequest{{missing: true}}, raw) {
		t.Fatal("stored retraction classification mismatch")
	}
	wire, err := MarshalWire([]any{RefTo("x"), Instant(1), Bytes([]byte{1}), Vector([]float32{1}), JSON(1), Tmp("t"), 1})
	if err != nil || !bytes.Contains(wire, []byte(`"tmp":"t"`)) {
		t.Fatalf("wire matrix = %s, %v", wire, err)
	}
	vectorWire, err := MarshalWire(Vector([]float32{0.1, -0.2}))
	if err != nil || string(vectorWire) != `{"vector":[0.10000000149011612,-0.20000000298023224]}` {
		t.Fatalf("exact vector wire = %s, %v", vectorWire, err)
	}
}

func TestRawSchemaFactsUseDeclarationValidation(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")

	for _, input := range []E{
		{"id": "not-an-attribute", "fgraph/many": true},
		{"id": "user/bad-type", "fgraph/type": "bogus"},
		{"id": "user/bad-dims", "fgraph/dims": int64(0)},
	} {
		if _, err := db.Transact(ctx, input); !errors.Is(err, ErrSchema) {
			t.Errorf("invalid raw declaration %#v error = %v", input, err)
		}
	}
	if _, err := db.Transact(ctx, E{"id": "user/email", "fgraph/unique": true}); !errors.Is(err, ErrSchema) {
		t.Fatalf("untyped raw unique declaration error = %v", err)
	}
	if _, err := db.Transact(ctx, E{
		"id": "user/email", "fgraph/type": "text", "fgraph/unique": true,
	}); err != nil {
		t.Fatalf("typed raw unique declaration = %v", err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "duplicate-a", "user/name": "same"},
		E{"id": "duplicate-b", "user/name": "same"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "user/name", "fgraph/type": "text"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "user/name", "fgraph/unique": true}); !errors.Is(err, ErrSchema) {
		t.Fatalf("raw unique over duplicates error = %v", err)
	}

	if _, err := db.Transact(ctx, E{"id": "user/tag", "fgraph/type": "text", "fgraph/many": true}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "tagged", "user/tag": []any{"a", "b"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "user/tag", "fgraph/many": false}); !errors.Is(err, ErrSchema) {
		t.Fatalf("raw many disable over multiple values error = %v", err)
	}

	if _, err := db.Transact(ctx, E{"id": "typed", "user/age": 42}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "user/age", "fgraph/type": "text"}); !errors.Is(err, ErrSchema) {
		t.Fatalf("raw incompatible type declaration error = %v", err)
	}

	if _, err := db.Transact(ctx, []any{
		E{"id": "pending-type", "pending/value": "text"},
		E{"id": "pending/value", "fgraph/type": "int"},
	}); !errors.Is(err, ErrSchema) {
		t.Fatalf("later raw type over pending value error = %v", err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "pending-unique-a", "pending/unique": "same"},
		E{"id": "pending-unique-b", "pending/unique": "same"},
		E{"id": "pending/unique", "fgraph/type": "text", "fgraph/unique": true},
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("later raw unique over pending duplicates error = %v", err)
	}
	if _, err := db.Declare(ctx, "pending/many", Type("text"), Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		E{"id": "pending-many", "pending/many": []any{"a", "b"}},
		E{"id": "pending/many", "fgraph/many": false},
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("later many disable over pending values error = %v", err)
	}
	if _, err := db.Transact(ctx, E{"id": "pending-vector", "pending/vector": Vector([]float32{1, 2})}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "pending/vector", "fgraph/dims": int64(3)}); !errors.Is(err, ErrSchema) {
		t.Fatalf("raw dimensions over existing vector error = %v", err)
	}
	if _, err := db.Declare(ctx, "overlay/value", Type("int")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		[]any{"retract", "overlay/value", "fgraph/type"},
		E{"id": "overlay-entity", "overlay/value": "now untyped"},
	}); err != nil {
		t.Fatalf("schema retraction did not apply to later item = %v", err)
	}
	if entity, err := db.Entity(ctx, "overlay-entity"); err != nil || entity["overlay/value"] != "now untyped" {
		t.Fatalf("value after same-tx schema retraction = %#v, %v", entity, err)
	}
}
