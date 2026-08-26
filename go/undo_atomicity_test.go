package fgraph

import (
	"context"
	"path/filepath"
	"testing"
)

func TestUndoPlansAfterAcquiringWriterLock(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "undo-atomicity.db")
	db := fixedDB(t, path)
	target, err := db.Transact(ctx, E{"id": "undo-race", "item/value": "preserve"})
	if err != nil {
		t.Fatal(err)
	}

	concurrent := fixedDB(t, path)
	var concurrentErr error
	var reasserted TxReport
	optionCalls := 0
	injectWriter := TxOption(func(*txOptions) {
		optionCalls++
		if _, concurrentErr = concurrent.Transact(ctx, []any{"retract", "undo-race", "item/value", "preserve"}); concurrentErr != nil {
			return
		}
		reasserted, concurrentErr = concurrent.Transact(ctx, E{"id": "undo-race", "item/value": "preserve"})
	})

	undone, undoErr := db.Undo(ctx, target.Tx, injectWriter)
	if concurrentErr != nil {
		t.Fatalf("concurrent retract and reassert = %v", concurrentErr)
	}
	if undoErr != nil {
		t.Fatal(undoErr)
	}
	if optionCalls != 1 {
		t.Fatalf("stateful undo option calls = %d, want 1", optionCalls)
	}
	if reasserted.Tx <= target.Tx {
		t.Fatalf("reassertion tx = %d, want later than target %d", reasserted.Tx, target.Tx)
	}
	if undone.Status != "applied" || undone.Tx <= reasserted.Tx {
		t.Fatalf("undo report = %+v, want audited transaction after reassertion %d", undone, reasserted.Tx)
	}
	if len(undone.Retracted) != 0 {
		t.Fatalf("undo retracted later facts: %+v", undone.Retracted)
	}
	audit, auditErr := db.Entity(ctx, undone.Tx)
	undoRef, hasUndoRef := audit["fgraph/undoes"].(map[string]any)
	if auditErr != nil || !hasUndoRef || undoRef["ref"] != target.Tx {
		t.Fatalf("undo audit entity = %#v, %v", audit, auditErr)
	}
	entity, entityErr := db.Entity(ctx, "undo-race")
	if entityErr != nil {
		t.Fatal(entityErr)
	}
	if entity["item/value"] != "preserve" {
		t.Fatalf("entity after undo = %#v, want later reassertion to survive", entity)
	}
}
