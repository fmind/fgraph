package fgraph

import (
	"context"
	"errors"
	"testing"
)

func TestCASMissingDesiredDeletesAndPreservesIsolation(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.Declare(ctx, "cas/optional", Type("text")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "cas/item", "cas/optional": "present"}); err != nil {
		t.Fatal(err)
	}
	missing := E{"missing": true}

	deleted, deleteErr := db.Transact(ctx, []any{"cas", "cas/item", "cas/optional", "present", missing})
	if deleteErr != nil {
		t.Fatalf("CAS delete: %v", deleteErr)
	}
	for _, fact := range deleted.Asserted {
		if fact.A == "cas/optional" {
			t.Fatalf("CAS delete reasserted its target: %#v", deleted)
		}
	}
	if len(deleted.Retracted) != 1 || deleted.Retracted[0].A != "cas/optional" || deleted.Retracted[0].V != "present" {
		t.Fatalf("CAS delete report = %#v", deleted)
	}
	entity, entityErr := db.Entity(ctx, "cas/item")
	if entityErr != nil {
		t.Fatal(entityErr)
	}
	if _, exists := entity["cas/optional"]; exists {
		t.Fatalf("CAS delete left value: %#v", entity)
	}

	created, createErr := db.Transact(ctx, []any{"cas", "cas/item", "cas/optional", missing, "again"})
	createdTarget := false
	for _, fact := range created.Asserted {
		createdTarget = createdTarget || (fact.A == "cas/optional" && fact.V == "again")
	}
	if createErr != nil || !createdTarget || len(created.Retracted) != 0 {
		t.Fatalf("CAS create report = %#v, %v", created, createErr)
	}
	beforeFailure := created.Tx
	if _, err := db.Transact(ctx, []any{"cas", "cas/item", "cas/optional", "again", E{"missing": false}}); !errors.Is(err, ErrType) {
		t.Fatalf("false desired missing sentinel error = %v", err)
	}
	if basis, err := db.latestTx(ctx); err != nil || basis != beforeFailure {
		t.Fatalf("invalid desired sentinel changed basis to %d: %v", basis, err)
	}
	if _, err := db.Transact(ctx, []any{
		"cas", "cas/item", "cas/optional", "again", E{"missing": true, "extra": true},
	}); !errors.Is(err, ErrType) {
		t.Fatalf("non-exact desired missing sentinel error = %v", err)
	}

	for name, data := range map[string]any{
		"delete then assert": []any{
			[]any{"cas", "cas/item", "cas/optional", "again", missing},
			[]any{"assert", "cas/item", "cas/optional", "other"},
		},
		"duplicate cas": []any{
			[]any{"cas", "cas/item", "cas/optional", "again", "two"},
			[]any{"cas", "cas/item", "cas/optional", "again", "three"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.Transact(ctx, data); !errors.Is(err, ErrConflict) {
				t.Fatalf("CAS isolation error = %v", err)
			}
		})
	}
	entity, entityErr = db.Entity(ctx, "cas/item")
	if entityErr != nil || entity["cas/optional"] != "again" {
		t.Fatalf("failed isolated CAS changed entity: %#v, %v", entity, entityErr)
	}

	if _, err := db.Transact(ctx, []any{"cas", "cas/item", "cas/optional", "again", missing}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, []any{
		[]any{"cas", "cas/item", "cas/optional", missing, missing},
		[]any{"assert", "cas/item", "cas/optional", "unexpected"},
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("missing-to-missing CAS isolation error = %v", err)
	}
	entity, entityErr = db.Entity(ctx, "cas/item")
	if entityErr != nil {
		t.Fatal(entityErr)
	}
	if _, exists := entity["cas/optional"]; exists {
		t.Fatalf("failed missing-to-missing isolation created a value: %#v", entity)
	}
}
