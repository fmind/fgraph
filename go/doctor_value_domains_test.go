package fgraph

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

func doctorValueFactID(t *testing.T, report TxReport) int64 {
	t.Helper()
	for _, fact := range report.Asserted {
		if fact.A == "item/value" {
			return fact.ID
		}
	}
	t.Fatalf("transaction has no item/value fact: %+v", report)
	return 0
}

func TestDoctorRejectsUnknownPhysicalValueTag(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	report, err := db.Transact(ctx, E{"id": "subject", "item/value": int64(42)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.store.sql.ExecContext(ctx, "PRAGMA ignore_check_constraints=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err = db.store.sql.ExecContext(ctx, "UPDATE fgraph_facts SET t=99 WHERE id=?", doctorValueFactID(t, report)); err != nil {
		t.Fatal(err)
	}
	if _, err = db.store.sql.ExecContext(ctx, "PRAGMA ignore_check_constraints=OFF"); err != nil {
		t.Fatal(err)
	}

	checked, err := db.Doctor(ctx)
	if err != nil || checked.OK || !strings.Contains(strings.Join(checked.Problems, "\n"), "invalid value tags: 1") {
		t.Fatalf("Doctor() = %+v, %v", checked, err)
	}
	if _, entityErr := db.Entity(ctx, "subject"); !errors.Is(entityErr, ErrFormat) {
		t.Fatalf("corrupt entity error = %v, want FormatError", entityErr)
	}
	if _, err = db.Doctor(ctx, true); !errors.Is(err, ErrFormat) {
		t.Fatalf("repair error = %v, want FormatError", err)
	}
}

func TestDoctorRejectsRenamedSystemIdentityWithoutMutation(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_ids SET name='corrupt/at' WHERE id=1"); err != nil {
		t.Fatal(err)
	}

	checked, err := db.Doctor(ctx)
	if err != nil || checked.OK || !strings.Contains(strings.Join(checked.Problems, "\n"), "invalid system identities: 1") {
		t.Fatalf("Doctor() = %+v, %v", checked, err)
	}
	if _, err = db.Doctor(ctx, true); !errors.Is(err, ErrFormat) {
		t.Fatalf("repair error = %v, want FormatError", err)
	}
	var name string
	if err = db.store.sql.QueryRowContext(ctx, "SELECT name FROM fgraph_ids WHERE id=1").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "corrupt/at" {
		t.Fatalf("repair mutated system identity to %q", name)
	}
}

func TestDoctorRejectsMutatedGenesisFactWithoutMutation(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	if _, err := db.store.sql.ExecContext(ctx, "UPDATE fgraph_facts SET e=2 WHERE id=2"); err != nil {
		t.Fatal(err)
	}
	before := physicalState(t, db)

	checked, err := db.Doctor(ctx)
	if err != nil || checked.OK || !strings.Contains(strings.Join(checked.Problems, "\n"), "invalid genesis facts: 1") {
		t.Fatalf("Doctor() = %+v, %v", checked, err)
	}
	if _, err = db.Doctor(ctx, true); !errors.Is(err, ErrFormat) {
		t.Fatalf("repair error = %v, want FormatError", err)
	}
	if after := physicalState(t, db); !reflect.DeepEqual(after, before) {
		t.Fatalf("repair mutated genesis corruption: after=%#v before=%#v", after, before)
	}
}

func TestDoctorRejectsInvalidPhysicalValues(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		value any
		name  string
		query string
	}{
		{name: "ref storage", query: "UPDATE fgraph_facts SET t=0,v='65' WHERE id=?"},
		{name: "bool domain", query: "UPDATE fgraph_facts SET t=1,v=2 WHERE id=?"},
		{name: "non-finite float", query: "UPDATE fgraph_facts SET t=3,v=? WHERE id=?", value: math.Inf(1)},
		{name: "inline text bound", query: "UPDATE fgraph_facts SET t=4,v=? WHERE id=?", value: strings.Repeat("x", 257)},
		{name: "instant domain", query: "UPDATE fgraph_facts SET t=5,v=253402300800000000 WHERE id=?"},
		{name: "inline bytes bound", query: "UPDATE fgraph_facts SET t=6,v=? WHERE id=?", value: make([]byte, 257)},
		{name: "canonical JSON", query: "UPDATE fgraph_facts SET t=10,v=? WHERE id=?", value: `{"b":2, "a":1}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			report, err := db.Transact(ctx, E{"id": "subject", "item/value": int64(42)})
			if err != nil {
				t.Fatal(err)
			}
			factID := doctorValueFactID(t, report)
			if test.value == nil {
				_, err = db.store.sql.ExecContext(ctx, test.query, factID)
			} else {
				_, err = db.store.sql.ExecContext(ctx, test.query, test.value, factID)
			}
			if err != nil {
				t.Fatal(err)
			}

			checked, doctorErr := db.Doctor(ctx)
			if doctorErr != nil || checked.OK || !strings.Contains(strings.Join(checked.Problems, "\n"), "invalid physical values: 1") {
				t.Fatalf("Doctor() = %+v, %v", checked, doctorErr)
			}
			if _, entityErr := db.Entity(ctx, "subject"); !errors.Is(entityErr, ErrFormat) {
				t.Fatalf("corrupt entity error = %v, want FormatError", entityErr)
			}
			if _, repairErr := db.Doctor(ctx, true); !errors.Is(repairErr, ErrFormat) {
				t.Fatalf("repair error = %v, want FormatError", repairErr)
			}
		})
	}
}
