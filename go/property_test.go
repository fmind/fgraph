package fgraph

import (
	"context"
	"math/rand"
	"slices"
	"testing"
)

type referenceFact struct {
	rx    *int64
	value string
	id    int64
	tx    int64
}

func TestTemporalModelFixedSeed(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	subject, err := db.Transact(ctx, E{"id": "subject"})
	if err != nil {
		t.Fatal(err)
	}

	// #nosec G404 -- deterministic pseudo-randomness is the point of this fixed-seed model test.
	random := rand.New(rand.NewSource(20260824))
	values := []string{"red", "green", "blue"}
	live := ""
	facts := []referenceFact{}
	txs := []int64{subject.Tx}
	snapshots := map[int64]string{subject.Tx: ""}

	for step := 0; step < 120; step++ {
		op := random.Intn(4)
		if step == 0 {
			op = 3 // Guarantee one validated schema transition; later draws prove idempotence.
		}
		value := values[random.Intn(len(values))]
		before := live
		var report TxReport
		var err error
		switch op {
		case 0:
			report, err = db.Transact(ctx, E{"id": "subject", "model/value": value})
		case 1:
			report, err = db.Retract(ctx, "subject", "model/value", value)
		case 2:
			report, err = db.Retract(ctx, "subject", "model/value")
		case 3:
			report, err = db.Declare(ctx, "model/value", Type("text"), Doc("fixed-seed model value"))
		}
		if err != nil {
			t.Fatalf("step %d op %d: %v", step, op, err)
		}
		changed := (op == 0 && before != value) || (op == 1 && before == value) || (op == 2 && before != "")
		if op == 3 {
			if report.Tx != 0 {
				txs = append(txs, report.Tx)
				snapshots[report.Tx] = live
			}
			changed = false
		}
		if !changed {
			if report.Tx != 0 && op != 3 {
				t.Fatalf("step %d allocated no-op transaction %d", step, report.Tx)
			}
		} else if op != 3 {
			if report.Tx == 0 {
				t.Fatalf("step %d omitted changing transaction", step)
			}
			if before != "" {
				index := len(facts) - 1
				rx := report.Tx
				facts[index].rx = &rx
				got := factsForAttribute(report.Retracted, "model/value")
				if len(got) != 1 || got[0].ID != facts[index].id {
					t.Fatalf("step %d retracted = %#v, want fact %d", step, got, facts[index].id)
				}
			}
			if op == 0 {
				got := factsForAttribute(report.Asserted, "model/value")
				if len(got) != 1 {
					t.Fatalf("step %d asserted = %#v", step, got)
				}
				facts = append(facts, referenceFact{id: got[0].ID, value: value, tx: report.Tx})
				live = value
			} else {
				live = ""
			}
			txs = append(txs, report.Tx)
			snapshots[report.Tx] = live
		}

		assertReferenceCurrent(t, ctx, db, live)
		assertReferenceHistory(t, ctx, db, facts)
		pastIndex := random.Intn(len(txs))
		pastTx := txs[pastIndex]
		assertReferenceCurrent(t, ctx, db.atTx(pastTx), snapshots[pastTx])
		if report.Tx != 0 {
			instantView, instantErr := db.AtInstant(ctx, report.At)
			if instantErr != nil {
				t.Fatalf("step %d at instant: %v", step, instantErr)
			}
			assertReferenceCurrent(t, ctx, instantView, live)
		}

		leftIndex := random.Intn(len(txs))
		rightIndex := leftIndex + random.Intn(len(txs)-leftIndex)
		assertReferenceDiff(t, ctx, db, txs[leftIndex], txs[rightIndex], facts)
	}

	// Random sampling above catches step-local defects; this exhaustive bounded
	// pass proves every retained snapshot and every adjacent temporal delta.
	for _, tx := range txs {
		assertReferenceCurrent(t, ctx, db.atTx(tx), snapshots[tx])
	}
	assertReferenceHistory(t, ctx, db, facts)
	for index := 1; index < len(txs); index++ {
		assertReferenceDiff(t, ctx, db, txs[index-1], txs[index], facts)
	}
}

func factsForAttribute(facts []Fact, attribute string) []Fact {
	result := []Fact{}
	for _, fact := range facts {
		if fact.A == attribute {
			result = append(result, fact)
		}
	}
	return result
}

func assertReferenceCurrent(t *testing.T, ctx context.Context, db *DB, want string) {
	t.Helper()
	entity, err := db.Entity(ctx, "subject")
	if err != nil {
		t.Fatal(err)
	}
	got, present := entity["model/value"]
	if want == "" {
		if present {
			t.Fatalf("current model/value = %#v, want absent", got)
		}
		return
	}
	if !present || got != want {
		t.Fatalf("current model/value = %#v (present %t), want %q", got, present, want)
	}
}

func assertReferenceHistory(t *testing.T, ctx context.Context, db *DB, want []referenceFact) {
	t.Helper()
	got, err := db.History(ctx, "subject", "model/value")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("history length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].id || got[i].V != want[i].value || got[i].Tx != want[i].tx || !equalOptionalInt(got[i].Rx, want[i].rx) {
			t.Fatalf("history[%d] = %#v, want %#v", i, got[i], want[i])
		}
	}
}

func assertReferenceDiff(t *testing.T, ctx context.Context, db *DB, from, to int64, model []referenceFact) {
	t.Helper()
	diff, err := db.Diff(ctx, from, to)
	if err != nil {
		t.Fatal(err)
	}
	wantAsserted, wantRetracted := []int64{}, []int64{}
	for _, fact := range model {
		if fact.tx > from && fact.tx <= to {
			wantAsserted = append(wantAsserted, fact.id)
		}
		if fact.rx != nil && *fact.rx > from && *fact.rx <= to {
			wantRetracted = append(wantRetracted, fact.id)
		}
	}
	gotAsserted := make([]int64, 0, len(model))
	gotRetracted := make([]int64, 0, len(model))
	for _, fact := range factsForAttribute(diff.Asserted, "model/value") {
		gotAsserted = append(gotAsserted, fact.ID)
	}
	for _, fact := range factsForAttribute(diff.Retracted, "model/value") {
		gotRetracted = append(gotRetracted, fact.ID)
	}
	if !slices.Equal(gotAsserted, wantAsserted) || !slices.Equal(gotRetracted, wantRetracted) {
		t.Fatalf("diff %d..%d = asserted %v retracted %v, want %v %v", from, to, gotAsserted, gotRetracted, wantAsserted, wantRetracted)
	}
}

func equalOptionalInt(left, right *int64) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}
