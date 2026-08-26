package fgraph

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
)

const (
	selectorInt64Min = -1 << 63
	selectorInt64Max = 1<<63 - 1
)

func TestHistoricalSelectorBounds(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	report, err := db.Transact(ctx, E{"id": "selector/subject", "item/value": "present"})
	if err != nil {
		t.Fatal(err)
	}
	if report.At <= report.Tx {
		t.Fatalf("test needs an instant above the latest transaction id: %#v", report)
	}
	view, err := db.At(ctx, report.At)
	if err != nil {
		t.Fatal(err)
	}
	if entity, entityErr := view.Entity(ctx, "selector/subject"); entityErr != nil || entity["item/value"] != "present" {
		t.Fatalf("integer instant view = %#v, %v", entity, entityErr)
	}

	for _, selector := range []any{
		int64(selectorInt64Min),
		int64(selectorInt64Max),
		json.Number("-9223372036854775809"),
		json.Number("9223372036854775808"),
	} {
		if _, selectorErr := db.At(ctx, selector); !errors.Is(selectorErr, ErrType) {
			t.Errorf("At(%v) error = %v, want TypeError", selector, selectorErr)
		}
	}
}

func TestCLIHistoricalSelectorBounds(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	path := t.TempDir() + "/selector.db"
	if _, err := runCLIForTest(t, "", "add", `{"id":"selector/subject","item/value":"present"}`, "--db", path); err != nil {
		t.Fatal(err)
	}
	query := `{"find":["?e"],"where":[["?e","item/value","present"]]}`
	selectors := []string{
		strconv.FormatInt(selectorInt64Min, 10),
		strconv.FormatInt(selectorInt64Max, 10),
		"-9223372036854775809",
		"9223372036854775808",
	}
	for _, command := range [][]string{{"get", "selector/subject"}, {"q", query}} {
		for _, selector := range selectors {
			args := append(append([]string{}, command...), "--at", selector, "--db", path)
			if _, err := runCLIForTest(t, "", args...); !errors.Is(err, ErrType) {
				t.Errorf("%v error = %v, want TypeError", args, err)
			}
		}
	}
}
