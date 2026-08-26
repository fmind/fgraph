package fgraph

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestCLIBranchAndFlagMatrix(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "branches.db")
	run := func(stdin string, args ...string) (string, error) {
		base := make([]string, 0, 3+len(args))
		base = append(base, "--db", path, "--json")
		return runCLIForTest(t, stdin, append(base, args...)...)
	}
	if _, err := run("", "init"); err != nil {
		t.Fatal(err)
	}
	if _, err := run("", "add", `{"id":"cli","item/value":1,"item/text":"hello"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := run("", "declare", "item/ref", "--ref"); err != nil {
		t.Fatal(err)
	}
	if _, err := run("", "add", `{"id":"other","item/text":"hello","item/ref":{"ref":"cli"}}`); err != nil {
		t.Fatal(err)
	}
	if _, err := run("", "declare", "item/vector", "--type", "vector", "--dims", "2"); err != nil {
		t.Fatal(err)
	}
	if _, err := run("", "add", `{"id":"embedded","item/vector":{"vector":[1,2]}}`); err != nil {
		t.Fatal(err)
	}

	usageCases := [][]string{
		{"add"},
		{"add", `{}`, `{}`},
		{"retract"},
		{"retract", "a", "b", "c", "d"},
		{"get"},
		{"get", "a", "b"},
		{"q"},
		{"q", `{}`, `{}`},
		{"history"},
		{"history", "a", "b", "c"},
		{"why"},
		{"why", "a", "b", "c"},
		{"diff", "64"},
		{"diff", "bad", "65"},
		{"diff", "64", "bad"},
		{"declare"},
		{"declare", "a/b", "extra"},
		{"undo"},
		{"undo", "bad"},
		{"backup"},
		{"backup", "a", "b"},
	}
	for _, args := range usageCases {
		if _, err := run("", args...); err == nil {
			t.Errorf("usage case %v succeeded", args)
		}
	}

	for _, args := range [][]string{
		{"get", "cli", "--depth", "-1"},
		{"q", `{`},
		{"q", `{"find":["?e"],"where":[["?e","item/value","_"]]}`, "--args", `[]`},
		{"q", `{"find":["?e"],"where":[["?e","item/value","_"]]}`, "--args", `{`},
		{"search", "--vector", `{}`},
		{"search", "hello", "--filter", `{`},
		{"search", "hello", "--filter", `{}`},
		{"search", "hello", "--embed-cmd", "  "},
	} {
		if _, err := run("", args...); err == nil {
			t.Errorf("typed failure case %v succeeded", args)
		}
	}

	if _, err := run("", "q", `{"find":["?e"],"where":[["?e","item/value","?v"],[">=","?v","?min"]],"in":["?min"]}`, "--args", `{"?min":1}`); err != nil {
		t.Fatal(err)
	}
	if _, err := run("", "search", "hello", "--filter", `["item/value",1]`); err != nil {
		t.Fatal(err)
	}
	if _, err := run("", "search", "--text", "hello", "--vector", `[1,2]`, "--vector-attribute", "item/vector", "--k", "1", "--expand", "1"); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"retract", "cli"},
		{"retract", "other", "item/text"},
		{"retract", "other", "item/ref", `{"ref":"cli"}`},
	} {
		if _, err := run("", args...); err != nil {
			t.Errorf("valid retract %v = %v", args, err)
		}
	}
	for _, args := range [][]string{
		{"declare", "flags/all", "--type", "text", "--many=false", "--unique=false", "--nohistory=false", "--doc", ""},
		{"declare", "flags/vector", "--type", "vector", "--dims", "2"},
		{"declare", "flags/ref", "--type", "text", "--ref"},
		{"declare", "flags/inverse", "--type", "text", "--many", "--unique", "--nohistory"},
		{"declare", "flags/inverse", "--one", "--not-unique", "--history"},
	} {
		if _, err := run("", args...); err != nil {
			t.Errorf("valid declare %v = %v", args, err)
		}
	}
	flagsDB, err := Open(path, WithReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	flags, err := flagsDB.Entity(ctx, "flags/inverse")
	closeTest(t, flagsDB)
	if err != nil || flags["fgraph/many"] != false || flags["fgraph/unique"] != false || flags["fgraph/nohistory"] != false {
		t.Fatalf("inverse declaration flags = %#v, %v", flags, err)
	}
	if output, err := run("", "tail", "--since", strconv.FormatInt(mathMaxInt64(), 10)); err != nil || output != "" {
		t.Fatalf("empty tail = %q, %v", output, err)
	}

	var stdout bytes.Buffer
	if err := RunCLI(ctx, []string{"fgraph", "--db", path, "--json", "info"}, strings.NewReader(""), errorWriter{}, &stdout); !errors.Is(err, ErrFormat) {
		t.Fatalf("CLI output writer error = %v", err)
	}
	if err := RunCLI(ctx, []string{"fgraph", "--db", path, "add", "-"}, errorReader{}, &stdout, &stdout); !errors.Is(err, ErrFormat) {
		t.Fatalf("CLI stdin reader error = %v", err)
	}
	if _, err := run("{\"id\":\"first\"}\n{\n", "add", "-"); err == nil {
		t.Fatal("malformed NDJSON add succeeded")
	}
	argumentFile := filepath.Join(dir, "argument.json")
	if err := os.WriteFile(argumentFile, []byte(`{"id":"from-file","file/value":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run("", "add", "@"+argumentFile); err != nil {
		t.Fatal(err)
	}
	if _, err := run("", "add", "@"+filepath.Join(dir, "missing.json")); !errors.Is(err, ErrFormat) {
		t.Fatalf("missing @file error = %v", err)
	}
}

func mathMaxInt64() int64 { return int64(^uint64(0) >> 1) }
