package fgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/urfave/cli/v3"
)

const embeddingHelperModeEnv = "FGRAPH_TEST_EMBED_HELPER_MODE"

func TestMain(m *testing.M) {
	mode := os.Getenv(embeddingHelperModeEnv)
	if mode == "" {
		os.Exit(m.Run())
	}

	exitCode := 0
	var output []byte
	switch mode {
	case "success":
		output = []byte("[1,0]\n")
	case "failure":
		exitCode = 7
	case "argument":
		if len(os.Args) != 2 {
			exitCode = 2
		} else {
			output = fmt.Appendf(nil, "[%s,0]\n", os.Args[1])
		}
	case "slow":
		time.Sleep(time.Second)
		output = []byte("[1,0]\n")
	case "descendant":
		executable, executableErr := os.Executable()
		if executableErr != nil {
			exitCode = 2
			break
		}
		// #nosec G204 -- this mode intentionally relaunches the current test binary.
		child := exec.Command(executable)
		child.Stdout = os.Stdout
		child.Env = os.Environ()
		for index, value := range child.Env {
			if strings.HasPrefix(value, embeddingHelperModeEnv+"=") {
				child.Env[index] = embeddingHelperModeEnv + "=pipe-holder"
			}
		}
		if err := child.Start(); err != nil {
			exitCode = 2
		} else {
			output = []byte("[1,0]\n")
		}
	case "pipe-holder":
		time.Sleep(time.Second)
	case "oversize":
		output = bytes.Repeat([]byte("0"), (1<<20)+1)
	default:
		exitCode = 2
	}
	if len(output) != 0 {
		if _, err := os.Stdout.Write(output); err != nil {
			exitCode = 2
		}
	}
	os.Exit(exitCode)
}

func embeddingHelperCommand(t *testing.T, mode string, args ...string) string {
	t.Helper()
	t.Setenv(embeddingHelperModeEnv, mode)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if len(args) == 0 {
		return executable
	}
	command, err := json.Marshal(append([]string{executable}, args...))
	if err != nil {
		t.Fatal(err)
	}
	return string(command)
}

func runCLIForTest(t *testing.T, stdin string, args ...string) (string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	full := append([]string{"fgraph"}, args...)
	err := RunCLI(context.Background(), full, strings.NewReader(stdin), &stdout, &stderr)
	return stdout.String(), err
}

func unsetFGraphDBForTest(t *testing.T) {
	t.Helper()
	const name = "FGRAPH_DB"
	previous, existed := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var err error
		if existed {
			err = os.Setenv(name, previous)
		} else {
			err = os.Unsetenv(name)
		}
		if err != nil {
			t.Errorf("restore %s: %v", name, err)
		}
	})
}

func TestCLIDefaultDatabasePath(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	unsetFGraphDBForTest(t)

	if _, err := runCLIForTest(t, "", "init"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat("facts.fgraph"); err != nil {
		t.Fatalf("default database was not created at facts.fgraph: %v", err)
	}
	if _, err := os.Stat("fgraph.db"); !os.IsNotExist(err) {
		t.Fatalf("legacy default database fgraph.db exists or cannot be inspected: %v", err)
	}

	environmentPath := filepath.Join(directory, "environment.fgraph")
	t.Setenv("FGRAPH_DB", environmentPath)
	if _, err := runCLIForTest(t, "", "init"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(environmentPath); err != nil {
		t.Fatalf("FGRAPH_DB database was not created: %v", err)
	}

	explicitPath := filepath.Join(directory, "explicit.fgraph")
	if _, err := runCLIForTest(t, "", "init", "--db", explicitPath); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(explicitPath); err != nil {
		t.Fatalf("explicit --db database was not created: %v", err)
	}
}

func TestCLILegacyImplicitDefaultRemainsCompatible(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	unsetFGraphDBForTest(t)

	if _, err := runCLIForTest(t, "", "init", "--db", "fgraph.db"); err != nil {
		t.Fatal(err)
	}
	if _, err := runCLIForTest(t, "", "init"); err != nil {
		t.Fatalf("implicit legacy default selection failed: %v", err)
	}
	if _, err := os.Lstat("facts.fgraph"); !os.IsNotExist(err) {
		t.Fatalf("implicit invocation created facts.fgraph or could not inspect it: %v", err)
	}
	t.Setenv("FGRAPH_DB", "")
	if _, err := runCLIForTest(t, "", "init"); !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "database path is empty") {
		t.Fatalf("empty FGRAPH_DB error = %v, want actionable FormatError", err)
	}
	if err := os.Unsetenv("FGRAPH_DB"); err != nil {
		t.Fatal(err)
	}
	if _, err := runCLIForTest(t, "", "init", "--definitely-invalid"); err == nil {
		t.Fatal("invalid syntax unexpectedly reached the migration guard")
	} else {
		var exit cli.ExitCoder
		if !errors.As(err, &exit) || exit.ExitCode() != 2 {
			t.Fatalf("invalid syntax error = %v, want usage exit 2", err)
		}
	}
	if err := os.WriteFile("facts.fgraph", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runCLIForTest(t, "", "init"); !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "not an initialized fgraph database") {
		t.Fatalf("unclaimed new default error = %v, want actionable FormatError", err)
	}
	if info, err := os.Stat("facts.fgraph"); err != nil || info.Size() != 0 {
		t.Fatalf("implicit invocation modified unclaimed facts.fgraph: info=%v err=%v", info, err)
	}
	if err := os.Remove("facts.fgraph"); err != nil {
		t.Fatal(err)
	}
	environmentPath := filepath.Join(directory, "environment.fgraph")
	t.Setenv("FGRAPH_DB", environmentPath)
	if _, err := runCLIForTest(t, "", "init"); err != nil {
		t.Fatalf("explicit FGRAPH_DB selection failed: %v", err)
	}
	if _, err := runCLIForTest(t, "", "init", "--db", "facts.fgraph"); err != nil {
		t.Fatalf("explicit new default path failed: %v", err)
	}
	if err := os.Unsetenv("FGRAPH_DB"); err != nil {
		t.Fatal(err)
	}
	if _, err := runCLIForTest(t, "", "init"); err != nil {
		t.Fatalf("implicit new default selection failed once both files existed: %v", err)
	}
}

func TestCLIRejectsInvalidArityBeforeOpeningDatabase(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	unsetFGraphDBForTest(t)
	if err := os.WriteFile(legacyDefaultDatabasePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	cases := [][]string{
		{"init", "extra"},
		{"info", "extra"},
		{"add"},
		{"retract"},
		{"get"},
		{"q"},
		{"explain"},
		{"datoms", "eavt", "extra"},
		{"history"},
		{"why"},
		{"tx"},
		{"diff", "64"},
		{"declare"},
		{"shape"},
		{"validate"},
		{"schema", "a", "b"},
		{"schema-export", "extra"},
		{"schema-check"},
		{"schema-apply"},
		{"apply", "a", "b"},
		{"snapshot", "extra"},
		{"restore", "a", "b"},
		{"undo"},
		{"excise", "--operation-id", "test", "--if-basis-tx", "64"},
		{"tail", "extra"},
		{"backup"},
		{"doctor", "extra"},
		{"mcp", "extra"},
		{"version", "extra"},
	}
	for _, args := range cases {
		_, err := runCLIForTest(t, "", args...)
		var exit cli.ExitCoder
		if !errors.As(err, &exit) || exit.ExitCode() != 2 {
			t.Errorf("%v error = %v, want usage exit 2 before database access", args, err)
		}
	}
	if info, err := os.Stat(legacyDefaultDatabasePath); err != nil || info.Size() != 0 {
		t.Fatalf("invalid usage modified the legacy path: info=%v err=%v", info, err)
	}
	if _, err := os.Lstat(defaultDatabasePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid usage created the new default path: %v", err)
	}
}

func TestCLIRejectsSemanticUsageBeforeOpeningDatabase(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "invalid transaction", args: []string{"tx", "not-an-int"}},
		{name: "invalid diff start", args: []string{"diff", "not-an-int", "64"}},
		{name: "invalid diff end", args: []string{"diff", "64", "not-an-int"}},
		{name: "invalid undo transaction", args: []string{"undo", "not-an-int"}},
		{name: "conflicting cardinality", args: []string{"declare", "person/name", "--many", "--one"}},
		{name: "conflicting uniqueness", args: []string{"declare", "person/name", "--unique", "--not-unique"}},
		{name: "conflicting history", args: []string{"declare", "person/name", "--nohistory", "--history"}},
		{name: "conflicting shape closure", args: []string{"shape", "person", "--closed", "--open"}},
		{name: "missing excise options", args: []string{"excise", "person"}},
		{name: "zero batch size", args: []string{"add", `{}`, "--batch-size", "0"}},
		{name: "oversized batch", args: []string{"add", `{}`, "--batch-size", "10001"}},
		{name: "conflicting operation ids", args: []string{"add", `{}`, "--operation-id", "one", "--operation-id-prefix", "many"}},
		{name: "prefix without batching", args: []string{"add", `{}`, "--operation-id-prefix", "many"}},
		{name: "conflicting MCP modes", args: []string{"mcp", "--write", "--read-only"}},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			t.Chdir(directory)
			unsetFGraphDBForTest(t)
			if err := os.WriteFile(legacyDefaultDatabasePath, nil, 0o600); err != nil {
				t.Fatal(err)
			}

			_, err := runCLIForTest(t, "", test.args...)
			var exit cli.ExitCoder
			if !errors.As(err, &exit) || exit.ExitCode() != 2 {
				t.Fatalf("%v error = %v, want usage exit 2 before database access", test.args, err)
			}
			contents, readErr := os.ReadFile(legacyDefaultDatabasePath)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(contents) != 0 {
				t.Fatalf("%v initialized the legacy database to %d bytes", test.args, len(contents))
			}
			if _, statErr := os.Lstat(defaultDatabasePath); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("%v created the new default path: %v", test.args, statErr)
			}
		})
	}
}

func TestCLIRejectsOutOfRangeTransactionsBeforeOpeningDatabase(t *testing.T) {
	cases := [][]string{
		{"tx", "9223372036854775808"},
		{"tx", "-9223372036854775809"},
		{"undo", "9223372036854775808"},
		{"diff", "9223372036854775808", "64"},
		{"diff", "64", "-9223372036854775809"},
	}

	for _, args := range cases {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			directory := t.TempDir()
			t.Chdir(directory)
			unsetFGraphDBForTest(t)
			if err := os.WriteFile(legacyDefaultDatabasePath, nil, 0o600); err != nil {
				t.Fatal(err)
			}

			_, err := runCLIForTest(t, "", args...)
			if !errors.Is(err, ErrType) {
				t.Fatalf("%v error = %v, want TypeError before database access", args, err)
			}
			contents, readErr := os.ReadFile(legacyDefaultDatabasePath)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(contents) != 0 {
				t.Fatalf("%v initialized the legacy database to %d bytes", args, len(contents))
			}
			if _, statErr := os.Lstat(defaultDatabasePath); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("%v created the new default path: %v", args, statErr)
			}
		})
	}
}

func TestValidatedCLIArgumentsRequirePreOpenValidation(t *testing.T) {
	cmd := &cli.Command{Name: "tx"}
	if _, err := loadValidatedCLIArguments(cmd); !errors.Is(err, ErrFormat) {
		t.Fatalf("missing validation metadata error = %v, want FormatError", err)
	}

	storeValidatedCLIArguments(cmd, 65, 66)
	validated, err := loadValidatedCLIArguments(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if validated.firstTransaction != 65 || validated.secondTransaction != 66 {
		t.Fatalf("validated arguments = %+v, want transactions 65 and 66", validated)
	}
}

func TestCLIRejectsMalformedHistoricalSelectorBeforeOpeningDatabase(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	unsetFGraphDBForTest(t)
	if err := os.WriteFile(legacyDefaultDatabasePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	for _, args := range [][]string{
		{"get", "entity", "--at", "not-an-integer"},
		{"q", `{}`, "--at", "not-an-integer"},
	} {
		_, err := runCLIForTest(t, "", args...)
		var exit cli.ExitCoder
		if !errors.As(err, &exit) || exit.ExitCode() != 2 || !strings.Contains(err.Error(), "is not an integer") {
			t.Errorf("%v error = %v, want actionable usage exit 2", args, err)
		}
	}
	if _, err := os.Lstat(defaultDatabasePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("malformed historical selector reached database opening: %v", err)
	}
}

func TestCLIMCPEmbedCommandHandlesCleanEOF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp.fgraph")
	if _, err := runCLIForTest(t, "", "init", "--db", path); err != nil {
		t.Fatal(err)
	}
	if _, err := runCLIForTest(t, "", "mcp", "--db", path, "--embed-cmd", "unused"); err != nil {
		t.Fatalf("MCP clean EOF with an embedding command: %v", err)
	}
}

func TestCLIBatchedAddIsBoundedAndResumable(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	path := filepath.Join(t.TempDir(), "bulk.db")
	input := "{\"id\":\"bulk/0\",\"bulk/value\":0}\n{\"id\":\"bulk/1\",\"bulk/value\":1}\n{\"id\":\"bulk/2\",\"bulk/value\":2}\n"
	arguments := []string{"add", "--batch-size", "2", "--operation-id-prefix", "import:bulk", "--db", path, "--json", "-"}
	firstOutput, err := runCLIForTest(t, input, arguments...)
	if err != nil {
		t.Fatal(err)
	}
	var first map[string]any
	if decodeErr := json.Unmarshal([]byte(firstOutput), &first); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if first["batches"] != float64(2) || first["items"] != float64(3) || first["applied"] != float64(2) || first["already_applied"] != float64(0) || first["basis_tx"] != first["tx"] {
		t.Fatalf("first batch summary = %s", firstOutput)
	}
	retriedOutput, err := runCLIForTest(t, input, arguments...)
	if err != nil {
		t.Fatal(err)
	}
	var retried map[string]any
	if err := json.Unmarshal([]byte(retriedOutput), &retried); err != nil {
		t.Fatal(err)
	}
	if retried["applied"] != float64(0) || retried["already_applied"] != float64(2) || retried["basis_tx"] != first["tx"] {
		t.Fatalf("retried batch summary = %s", retriedOutput)
	}

	partialArguments := []string{"add", "--batch-size", "1", "--operation-id-prefix", "import:partial", "--db", path, "--json", "-"}
	if _, partialErr := runCLIForTest(t, "{\"id\":\"partial/0\"}\n{\n", partialArguments...); !errors.Is(partialErr, ErrType) {
		t.Fatalf("malformed streamed batch error = %v, want TypeError", partialErr)
	}
	partialOutput, partialErr := runCLIForTest(t, "{\"id\":\"partial/0\"}\n{\"id\":\"partial/1\"}\n", partialArguments...)
	if partialErr != nil {
		t.Fatal(partialErr)
	}
	var partial map[string]any
	if decodeErr := json.Unmarshal([]byte(partialOutput), &partial); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if partial["items"] != float64(2) || partial["already_applied"] != float64(1) || partial["applied"] != float64(1) {
		t.Fatalf("resumed partial batch summary = %s", partialOutput)
	}
}

func TestCLISchemaManifestWorkflow(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	path := filepath.Join(t.TempDir(), "schema.db")
	if _, err := runCLIForTest(t, "", "declare", "--type", "text", "--unique", "--db", path, "schema/id"); err != nil {
		t.Fatal(err)
	}
	exported, err := runCLIForTest(t, "", "schema-export", "--db", path, "--json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest SchemaManifest
	if decodeErr := json.Unmarshal([]byte(exported), &manifest); decodeErr != nil || len(manifest.Attributes) != 1 {
		t.Fatalf("schema export = %s, %v", exported, decodeErr)
	}
	checked, err := runCLIForTest(t, "", "schema-check", "--db", path, "--json", exported)
	if err != nil || !strings.Contains(checked, `"valid":true`) {
		t.Fatalf("schema check = %s, %v", checked, err)
	}
	empty := `{"fgraph":"schema/1","attributes":[],"shapes":[]}`
	applied, err := runCLIForTest(t, "", "schema-apply", "--operation-id", "schema:empty", "--db", path, "--json", empty)
	if err != nil || !strings.Contains(applied, `"status":"applied"`) {
		t.Fatalf("schema apply = %s, %v", applied, err)
	}
	checked, err = runCLIForTest(t, "", "schema-check", "--db", path, "--json", empty)
	if err != nil || !strings.Contains(checked, `"valid":true`) {
		t.Fatalf("empty schema check = %s, %v", checked, err)
	}
	for _, digest := range []string{`1`, `null`, `true`, `{"stale":true}`, `["stale"]`} {
		input := fmt.Sprintf(`{"fgraph":"schema/1","digest":%s,"attributes":[],"shapes":[]}`, digest)
		checked, digestErr := runCLIForTest(t, "", "schema-check", "--db", path, "--json", input)
		if digestErr != nil || !strings.Contains(checked, `"valid":true`) {
			t.Fatalf("schema check with ignored digest %s = %s, %v", digest, checked, digestErr)
		}
	}
	invalidFields := map[string]string{
		"missing attribute declaration": `{"fgraph":"schema/1","attributes":[{"name":"schema/id"}],"shapes":[]}`,
		"unknown declaration field":     `{"fgraph":"schema/1","attributes":[{"name":"schema/id","declared":{"other":true}}],"shapes":[]}`,
		"missing shape field":           `{"fgraph":"schema/1","attributes":[],"shapes":[{"name":"shape/schema","required":[],"allowed":[]}]}`,
		"unknown top-level field":       `{"fgraph":"schema/1","attributes":[],"shapes":[],"other":true}`,
	}
	for name, input := range invalidFields {
		t.Run(name, func(t *testing.T) {
			if _, invalidErr := runCLIForTest(t, "", "schema-check", "--db", path, input); !errors.Is(invalidErr, ErrSchema) {
				t.Fatalf("schema-check error = %v, want SchemaError", invalidErr)
			}
		})
	}
	invalidDocuments := []struct {
		want  error
		name  string
		input string
	}{
		{name: "malformed JSON", input: `{`, want: ErrType},
		{name: "non-object manifest", input: `[]`, want: ErrType},
		{name: "missing fgraph", input: `{}`, want: ErrSchema},
		{name: "attributes container", input: `{"fgraph":"schema/1","attributes":{}}`, want: ErrSchema},
		{name: "non-object attribute", input: `{"fgraph":"schema/1","attributes":[1]}`, want: ErrSchema},
		{name: "declaration container", input: `{"fgraph":"schema/1","attributes":[{"name":"schema/id","declared":[]}]}`, want: ErrSchema},
		{name: "shapes container", input: `{"fgraph":"schema/1","shapes":{}}`, want: ErrSchema},
		{name: "non-object shape", input: `{"fgraph":"schema/1","shapes":[1]}`, want: ErrSchema},
		{name: "required container", input: `{"fgraph":"schema/1","shapes":[{"name":"shape/schema","required":{},"allowed":[],"closed":false}]}`, want: ErrSchema},
	}
	for _, test := range invalidDocuments {
		t.Run(test.name, func(t *testing.T) {
			if _, invalidErr := runCLIForTest(t, "", "schema-check", "--db", path, test.input); !errors.Is(invalidErr, test.want) {
				t.Fatalf("schema-check error = %v, want %v", invalidErr, test.want)
			}
		})
	}
}

func TestCLIWorkflow(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	dir := t.TempDir()
	path := filepath.Join(dir, "cli.db")
	dbArgs := []string{"--db", path, "--json"}
	command := func(stdin, name string, args ...string) (string, error) {
		all := append([]string{name}, dbArgs...)
		all = append(all, args...)
		return runCLIForTest(t, stdin, all...)
	}
	if output, err := command("", "init"); err != nil || !strings.Contains(output, `"application_id"`) {
		t.Fatalf("init = %s, %v", output, err)
	} else if !strings.HasPrefix(output, `{"application_id":1718055521,"attributes":`) {
		t.Fatalf("machine output is not canonical key order: %s", output)
	}
	if _, err := command("", "declare", "person/vector", "--type", "vector", "--dims", "2"); err != nil {
		t.Fatal(err)
	}
	added, addErr := command("", "add", `{"id":"ada","person/name":"Ada","person/bio":"compiler pioneer","person/vector":{"vector":[1,0]}}`)
	if addErr != nil {
		t.Fatal(addErr)
	}
	var report TxReport
	if err := json.Unmarshal([]byte(added), &report); err != nil || report.Tx == 0 {
		t.Fatalf("add = %s, %v", added, err)
	}
	if _, err := command("", "add", `{"id":"ada","person/name":"Augusta"}`); err != nil {
		t.Fatal(err)
	}
	asOf := strconv.FormatInt(report.Tx, 10)
	if historical, err := command("", "get", "ada", "--at", asOf); err != nil || !strings.Contains(historical, `"person/name":"Ada"`) {
		t.Fatalf("historical get = %s, %v", historical, err)
	}
	queryAt := `{"find":["?name"],"where":[["ada","person/name","?name"]]}`
	if historical, err := command("", "q", queryAt, "--at", asOf); err != nil || !strings.Contains(historical, `"rows":[["Ada"]]`) {
		t.Fatalf("historical query = %s, %v", historical, err)
	}
	if _, err := command("{\"id\":\"grace\",\"person/name\":\"Grace\"}\n{\"id\":\"linus\",\"person/name\":\"Linus\"}\n", "add", "-"); err != nil {
		t.Fatal(err)
	}
	for _, call := range []struct {
		name string
		args []string
	}{
		{"info", nil},
		{"get", []string{"ada", "--depth", "1"}},
		{"q", []string{`{"find":["?n"],"where":[["?e","person/name","?n"]],"order":[["?n","asc"]]}`}},
		{"search", []string{"compiler", "--k", "2"}},
		{"search", []string{"--vector", `[1,0]`, "--vector-attribute", "person/vector"}},
		{"history", []string{"ada", "person/name"}},
		{"why", []string{"ada", "person/name"}},
		{"diff", []string{strconv.FormatInt(GenesisTx, 10), strconv.FormatInt(report.Tx, 10)}},
		{"declare", []string{"person/knows", "--ref", "--many", "--doc", "friends"}},
		{"schema", []string{"person/"}},
		{"retract", []string{"ada", "person/bio", `"compiler pioneer"`}},
		{"undo", []string{strconv.FormatInt(report.Tx, 10)}},
		{"tail", []string{"--since", strconv.FormatInt(GenesisTx, 10)}},
		{"doctor", nil},
	} {
		if output, err := command("", call.name, call.args...); err != nil || output == "" {
			t.Fatalf("%s = %q, %v", call.name, output, err)
		}
	}
	queryFile := filepath.Join(dir, "query.json")
	if err := os.WriteFile(queryFile, []byte(`{"find":["?n"],"where":[["?e","person/name","?n"]]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := command("", "q", "@"+queryFile, "--args", `{}`); err != nil {
		t.Fatal(err)
	}
	snapshotPath := filepath.Join(dir, "memory.snapshot.ndjson")
	snapshot, snapshotErr := command("", "snapshot")
	if snapshotErr != nil {
		t.Fatal(snapshotErr)
	}
	if err := os.WriteFile(snapshotPath, []byte(snapshot), 0o600); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(snapshotPath); err != nil || info.Size() == 0 {
		t.Fatalf("snapshot file = %v, %v", info, err)
	}
	copyPath := filepath.Join(dir, "copy.db")
	if output, err := runCLIForTest(t, "", "restore", "--db", copyPath, "--json", snapshotPath); err != nil || !strings.Contains(output, `"ok":true`) {
		t.Fatalf("restore = %s, %v", output, err)
	}
	backupPath := filepath.Join(dir, "backup.db")
	if _, err := command("", "backup", backupPath); err != nil {
		t.Fatal(err)
	}
	if version, err := runCLIForTest(t, "", "version"); err != nil || strings.TrimSpace(version) != Version {
		t.Fatalf("version = %q, %v", version, err)
	}

	embed := embeddingHelperCommand(t, "success")
	if _, err := command("", "search", "compiler", "--embed-cmd", embed, "--vector-attribute", "person/vector"); err != nil {
		t.Fatal(err)
	}
}

func TestCLIUsageAndParsingErrors(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "bad")
	path := filepath.Join(t.TempDir(), "bad-clock.db")
	if _, err := runCLIForTest(t, "", "init", "--db", path); !errors.Is(err, ErrType) {
		t.Fatalf("clock error = %v", err)
	}
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	if _, err := runCLIForTest(t, "", "init", "--db", path); err != nil {
		t.Fatal(err)
	}
	if _, err := runCLIForTest(t, "", "get", "--db", path, "999"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown numeric CLI get error = %v", err)
	}
	if _, err := runCLIForTest(t, " \n\t", "add", "--db", path, "-"); !errors.Is(err, ErrType) {
		t.Fatalf("blank add stdin error = %v", err)
	}
	for _, args := range [][]string{
		{"add", "--db", path},
		{"get", "--db", path},
		{"q", "--db", path},
		{"history", "--db", path},
		{"why", "--db", path},
		{"diff", "--db", path, "x", "y"},
		{"declare", "--db", path},
		{"undo", "--db", path, "x"},
		{"backup", "--db", path},
		{"schema", "--db", path, "a/", "b/"},
		{"doctor", "--db", path, "unexpected"},
	} {
		if _, err := runCLIForTest(t, "", args...); err == nil {
			t.Fatalf("%v unexpectedly succeeded", args)
		} else {
			var exit cli.ExitCoder
			if !errors.As(err, &exit) && !errors.Is(err, ErrType) {
				t.Fatalf("%v error = %v", args, err)
			}
		}
	}
	if _, err := parseVectorJSON(`{"bad":true}`); err == nil {
		t.Fatal("invalid vector accepted")
	}
	for _, raw := range []string{`[]`, `[0,0]`, `[1e100]`} {
		if _, err := parseVectorJSON(raw); !errors.Is(err, ErrType) {
			t.Fatalf("parseVectorJSON(%s) error = %v", raw, err)
		}
	}
	if got := parseRef("42"); got != int64(42) {
		t.Fatalf("parseRef = %v", got)
	}
	if got := parseJSONOrText("plain"); got != "plain" {
		t.Fatalf("parseJSONOrText = %v", got)
	}
}

func TestCommandEmbedderFailuresAreTypedAndNoShell(t *testing.T) {
	ctx := context.Background()
	missing := filepath.Join(t.TempDir(), "missing-embedder")
	if _, err := commandEmbedder(missing)(ctx, "text"); !errors.Is(err, ErrType) {
		t.Fatalf("missing embedder error = %v", err)
	}

	dir := t.TempDir()
	failing := embeddingHelperCommand(t, "failure")
	if _, err := commandEmbedder(failing)(ctx, "text"); !errors.Is(err, ErrType) {
		t.Fatalf("failing embedder error = %v", err)
	}

	success := embeddingHelperCommand(t, "success")
	marker := filepath.Join(dir, "must-not-exist")
	vector, embedErr := commandEmbedder(success)(ctx, "text")
	if embedErr != nil || len(vector) != 2 || vector[0] != 1 {
		t.Fatalf("embedder vector = %v, %v", vector, embedErr)
	}
	if _, err := commandEmbedder(success+" ; touch "+marker)(ctx, "text"); !errors.Is(err, ErrType) {
		t.Fatalf("non-JSON command arguments error = %v", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("command string was interpreted by a shell: %v", err)
	}

	withArg := embeddingHelperCommand(t, "argument", "2")
	vector, embedErr = commandEmbedder(withArg)(ctx, "text")
	if embedErr != nil || len(vector) != 2 || vector[0] != 2 {
		t.Fatalf("JSON argv embedder = %v, %v", vector, embedErr)
	}
	for _, raw := range []string{"[]", `["", "arg"]`, `[1]`, `["x", 1]`, `[`} {
		if _, err := parseEmbeddingCommand(raw); !errors.Is(err, ErrType) {
			t.Errorf("parseEmbeddingCommand(%q) error = %v", raw, err)
		}
	}

	slow := embeddingHelperCommand(t, "slow")
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := commandEmbedder(slow)(timeoutCtx, "text"); !errors.Is(err, ErrType) {
		t.Fatalf("timed-out embedder error = %v", err)
	}

	descendant := embeddingHelperCommand(t, "descendant")
	descendantCtx, descendantCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer descendantCancel()
	started := time.Now()
	if _, err := commandEmbedder(descendant)(descendantCtx, "text"); !errors.Is(err, ErrType) {
		t.Fatalf("descendant-held stdout error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("descendant-held stdout returned after %s; timeout must remain bounded", elapsed)
	}

	oversize := embeddingHelperCommand(t, "oversize")
	if _, err := commandEmbedder(oversize)(ctx, "text"); !errors.Is(err, ErrType) {
		t.Fatalf("oversized embedder output error = %v", err)
	}
	output := &limitedCommandOutput{limit: 3}
	if count, err := output.Write([]byte("ab")); err != nil || count != 2 {
		t.Fatalf("bounded output first write = %d, %v", count, err)
	}
	if count, err := output.Write([]byte("cd")); !errors.Is(err, ErrType) || count != 0 {
		t.Fatalf("bounded output overflow = %d, %v", count, err)
	}
}

func TestCLITailFollowStreamsExportRecords(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	path := filepath.Join(t.TempDir(), "tail.db")
	db := fixedDB(t, path)
	first, err := db.Transact(context.Background(), E{"id": "one", "event/name": "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.Transact(context.Background(), E{"id": "two", "event/name": "second"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Cancel from observed output instead of racing a fixed deadline against
	// record export under the race detector.
	var output bytes.Buffer
	writer := cancelAfterLinesWriter{output: &output, cancel: cancel, remaining: 2}
	err = RunCLI(ctx, []string{"fgraph", "tail", "--db", path, "--json", "--since", strconv.FormatInt(GenesisTx, 10), "--follow"}, strings.NewReader(""), &writer, &bytes.Buffer{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("tail error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("tail lines = %d\n%s", len(lines), output.String())
	}
	for i, want := range []string{first.EventID, second.EventID} {
		value, decodeErr := DecodeJSON(strings.NewReader(lines[i]))
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		line, _ := objectMap(value)
		if line["event"] != want {
			t.Fatalf("tail line %d event = %v, want %s", i, line["event"], want)
		}
		if _, ok := line["change"]; ok {
			t.Fatalf("tail emitted FollowEvent instead of an event/1 record: %s", lines[i])
		}
		if _, ok := line["asserted"].([]any); !ok {
			t.Fatalf("tail asserted missing: %s", lines[i])
		}
	}
}

type cancelAfterLinesWriter struct {
	output    *bytes.Buffer
	cancel    context.CancelFunc
	remaining int
}

func (writer *cancelAfterLinesWriter) Write(data []byte) (int, error) {
	written, err := writer.output.Write(data)
	writer.remaining -= bytes.Count(data[:written], []byte{'\n'})
	if writer.remaining <= 0 {
		writer.cancel()
	}
	return written, err
}
