package fgraph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestCLIV1ParitySurfaces(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	t.Setenv("FGRAPH_EVENT_SEED", "cli-v1-surfaces")
	path := filepath.Join(t.TempDir(), "surfaces.db")
	run := func(stdin string, args ...string) (string, error) {
		base := make([]string, 0, 3+len(args))
		base = append(base, "--db", path, "--json")
		return runCLIForTest(t, stdin, append(base, args...)...)
	}
	if _, err := run("", "init"); err != nil {
		t.Fatal(err)
	}

	declaredOutput, declareErr := run("", "declare", "person/vector",
		"--type", "vector", "--dims", "2", "--vector-model", "embedding/v1",
		"--operation-id", "declare-vector-v1", "--if-basis-tx", strconv.FormatInt(GenesisTx, 10))
	if declareErr != nil {
		t.Fatal(declareErr)
	}
	declared := decodeCLIReport(t, declaredOutput)
	schemaOutput, schemaErr := run("", "schema", "person/")
	if schemaErr != nil {
		t.Fatal(schemaErr)
	}
	var snapshot SchemaSnapshot
	if err := json.Unmarshal([]byte(schemaOutput), &snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(snapshot.Digest, "sha256:") || len(snapshot.Attributes) != 1 ||
		snapshot.Attributes[0].Effective.VectorModel == nil || *snapshot.Attributes[0].Effective.VectorModel != "embedding/v1" {
		t.Fatalf("schema = %s", schemaOutput)
	}

	addPayload := `{"id":"ada","person/name":"Ada","person/vector":{"vector":[1,0]}}`
	addedOutput, addErr := run("", "add", addPayload, "--operation-id", "add-ada-v1",
		"--if-basis-tx", strconv.FormatInt(declared.Tx, 10))
	if addErr != nil {
		t.Fatal(addErr)
	}
	added := decodeCLIReport(t, addedOutput)
	for _, args := range [][]string{
		{"excise", "ada"},
		{"excise", "ada", "--operation-id", "missing-basis"},
		{"excise", "ada", "--if-basis-tx", strconv.FormatInt(added.Tx, 10)},
	} {
		if _, err := run("", args...); err == nil {
			t.Fatalf("unsafe excise flags %v unexpectedly succeeded", args)
		}
	}
	retryOutput, retryErr := run("", "add", addPayload, "--operation-id", "add-ada-v1",
		"--if-basis-tx", strconv.FormatInt(declared.Tx, 10))
	if retryErr != nil {
		t.Fatal(retryErr)
	}
	retry := decodeCLIReport(t, retryOutput)
	if retry.Status != "already_applied" || retry.Tx != added.Tx || retry.BasisTx != declared.Tx {
		t.Fatalf("retry = %s", retryOutput)
	}

	ambiguous := "{\"id\":\"must-not-exist-1\"}\n{\"id\":\"must-not-exist-2\"}\n"
	if payloads, decodeErr := decodeAddPayloads([]byte(ambiguous)); decodeErr != nil || len(payloads) != 2 {
		t.Fatalf("ambiguous fixture decoded as %d payloads: %v", len(payloads), decodeErr)
	}
	if _, err := run(ambiguous, "add", "--operation-id", "ambiguous-v1", "-"); err == nil {
		t.Fatal("operation id with NDJSON unexpectedly succeeded")
	}
	readOnly, openErr := Open(path, WithReadOnly())
	if openErr != nil {
		t.Fatal(openErr)
	}
	_, missingErr := readOnly.Entity(context.Background(), "must-not-exist-1")
	closeTest(t, readOnly)
	if !errors.Is(missingErr, ErrNotFound) {
		t.Fatalf("ambiguous NDJSON mutated the database: %v", missingErr)
	}

	shapeOutput, shapeErr := run("", "shape", "shape/person", "--required", "person/name",
		"--allowed", "person/vector", "--closed", "--operation-id", "shape-person-v1",
		"--if-basis-tx", strconv.FormatInt(added.Tx, 10))
	if shapeErr != nil {
		t.Fatal(shapeErr)
	}
	shape := decodeCLIReport(t, shapeOutput)
	shapedPayload := `{"id":"grace","fgraph/shape":{"ref":"shape/person"},"person/name":"Grace","person/vector":{"vector":[0,1]}}`
	shapedOutput, shapedErr := run("", "add", shapedPayload, "--operation-id", "add-grace-v1",
		"--if-basis-tx", strconv.FormatInt(shape.Tx, 10))
	if shapedErr != nil {
		t.Fatal(shapedErr)
	}
	shaped := decodeCLIReport(t, shapedOutput)
	if validated, err := run("", "validate", "grace"); err != nil || !strings.Contains(validated, `"valid":true`) {
		t.Fatalf("validate = %s, %v", validated, err)
	}
	if searched, err := run("", "search", "--text", "Ada", "--text-attribute", "person/name"); err != nil ||
		!strings.Contains(searched, `"entity":"ada"`) || strings.Contains(searched, `"entity":"grace"`) {
		t.Fatalf("attribute-scoped search = %s, %v", searched, err)
	}

	query := `{"find":["?name"],"where":[["?e","person/name","?name"]]}`
	if explained, err := run("", "explain", query, "--args", `{}`); err != nil ||
		!strings.Contains(explained, `"access":"avet/a"`) {
		t.Fatalf("explain = %s, %v", explained, err)
	}
	pageOutput, datomsErr := run("", "datoms", "eavt", "--components", `["grace"]`, "--limit", "1")
	if datomsErr != nil {
		t.Fatal(datomsErr)
	}
	var page DatomPage
	if err := json.Unmarshal([]byte(pageOutput), &page); err != nil || len(page.Items) != 1 || page.BasisTx != shaped.Tx {
		t.Fatalf("datoms = %s, %v", pageOutput, err)
	}
}

func TestCLIV1PortableCommandInventory(t *testing.T) {
	commands := map[string]bool{}
	for _, command := range NewCLI(strings.NewReader(""), &strings.Builder{}, &strings.Builder{}).Commands {
		commands[command.Name] = true
	}
	for _, required := range []string{"tail", "apply", "snapshot", "restore"} {
		if !commands[required] {
			t.Fatalf("missing portable command %q", required)
		}
	}
	for _, legacy := range []string{"export", "import"} {
		if commands[legacy] {
			t.Fatalf("legacy command %q remains public", legacy)
		}
	}
}

func TestCLIBoundedBulkAndSchemaManifestSurfaces(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	t.Setenv("FGRAPH_EVENT_SEED", "cli-bulk-schema")
	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "bulk.db")
	run := func(stdin string, args ...string) (string, error) {
		base := make([]string, 0, 3+len(args))
		base = append(base, "--db", path, "--json")
		return runCLIForTest(t, stdin, append(base, args...)...)
	}
	if _, err := run("", "init"); err != nil {
		t.Fatal(err)
	}
	if _, err := run("", "declare", "item/code", "--type", "text", "--unique"); err != nil {
		t.Fatal(err)
	}
	manifest, err := run("", "schema-export")
	if err != nil || !strings.Contains(manifest, `"fgraph":"schema/1"`) {
		t.Fatalf("schema export = %q, %v", manifest, err)
	}
	manifestPath := filepath.Join(directory, "schema.json")
	if writeErr := os.WriteFile(manifestPath, []byte(manifest), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if checked, checkErr := run("", "schema-check", "@"+manifestPath); checkErr != nil || !strings.Contains(checked, `"valid":true`) {
		t.Fatalf("schema check = %q, %v", checked, checkErr)
	}
	if applied, applyErr := run(manifest, "schema-apply", "--operation-id", "schema:round-trip", "-"); applyErr != nil ||
		!strings.Contains(applied, `"status":"applied"`) {
		t.Fatalf("schema apply = %q, %v", applied, applyErr)
	}
	for _, args := range [][]string{
		{"schema-export", "extra"},
		{"schema-check"},
		{"schema-check", "@" + filepath.Join(directory, "missing-schema.json")},
		{"schema-apply", `{"fgraph":"schema/1","unknown":true}`},
		{"apply", filepath.Join(directory, "missing-events.ndjson")},
		{"q", "@" + filepath.Join(directory, "missing-query.json")},
		{"explain", "@" + filepath.Join(directory, "missing-query.json")},
	} {
		if _, commandErr := run("", args...); commandErr == nil {
			t.Fatalf("invalid schema command %v succeeded", args)
		}
	}

	inline := `{"id":"bulk/inline","item/code":"inline"}`
	first, err := run("", "add", "--batch-size", "1", "--operation-id", "bulk:inline", inline)
	if err != nil || !strings.Contains(first, `"applied":1`) {
		t.Fatalf("inline batch = %q, %v", first, err)
	}
	noop, err := run("", "add", "--batch-size", "1", inline)
	if err != nil || !strings.Contains(noop, `"noop":1`) || !strings.Contains(noop, `"tx":null`) {
		t.Fatalf("noop batch = %q, %v", noop, err)
	}
	stream := "{\"id\":\"bulk/0\",\"item/code\":\"zero\"}\n\n{\"id\":\"bulk/1\",\"item/code\":\"one\"}\n"
	batchArgs := []string{"add", "--batch-size", "1", "--operation-id-prefix", "import:bulk", "-"}
	loaded, err := run(stream, batchArgs...)
	if err != nil || !strings.Contains(loaded, `"batches":2`) || !strings.Contains(loaded, `"applied":2`) {
		t.Fatalf("streamed batch = %q, %v", loaded, err)
	}
	retried, err := run(stream, batchArgs...)
	if err != nil || !strings.Contains(retried, `"already_applied":2`) {
		t.Fatalf("retried batch = %q, %v", retried, err)
	}
	batchPath := filepath.Join(directory, "batch.ndjson")
	if err := os.WriteFile(batchPath, []byte(`{"id":"bulk/file","item/code":"file"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if fromFile, fileErr := run("", "add", "--batch-size", "500", "@"+batchPath); fileErr != nil ||
		!strings.Contains(fromFile, `"items":1`) {
		t.Fatalf("file batch = %q, %v", fromFile, fileErr)
	}

	invalid := []struct {
		stdin string
		args  []string
	}{
		{"", []string{"add", "--batch-size", "1", "   "}},
		{"", []string{"add", "--batch-size", "1", "@" + filepath.Join(directory, "missing.ndjson")}},
		{"", []string{"add", "--batch-size", "1", "@" + filepath.Join(directory, "missing", "input.ndjson")}},
		{stream, []string{"add", "--batch-size", "1", "--operation-id", "one-transaction", "-"}},
		{stream, []string{"add", "--batch-size", "1", "--if-basis-tx", strconv.FormatInt(GenesisTx, 10), "-"}},
		{stream, []string{"add", "--if-basis-tx", strconv.FormatInt(GenesisTx, 10), "-"}},
		{stream, []string{"add", "--batch-size", "1", "--operation-id", "one", "--operation-id-prefix", "many", "-"}},
		{stream, []string{"add", "--operation-id-prefix", "many", "-"}},
		{stream, []string{"add", "--batch-size", "10001", "-"}},
	}
	for _, test := range invalid {
		if _, commandErr := run(test.stdin, test.args...); commandErr == nil {
			t.Fatalf("invalid bulk command %v succeeded", test.args)
		}
	}
	conflictStream := "{\"id\":\"unique/0\",\"item/code\":\"duplicate\"}\n{\"id\":\"unique/1\",\"item/code\":\"duplicate\"}\n"
	if _, commandErr := run(conflictStream, "add", "--batch-size", "2", "-"); !errors.Is(commandErr, ErrConflict) {
		t.Fatalf("conflicting batch error = %v", commandErr)
	}
	if _, commandErr := run(conflictStream, "add", "-"); !errors.Is(commandErr, ErrConflict) {
		t.Fatalf("partially committed NDJSON error = %v", commandErr)
	}

	var output strings.Builder
	if commandErr := RunCLI(ctx, []string{"fgraph", "--db", path, "add", "--batch-size", "1", "-"}, errorReader{}, &output, &output); !errors.Is(commandErr, ErrFormat) {
		t.Fatalf("failed first batch read error = %v", commandErr)
	}
	if commandErr := RunCLI(ctx, []string{"fgraph", "--db", path, "add", "--batch-size", "1", "--operation-id", "read:once", "-"}, &prefixThenErrorReader{prefix: []byte("{\"id\":\"read/once\"}\n")}, &output, &output); !errors.Is(commandErr, ErrFormat) {
		t.Fatalf("failed lookahead read error = %v", commandErr)
	}

	relativeBatch := "relative-batch.ndjson"
	if err := os.WriteFile(filepath.Join(directory, relativeBatch), []byte(`{"id":"bulk/relative"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(directory)
	if _, err := run("", "add", "--batch-size", "1", "@"+relativeBatch); err != nil {
		t.Fatal(err)
	}
}

type prefixThenErrorReader struct {
	prefix []byte
	sent   bool
}

func (reader *prefixThenErrorReader) Read(destination []byte) (int, error) {
	if reader.sent {
		return 0, errors.New("read failed")
	}
	reader.sent = true
	return copy(destination, reader.prefix), nil
}

func TestCLIV1PortableRoundTripAndExcisionSafety(t *testing.T) {
	t.Setenv("FGRAPH_CLOCK", "1767225600000000")
	t.Setenv("FGRAPH_EVENT_SEED", "cli-portable-round-trip")
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	run := func(path, stdin string, args ...string) (string, error) {
		base := make([]string, 0, 3+len(args))
		base = append(base, "--db", path, "--json")
		return runCLIForTest(t, stdin, append(base, args...)...)
	}
	if _, err := run(sourcePath, "", "init"); err != nil {
		t.Fatal(err)
	}
	output, addErr := run(sourcePath, "", "add", `{"id":"portable/item","item/value":"v1"}`,
		"--operation-id", "cli-portable-add", "--if-basis-tx", strconv.FormatInt(GenesisTx, 10))
	if addErr != nil {
		t.Fatal(addErr)
	}
	added := decodeCLIReport(t, output)
	events, tailErr := run(sourcePath, "", "tail", "--since", strconv.FormatInt(GenesisTx, 10))
	if tailErr != nil || !strings.Contains(events, `"fgraph":"event/1"`) {
		t.Fatalf("tail = %q, %v", events, tailErr)
	}
	if receipt, err := run(sourcePath, "", "tx", strconv.FormatInt(added.Tx, 10)); err != nil || !strings.Contains(receipt, added.EventID) {
		t.Fatalf("tx receipt = %q, %v", receipt, err)
	}
	snapshot, snapshotErr := run(sourcePath, "", "snapshot")
	if snapshotErr != nil || !strings.Contains(snapshot, `"fgraph":"snapshot/1"`) {
		t.Fatalf("snapshot = %q, %v", snapshot, snapshotErr)
	}

	appliedPath := filepath.Join(directory, "applied.db")
	if output, err := run(appliedPath, events, "apply", "-"); err != nil || !strings.Contains(output, `"applied":`) || !strings.Contains(output, `"events":`) {
		t.Fatalf("apply = %q, %v", output, err)
	}
	if entity, err := run(appliedPath, "", "get", "portable/item"); err != nil || !strings.Contains(entity, `"item/value":"v1"`) {
		t.Fatalf("applied entity = %q, %v", entity, err)
	}
	restoredPath := filepath.Join(directory, "restored.db")
	if output, err := run(restoredPath, snapshot, "restore", "-"); err != nil || !strings.Contains(output, `"ok":true`) {
		t.Fatalf("restore = %q, %v", output, err)
	}
	if entity, err := run(restoredPath, "", "get", "portable/item"); err != nil || !strings.Contains(entity, `"item/value":"v1"`) {
		t.Fatalf("restored entity = %q, %v", entity, err)
	}

	if _, err := run(sourcePath, "", "excise", "portable/item", "--operation-id", "cli-portable-excise", "--if-basis-tx", strconv.FormatInt(added.Tx, 10)); err != nil {
		t.Fatal(err)
	}
	if entity, err := run(sourcePath, "", "get", "portable/item"); err != nil || strings.Contains(entity, `"item/value"`) {
		t.Fatalf("excised entity = %q, %v", entity, err)
	}
	for _, args := range [][]string{
		{"snapshot", "unexpected"},
		{"apply", "a", "b"},
		{"restore", "a", "b"},
		{"tx", "bad"},
		{"tx", "1"},
		{"tx"},
		{"excise"},
	} {
		if _, err := run(sourcePath, "", args...); err == nil {
			t.Fatalf("invalid portable CLI %v unexpectedly succeeded", args)
		}
	}
}

func TestDeclareShapeReplacesDefinitionAndSupportsReceipts(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, filepath.Join(t.TempDir(), "shape.db"))
	first, declareErr := db.DeclareShape(ctx, "shape/record", ShapeDefinition{
		Required: []string{"record/name", "record/name"},
		Allowed:  []string{"record/age"},
		Closed:   true,
	}, WithOperationID("shape-record-v1"), IfBasis(GenesisTx))
	if declareErr != nil {
		t.Fatal(declareErr)
	}
	retry, retryErr := db.DeclareShape(ctx, "shape/record", ShapeDefinition{
		Required: []string{"record/name", "record/name"},
		Allowed:  []string{"record/age"},
		Closed:   true,
	}, WithOperationID("shape-record-v1"), IfBasis(GenesisTx))
	if retryErr != nil || retry.Status != "already_applied" || retry.Tx != first.Tx {
		t.Fatalf("shape retry = %#v, %v", retry, retryErr)
	}
	snapshot, schemaErr := db.Schema(ctx, "record/", false)
	if schemaErr != nil || len(snapshot.Shapes) != 1 {
		t.Fatalf("schema = %#v, %v", snapshot, schemaErr)
	}
	shape := snapshot.Shapes[0]
	if shape.Name != "shape/record" || strings.Join(shape.Required, ",") != "record/name" ||
		strings.Join(shape.Allowed, ",") != "record/age,record/name" || !shape.Closed {
		t.Fatalf("shape = %#v", shape)
	}
	if _, err := db.DeclareShape(ctx, "shape/invalid", ShapeDefinition{Required: []string{"Invalid"}}); !errors.Is(err, ErrSchema) {
		t.Fatalf("invalid shape attribute = %v", err)
	}
	if _, err := db.DeclareShape(ctx, "", ShapeDefinition{}); !errors.Is(err, ErrType) {
		t.Fatalf("invalid shape name = %v", err)
	}
	if _, err := db.DeclareShape(ctx, "shape/record", ShapeDefinition{Allowed: []string{"record/age"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, schemaErr = db.Schema(ctx, "record/", false)
	if schemaErr != nil || len(snapshot.Shapes) != 1 || snapshot.Shapes[0].Closed || len(snapshot.Shapes[0].Required) != 0 ||
		strings.Join(snapshot.Shapes[0].Allowed, ",") != "record/age" {
		t.Fatalf("replaced shape = %#v, %v", snapshot.Shapes, schemaErr)
	}
}

func TestSchemaSnapshotUsesCanonicalPresenceAndDoesNotInferType(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, filepath.Join(t.TempDir(), "schema.db"))
	before, transactErr := db.Transact(ctx, E{"id": "before", "mixed/value": int64(1)})
	if transactErr != nil {
		t.Fatal(transactErr)
	}
	if _, err := db.Declare(ctx, "mixed/value", Many(false), NoHistory(false), Doc("Value")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.Schema(ctx, "mixed/", false)
	if err != nil || len(snapshot.Attributes) != 1 {
		t.Fatalf("schema = %#v, %v", snapshot, err)
	}
	attribute := snapshot.Attributes[0]
	if attribute.Name != "mixed/value" || attribute.Declared.Many == nil || *attribute.Declared.Many ||
		attribute.Declared.NoHistory == nil || *attribute.Declared.NoHistory || attribute.Declared.Doc == nil ||
		*attribute.Declared.Doc != "Value" || attribute.Declared.Type != nil {
		t.Fatalf("declared = %#v", attribute.Declared)
	}
	if attribute.Effective.Type != nil || attribute.Effective.Many || attribute.Effective.NoHistory ||
		attribute.Effective.Doc == nil || *attribute.Effective.Doc != "Value" || attribute.Effective.Dims != nil ||
		attribute.Effective.VectorModel != nil {
		t.Fatalf("effective inferred or incomplete = %#v", attribute.Effective)
	}
	if strings.Join(attribute.Observed.Types, ",") != "int" || attribute.Observed.LiveFacts != 1 || attribute.Observed.Entities != 1 {
		t.Fatalf("observed = %#v", attribute.Observed)
	}
	if len(snapshot.Digest) != len("sha256:")+64 || !strings.HasPrefix(snapshot.Digest, "sha256:") {
		t.Fatalf("digest = %q", snapshot.Digest)
	}
	digestInput := map[string]any{
		"attributes": []any{map[string]any{
			"name": attribute.Name, "declared": declaredAttributeWire(attribute.Declared),
			"effective": effectiveAttributeWire(attribute.Effective),
		}},
		"shapes": []any{},
	}
	encoded, err := canonicalJSON(digestInput)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(encoded)
	wantDigest := hex.EncodeToString(sum[:])
	if snapshot.Digest != "sha256:"+wantDigest {
		t.Fatalf("digest = %s, want sha256:%s", snapshot.Digest, wantDigest)
	}

	historical, err := db.At(ctx, before.Tx)
	if err != nil {
		t.Fatal(err)
	}
	oldSnapshot, err := historical.Schema(ctx, "mixed/", false)
	if err != nil || len(oldSnapshot.Attributes) != 1 {
		t.Fatalf("historical schema = %#v, %v", oldSnapshot, err)
	}
	old := oldSnapshot.Attributes[0]
	if old.Declared != (DeclaredAttribute{}) || old.Effective.Type != nil || old.Effective.Doc != nil ||
		old.Observed.LiveFacts != 1 || old.Observed.Entities != 1 {
		t.Fatalf("historical schema leaked future declaration: %#v", old)
	}
	closeTest(t, historical)
}

func TestSchemaObservedEntitiesAreDistinctAcrossTypes(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, filepath.Join(t.TempDir(), "schema-observed.db"))
	if _, err := db.Declare(ctx, "mixed/many", Many()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "one", "mixed/many": []any{int64(1), "one"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "two", "mixed/many": int64(2)}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.Schema(ctx, "mixed/", false)
	if err != nil || len(snapshot.Attributes) != 1 {
		t.Fatalf("schema = %#v, %v", snapshot, err)
	}
	observed := snapshot.Attributes[0].Observed
	if strings.Join(observed.Types, ",") != "int,text" || observed.LiveFacts != 3 || observed.Entities != 2 {
		t.Fatalf("observed entities were counted per type: %#v", observed)
	}
}

func TestValidationWireUsesStableViolationCodes(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, filepath.Join(t.TempDir(), "validation.db"))
	if _, err := db.DeclareShape(ctx, "shape/person", ShapeDefinition{
		Required: []string{"person/name"}, Closed: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "ada", "fgraph/shape": RefTo("shape/person"), "person/name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transact(ctx, E{"id": "seed", "person/extra": true}); err != nil {
		t.Fatal(err)
	}
	var entity, shape, nameAttribute, extraAttribute, latest int64
	for query, destination := range map[string]*int64{
		"SELECT id FROM fgraph_ids WHERE name='ada'":          &entity,
		"SELECT id FROM fgraph_ids WHERE name='shape/person'": &shape,
		"SELECT id FROM fgraph_ids WHERE name='person/name'":  &nameAttribute,
		"SELECT id FROM fgraph_ids WHERE name='person/extra'": &extraAttribute,
		"SELECT MAX(tx) FROM fgraph_events":                   &latest,
	} {
		if err := db.store.sql.QueryRowContext(ctx, query).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.store.sql.ExecContext(ctx, "DELETE FROM fgraph_facts WHERE e=? AND a=?", entity, nameAttribute); err != nil {
		t.Fatal(err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "DELETE FROM fgraph_facts WHERE e=? AND a=17 AND v=?", shape, nameAttribute); err != nil {
		t.Fatal(err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (?,?,?,?,?,NULL)", entity, extraAttribute, 1, TagBool, latest); err != nil {
		t.Fatal(err)
	}
	report, err := db.Validate(ctx, "ada")
	if err != nil || report.Valid || len(report.Violations) != 3 {
		t.Fatalf("validation = %#v, %v", report, err)
	}
	wantCodes := []string{"shape_definition", "required", "allowed"}
	for index, want := range wantCodes {
		if report.Violations[index].Code != want || report.Violations[index].Entity != "ada" ||
			report.Violations[index].Shape != "shape/person" || report.Violations[index].Message == "" {
			t.Fatalf("violation %d = %#v", index, report.Violations[index])
		}
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"violations"`) || strings.Contains(string(encoded), `"issues"`) ||
		strings.Contains(string(encoded), `"problem"`) {
		t.Fatalf("validation JSON = %s", encoded)
	}
}

func TestExplainReportsPreClauseBindingsAndIndexedAccess(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, filepath.Join(t.TempDir(), "explain.db"))
	plan, err := db.ExplainJSON(ctx, map[string]any{
		"find": []any{"?name"}, "in": []any{"?entity"},
		"where": []any{[]any{"?entity", "person/name", "?name"}},
	}, map[string]any{"?entity": "ada"})
	if err != nil || len(plan.Clauses) != 1 || plan.Clauses[0].Access != "eavt/batch" ||
		strings.Join(plan.Clauses[0].Bound, ",") != "?entity" {
		t.Fatalf("bound plan = %#v, %v", plan, err)
	}
	unbound, err := db.ExplainJSON(ctx, map[string]any{
		"find": []any{"?entity"}, "where": []any{[]any{"?entity", "?attribute", "known"}},
	}, nil)
	if err != nil || len(unbound.Clauses) != 1 || unbound.Clauses[0].Access != "value-scan" || len(unbound.Clauses[0].Bound) != 0 {
		t.Fatalf("value plan = %#v, %v", unbound, err)
	}
}

func TestGenesisSystemDocumentationMatchesFormatV2(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, filepath.Join(t.TempDir(), "genesis-docs.db"))
	want := map[int64]string{
		14: "Schema: opaque identity of the embedding model used by a vector attribute.",
		15: "Validation: shape assigned to an entity.",
		16: "Validation: attribute required by a shape.",
		17: "Validation: attribute allowed by a closed shape.",
		18: "Validation: reject application attributes not allowed by the shape.",
	}
	for id, document := range want {
		if systemDocs[id] != document {
			t.Fatalf("systemDocs[%d] = %q", id, systemDocs[id])
		}
		entity, err := db.Entity(ctx, systemNames[id])
		if err != nil || entity[systemNames[10]] != document {
			t.Fatalf("genesis doc %d = %#v, %v", id, entity[systemNames[10]], err)
		}
	}
}

func decodeCLIReport(t *testing.T, output string) TxReport {
	t.Helper()
	var report TxReport
	if err := json.Unmarshal([]byte(output), &report); err != nil {
		t.Fatalf("decode report %q: %v", output, err)
	}
	return report
}
