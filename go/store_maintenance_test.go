package fgraph

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type countingQueryRunner struct {
	sqlRunner
	queries     int
	blobQueries int
}

func (runner *countingQueryRunner) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	runner.queries++
	return runner.sqlRunner.QueryContext(ctx, query, args...)
}

func (runner *countingQueryRunner) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if strings.Contains(query, "SELECT data FROM fgraph_blobs") {
		runner.blobQueries++
	}
	return runner.sqlRunner.QueryRowContext(ctx, query, args...)
}

func TestOpenFormatValidationAndLifecycleEdges(t *testing.T) {
	ctx := context.Background()
	if _, err := Open(""); !errors.Is(err, ErrFormat) {
		t.Fatalf("empty path error = %v", err)
	}
	if _, err := Open(":memory:", WithClock(func() int64 { return maxInstantMicros + 1 })); !errors.Is(err, ErrType) {
		t.Fatalf("invalid genesis instant error = %v", err)
	}
	if got, err := sqliteDSN(":memory:", true); err != nil || got != ":memory:" {
		t.Fatalf("memory DSN = %q, %v", got, err)
	}
	if got, err := sqliteDSN("relative.db", false); err != nil || got != "relative.db" {
		t.Fatalf("writable DSN = %q, %v", got, err)
	}
	if got, err := sqliteDSN("relative.db", true); err != nil || !strings.HasPrefix(got, "file:") || !strings.Contains(got, "mode=ro") {
		t.Fatalf("readonly DSN = %q, %v", got, err)
	}

	makeSQLite := func(t *testing.T, setup ...string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "test.db")
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, "PRAGMA user_version=0"); err != nil {
			closeTest(t, db)
			t.Fatal(err)
		}
		for _, statement := range setup {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				closeTest(t, db)
				t.Fatal(err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		return path
	}
	foreign := makeSQLite(t, "PRAGMA application_id=123", "PRAGMA user_version=4")
	if _, err := Open(foreign); !errors.Is(err, ErrFormat) {
		t.Fatalf("foreign markers error = %v", err)
	}
	partial := makeSQLite(t, "CREATE TABLE fgraph_partial(x)")
	if _, err := Open(partial); !errors.Is(err, ErrFormat) {
		t.Fatalf("partial schema error = %v", err)
	}
	claimed := makeSQLite(t, "PRAGMA application_id=1718055521", "PRAGMA user_version=1")
	if _, err := Open(claimed); !errors.Is(err, ErrFormat) {
		t.Fatalf("claimed incomplete schema error = %v", err)
	}
	empty := makeSQLite(t)
	if _, err := Open(empty, WithReadOnly()); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("readonly uninitialized error = %v", err)
	}
	notSQLite := filepath.Join(t.TempDir(), "not-sqlite.db")
	if err := os.WriteFile(notSQLite, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(notSQLite); !errors.Is(err, ErrFormat) {
		t.Fatalf("non-SQLite error = %v", err)
	}

	var nilDB *DB
	if err := nilDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := nilDB.checkUsable(false); !errors.Is(err, ErrFormat) {
		t.Fatalf("nil usable error = %v", err)
	}
	db := fixedDB(t, ":memory:")
	view := db.atTx(GenesisTx)
	if err := view.Close(); err != nil {
		t.Fatalf("view close = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second close = %v", err)
	}
	if _, err := db.Stats(ctx); !errors.Is(err, ErrFormat) {
		t.Fatalf("closed read error = %v", err)
	}
	if _, err := db.Apply(ctx, strings.NewReader("")); !errors.Is(err, ErrFormat) {
		t.Fatalf("closed apply error = %v", err)
	}
}

func TestConcurrentPristineInitializationAcceptsTheWinningStore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "concurrent.db")
	newStore := func() *store {
		runner, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		runner.SetMaxOpenConns(1)
		runner.SetMaxIdleConns(1)
		t.Cleanup(func() { closeTest(t, runner) })
		root := &store{
			sql: runner, path: path, clock: func() int64 { return 1_767_225_600_000_000 },
			names: map[string]int64{}, idNames: map[int64]string{}, gids: map[int64]string{},
		}
		if err := root.configure(ctx, false); err != nil {
			t.Fatal(err)
		}
		return root
	}
	stores := []*store{newStore(), newStore()}
	start := make(chan struct{})
	errorsByStore := make([]error, len(stores))
	var wait sync.WaitGroup
	for index, root := range stores {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errorsByStore[index] = root.initializeIfPristine(ctx)
		}()
	}
	close(start)
	wait.Wait()
	for index, err := range errorsByStore {
		if err != nil {
			t.Errorf("initializer %d = %v", index, err)
		}
	}
	if err := stores[0].validateSchemaLayout(ctx); err != nil {
		t.Fatalf("winning schema is invalid: %v", err)
	}
}

func TestSchemaManifestRoundTripAndAtomicValidation(t *testing.T) {
	ctx := context.Background()
	source := fixedDB(t, ":memory:")
	if _, err := source.Declare(ctx, "person/name", Type("text"), Doc("Display name")); err != nil {
		t.Fatal(err)
	}
	if _, err := source.DeclareShape(ctx, "person/shape", ShapeDefinition{Required: []string{"person/name"}, Closed: true}); err != nil {
		t.Fatal(err)
	}
	manifest, err := source.SchemaManifest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	check, err := source.CheckSchemaManifest(ctx, manifest)
	if err != nil || !check.Valid {
		t.Fatalf("manifest check = %#v, %v", check, err)
	}
	target := fixedDB(t, ":memory:")
	if _, applyErr := target.ApplySchemaManifest(ctx, manifest, WithOperationID("schema:v1")); applyErr != nil {
		t.Fatal(applyErr)
	}
	applied, err := target.SchemaManifest(ctx)
	if err != nil || applied.Digest != manifest.Digest {
		t.Fatalf("applied manifest = %#v, %v", applied, err)
	}
	zero := int64(0)
	invalid := manifest
	invalid.Attributes = []SchemaManifestAttribute{{Name: "person/name", Declared: DeclaredAttribute{Dims: &zero}}}
	if _, err := target.ApplySchemaManifest(ctx, invalid); !errors.Is(err, ErrSchema) {
		t.Fatalf("invalid manifest error = %v", err)
	}
}

func TestSchemaManifestRejectsMalformedControlPlaneAndReportsDrift(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	textType, unknownType := "text", "unknown"
	zero := int64(0)
	blankModel := " "
	invalid := []SchemaManifest{
		{},
		{FGraph: "schema/2"},
		{FGraph: "schema/1", Attributes: []SchemaManifestAttribute{{Name: "Bad", Declared: DeclaredAttribute{Type: &textType}}}},
		{FGraph: "schema/1", Attributes: []SchemaManifestAttribute{
			{Name: "item/id", Declared: DeclaredAttribute{Type: &textType}},
			{Name: "item/id", Declared: DeclaredAttribute{Type: &textType}},
		}},
		{FGraph: "schema/1", Attributes: []SchemaManifestAttribute{{Name: "item/id", Declared: DeclaredAttribute{Type: &unknownType}}}},
		{FGraph: "schema/1", Attributes: []SchemaManifestAttribute{{Name: "item/id", Declared: DeclaredAttribute{Dims: &zero}}}},
		{FGraph: "schema/1", Attributes: []SchemaManifestAttribute{{Name: "item/id", Declared: DeclaredAttribute{VectorModel: &blankModel}}}},
		{FGraph: "schema/1", Shapes: []ShapeInfo{{Name: int64(1)}}},
		{FGraph: "schema/1", Shapes: []ShapeInfo{{Name: ""}}},
		{FGraph: "schema/1", Shapes: []ShapeInfo{{Name: "shape/item"}, {Name: "shape/item"}}},
		{FGraph: "schema/1", Shapes: []ShapeInfo{{Name: "shape/item", Required: []string{"Bad"}}}},
		{FGraph: "schema/1", Shapes: []ShapeInfo{{Name: "shape/item", Allowed: []string{"Bad"}}}},
	}
	for index, manifest := range invalid {
		if _, err := db.CheckSchemaManifest(ctx, manifest); !errors.Is(err, ErrSchema) {
			t.Errorf("invalid manifest %d error = %v, want SchemaError", index, err)
		}
	}

	vectorType, vectorModel, documentation := "vector", "local/test-v1", "Embedding"
	dims := int64(3)
	trueValue := true
	desired := SchemaManifest{
		FGraph: "schema/1",
		Attributes: []SchemaManifestAttribute{
			{Name: "item/vector", Declared: DeclaredAttribute{
				Type: &vectorType, Many: &trueValue, NoHistory: &trueValue, Dims: &dims,
				Doc: &documentation, VectorModel: &vectorModel,
			}},
			{Name: "item/id", Declared: DeclaredAttribute{Type: &textType, Unique: &trueValue}},
			{Name: "item/ignored", Declared: DeclaredAttribute{}},
		},
		Shapes: []ShapeInfo{{
			Name: "shape/item", Required: []string{"item/id", "item/id"},
			Allowed: []string{"item/vector"}, Closed: true,
		}},
	}
	check, err := db.CheckSchemaManifest(ctx, desired)
	if err != nil || check.Valid || len(check.Changes) != 3 {
		t.Fatalf("schema drift = %#v, %v", check, err)
	}
	if _, applyErr := db.ApplySchemaManifest(ctx, desired, WithOperationID("schema:rich")); applyErr != nil {
		t.Fatal(applyErr)
	}
	normalized, err := db.SchemaManifest(ctx)
	if err != nil || len(normalized.Attributes) != 2 || len(normalized.Shapes) != 1 ||
		len(normalized.Shapes[0].Required) != 1 || len(normalized.Shapes[0].Allowed) != 2 {
		t.Fatalf("normalized manifest = %#v, %v", normalized, err)
	}
	check, err = db.CheckSchemaManifest(ctx, normalized)
	if err != nil || !check.Valid || len(check.Changes) != 0 {
		t.Fatalf("normalized check = %#v, %v", check, err)
	}
	removal, err := db.CheckSchemaManifest(ctx, SchemaManifest{FGraph: "schema/1"})
	if err != nil || removal.Valid || len(removal.Changes) != 3 {
		t.Fatalf("removal drift = %#v, %v", removal, err)
	}
	for _, change := range removal.Changes {
		if change.Before == nil || change.After != nil {
			t.Fatalf("removal change = %#v", change)
		}
	}
}

func TestWorkingFactsLoadsTouchedEntitiesInBoundedPages(t *testing.T) {
	ctx := context.Background()
	db := fixedDB(t, ":memory:")
	const attribute = int64(65)
	assertions := make([]plannedFact, 1000)
	for index := range assertions {
		assertions[index] = plannedFact{
			e: int64(1000 + index), a: attribute, attr: "bulk/value",
			value: storedValue{logical: int64(index), storage: int64(index), tag: TagInt},
		}
	}
	plan := &transactionPlan{
		assertions: assertions,
		schemas:    map[int64]attributeSchema{attribute: {many: true}},
	}
	runner := &countingQueryRunner{sqlRunner: db.store.sql}
	if _, err := db.workingFactsForPlan(ctx, runner, plan); err != nil {
		t.Fatal(err)
	}
	if runner.queries > 3 {
		t.Fatalf("1000 touched entities used %d SQLite queries, want at most three 400-entity pages", runner.queries)
	}
}

func TestNameAndAllocatorBoundaries(t *testing.T) {
	valid := []struct {
		name string
		attr bool
	}{{"entity", false}, {"é", false}, {"app/name", true}, {"fgraph/at", true}}
	for _, test := range valid {
		if err := validateName(test.name, test.attr); err != nil {
			t.Errorf("valid name %q = %v", test.name, err)
		}
	}
	invalid := []struct {
		kind error
		name string
		attr bool
	}{
		{name: "", kind: ErrType},
		{name: strings.Repeat("x", 513), kind: ErrType},
		{name: "bad\nname", kind: ErrType},
		{name: "NoSlash", attr: true, kind: ErrSchema},
		{name: "Bad/name", attr: true, kind: ErrSchema},
		{name: "a/b/c", attr: true, kind: ErrSchema},
		{name: "fgraph/private", kind: ErrSchema},
		{name: string([]byte{0xff}), kind: ErrType},
	}
	for _, test := range invalid {
		if err := validateName(test.name, test.attr); !errors.Is(err, test.kind) {
			t.Errorf("invalid name %q error = %v", test.name, err)
		}
	}
	ctx := context.Background()
	invalidAllocatorDB := fixedDB(t, ":memory:")
	if _, err := invalidAllocatorDB.store.sql.ExecContext(ctx, "UPDATE fgraph_meta SET value=64 WHERE key='next_id'"); err != nil {
		t.Fatal(err)
	}
	if err := invalidAllocatorDB.withRead(ctx, func(runner sqlRunner) error {
		_, allocatorErr := newAllocator(ctx, runner, invalidAllocatorDB.store)
		return allocatorErr
	}); !errors.Is(err, ErrFormat) {
		t.Fatalf("invalid allocator metadata error = %v", err)
	}
	db := fixedDB(t, ":memory:")
	db.store.mu.Lock()
	conn, connErr := db.store.sql.Conn(ctx)
	if connErr != nil {
		db.store.mu.Unlock()
		t.Fatal(connErr)
	}
	alloc, allocErr := newAllocator(ctx, conn, db.store)
	if allocErr != nil {
		closeTest(t, conn)
		db.store.mu.Unlock()
		t.Fatal(allocErr)
	}
	if _, found, err := alloc.name(ctx, "does-not-exist", false, false); err != nil || found {
		t.Fatalf("noncreating name = %v, %v", found, err)
	}
	if _, found, err := alloc.name(ctx, "fgraph/at", true, false); err != nil || !found {
		t.Fatalf("cached name = %v, %v", found, err)
	}
	id, found, err := alloc.name(ctx, "allocated", false, true)
	if err != nil || !found {
		t.Fatalf("allocated name = %d, %v, %v", id, found, err)
	}
	if again, found, err := alloc.name(ctx, "allocated", false, true); err != nil || !found || again != id {
		t.Fatalf("staged name = %d, %v, %v", again, found, err)
	}
	if err := alloc.flush(ctx); err != nil {
		t.Fatal(err)
	}
	closeTest(t, conn)
	db.store.mu.Unlock()
	if db.store.fileSize() != 0 {
		t.Fatal("memory database reported file size")
	}
	db.store.path = filepath.Join(t.TempDir(), "missing")
	if db.store.fileSize() != 0 {
		t.Fatal("missing file reported size")
	}
}

func TestBackupDoctorAndExciseEdges(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "source.db")
	db := fixedDB(t, path)
	for _, destination := range []string{"", ":memory:", path} {
		if err := db.Backup(ctx, destination); err == nil {
			t.Errorf("backup destination %q accepted", destination)
		}
	}
	nonempty := filepath.Join(dir, "existing.db")
	if err := os.WriteFile(nonempty, []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(ctx, nonempty); !errors.Is(err, ErrConflict) {
		t.Fatalf("existing backup error = %v", err)
	}
	empty := filepath.Join(dir, "empty.db")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(ctx, empty); !errors.Is(err, ErrConflict) {
		t.Fatalf("existing empty backup destination = %v", err)
	}
	backupPath := filepath.Join(dir, "backup.db")
	if err := db.Backup(ctx, backupPath); err != nil {
		t.Fatalf("new backup destination = %v", err)
	}
	backup, err := Open(backupPath, WithReadOnly())
	if err != nil {
		t.Fatalf("open backup = %v", err)
	}
	closeTest(t, backup)
	if err := db.Backup(ctx, filepath.Join(dir, "missing", "backup.db")); !errors.Is(err, ErrFormat) {
		t.Fatalf("unwritable backup error = %v", err)
	}

	if _, err := db.store.sql.ExecContext(ctx, "INSERT INTO fgraph_blobs(hash,data) VALUES (?,?)", []byte("orphan"), []byte("data")); err != nil {
		t.Fatal(err)
	}
	report, doctorErr := db.Doctor(ctx)
	if doctorErr != nil || report.OrphanedBlobs != 1 || report.OK || !report.RepairNeeded || report.Integrity != "ok" {
		t.Fatalf("orphan check = %+v, %v", report, doctorErr)
	}
	report, doctorErr = db.Doctor(ctx, true)
	if doctorErr != nil || !report.OK || !report.Repaired || report.OrphanedBlobsRemoved != 1 {
		t.Fatalf("orphan repair = %+v, %v", report, doctorErr)
	}

	if _, err := db.Transact(ctx, E{"id": "missing-blob", "doc/text": strings.Repeat("z", 300)}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.store.sql.ExecContext(ctx, "DELETE FROM fgraph_blobs WHERE hash=(SELECT v FROM fgraph_facts WHERE e=(SELECT id FROM fgraph_ids WHERE name='missing-blob') AND t=8)"); err != nil {
		t.Fatal(err)
	}
	if report, err := db.Doctor(ctx); err != nil || report.OK || len(report.Problems) == 0 {
		t.Fatalf("missing blob doctor = %+v, %v", report, err)
	}
	if report, err := db.Doctor(ctx, true); !errors.Is(err, ErrFormat) || report.OK {
		t.Fatalf("missing blob repair = %+v, %v", report, err)
	}

	intervalDB := fixedDB(t, ":memory:")
	seed, seedErr := intervalDB.Transact(ctx, E{"id": "bad-interval", "item/value": 1})
	if seedErr != nil {
		t.Fatal(seedErr)
	}
	if _, err := intervalDB.store.sql.ExecContext(ctx, "PRAGMA ignore_check_constraints=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err := intervalDB.store.sql.ExecContext(ctx, "UPDATE fgraph_facts SET rx=tx WHERE id=?", seed.Asserted[len(seed.Asserted)-1].ID); err != nil {
		t.Fatal(err)
	}
	if report, err := intervalDB.Doctor(ctx); err != nil || report.OK {
		t.Fatalf("invalid interval doctor = %+v, %v", report, err)
	}
	danglingDB := fixedDB(t, ":memory:")
	dangling, danglingErr := danglingDB.Transact(ctx, E{"id": "dangling-attribute", "item/value": 1})
	if danglingErr != nil {
		t.Fatal(danglingErr)
	}
	if _, err := danglingDB.store.sql.ExecContext(ctx, "UPDATE fgraph_facts SET a=999999 WHERE id=?", dangling.Asserted[len(dangling.Asserted)-1].ID); err != nil {
		t.Fatal(err)
	}
	if report, err := danglingDB.Doctor(ctx); err != nil || report.OK {
		t.Fatalf("dangling attribute doctor = %+v, %v", report, err)
	}

	exciseDB := fixedDB(t, ":memory:")
	if _, err := exciseDB.Excise(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing excise error = %v", err)
	}
	if _, err := exciseDB.Transact(ctx, E{"id": "child", "item/text": "child"}); err != nil {
		t.Fatal(err)
	}
	if _, err := exciseDB.Declare(ctx, "item/ref", Ref()); err != nil {
		t.Fatal(err)
	}
	if _, err := exciseDB.Transact(ctx, E{"id": "parent", "item/ref": RefTo("child")}); err != nil {
		t.Fatal(err)
	}
	excised, exciseErr := exciseDB.Excise(ctx, "child")
	if exciseErr != nil || len(excised.Retracted) != 2 {
		t.Fatalf("inbound excise = %+v, %v", excised, exciseErr)
	}
	parent, parentErr := exciseDB.Entity(ctx, "parent")
	if parentErr != nil || len(parent) != 0 {
		t.Fatalf("parent after inbound excise = %#v, %v", parent, parentErr)
	}
	readOnly := exciseDB.atTx(excised.Tx)
	if report, err := readOnly.Doctor(ctx); err != nil || !report.OK {
		t.Fatalf("as-of doctor = %+v, %v", report, err)
	}
	if _, err := readOnly.Doctor(ctx, true); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("as-of doctor repair error = %v", err)
	}
	if _, err := readOnly.Excise(ctx, "parent"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("as-of excise error = %v", err)
	}
}

func TestDoctorValidatesAllocatorAndGenesisMetadata(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		corrupt string
	}{
		{name: "non-integer next id", corrupt: "UPDATE fgraph_meta SET value='broken' WHERE key='next_id'"},
		{name: "unsafe next id", corrupt: "UPDATE fgraph_meta SET value=64 WHERE key='next_id'"},
		{name: "ahead next id", corrupt: "UPDATE fgraph_meta SET value=1000 WHERE key='next_id'"},
		{name: "missing created at", corrupt: "DELETE FROM fgraph_meta WHERE key='created_at'"},
		{name: "non-integer created at", corrupt: "UPDATE fgraph_meta SET value='broken' WHERE key='created_at'"},
		{name: "out-of-range created at", corrupt: "UPDATE fgraph_meta SET value=253402300800000000 WHERE key='created_at'"},
		{name: "mismatched genesis", corrupt: "UPDATE fgraph_meta SET value=value+1 WHERE key='created_at'"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := fixedDB(t, ":memory:")
			if _, err := db.store.sql.ExecContext(ctx, test.corrupt); err != nil {
				t.Fatal(err)
			}
			report, err := db.Doctor(ctx)
			if err != nil || report.OK || len(report.Problems) == 0 {
				t.Fatalf("Doctor() = %+v, %v", report, err)
			}
		})
	}
}
