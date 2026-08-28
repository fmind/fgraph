package fgraph

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	modernsqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const schemaSQL = `
CREATE TABLE fgraph_meta (
  key TEXT NOT NULL PRIMARY KEY,
  value ANY NOT NULL
) STRICT;
CREATE TABLE fgraph_ids (
  id INTEGER NOT NULL PRIMARY KEY,
  name TEXT UNIQUE,
  gid BLOB UNIQUE,
  created_tx INTEGER NOT NULL,
  CHECK ((name IS NULL) <> (gid IS NULL)),
  CHECK (gid IS NULL OR (typeof(gid) = 'blob' AND length(gid) = 16)),
  CHECK (created_tx >= 64)
) STRICT;
CREATE INDEX fgraph_ids_created ON fgraph_ids (created_tx, id);
CREATE TABLE fgraph_events (
  tx INTEGER NOT NULL PRIMARY KEY,
  event_hash BLOB NOT NULL,
  event_data TEXT,
  operation_id TEXT UNIQUE,
  request_hash BLOB,
  CHECK (typeof(event_hash) = 'blob' AND length(event_hash) = 32),
  CHECK (event_data IS NULL OR typeof(event_data) = 'text'),
  CHECK (request_hash IS NULL OR (typeof(request_hash) = 'blob' AND length(request_hash) = 32)),
  CHECK ((operation_id IS NULL) = (request_hash IS NULL))
) STRICT;
CREATE TABLE fgraph_facts (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  e INTEGER NOT NULL,
  a INTEGER NOT NULL,
  v ANY NOT NULL,
  t INTEGER NOT NULL,
  tx INTEGER NOT NULL,
  rx INTEGER,
  CHECK (t BETWEEN 0 AND 10),
  CHECK (rx IS NULL OR rx > tx)
) STRICT;
CREATE TABLE fgraph_blobs (
  hash BLOB NOT NULL PRIMARY KEY,
  data ANY NOT NULL
) STRICT;
CREATE VIRTUAL TABLE fgraph_fts USING fts5(
  text, tokenize = "unicode61 remove_diacritics 2"
);
CREATE UNIQUE INDEX fgraph_eavt ON fgraph_facts (e, a, v, t) WHERE rx IS NULL;
CREATE INDEX fgraph_avet ON fgraph_facts (a, t, v, e, tx, rx, id);
CREATE INDEX fgraph_vaet ON fgraph_facts (v, a, e) WHERE rx IS NULL AND t = 0;
CREATE INDEX fgraph_hist ON fgraph_facts (e, a, tx);
CREATE INDEX fgraph_txin ON fgraph_facts (tx);
CREATE INDEX fgraph_txout ON fgraph_facts (rx) WHERE rx IS NOT NULL;
CREATE VIEW fgraph_view AS
SELECT f.id, f.e, i.name AS attribute,
       CASE WHEN f.t IN (7, 8, 9)
            THEN (SELECT b.data FROM fgraph_blobs b WHERE b.hash = f.v)
            ELSE f.v END AS value,
       f.t AS tag, f.tx, f.rx
FROM fgraph_facts f JOIN fgraph_ids i ON i.id = f.a;
CREATE VIEW fgraph_now AS SELECT * FROM fgraph_view WHERE rx IS NULL;
`

var attributePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*$`)

var systemNames = [...]string{
	"",
	"fgraph/at",
	"fgraph/by",
	"fgraph/source",
	"fgraph/meta",
	"fgraph/many",
	"fgraph/unique",
	"fgraph/nohistory",
	"fgraph/type",
	"fgraph/dims",
	"fgraph/doc",
	"fgraph/excised",
	"fgraph/undoes",
	"fgraph/imported-at",
	"fgraph/vector-model",
	"fgraph/shape",
	"fgraph/shape-required",
	"fgraph/shape-allowed",
	"fgraph/shape-closed",
}

var systemTypes = [...]string{
	"",
	"instant",
	"text",
	"text",
	"json",
	"bool",
	"bool",
	"bool",
	"text",
	"int",
	"text",
	"ref",
	"ref",
	"instant",
	"text",
	"ref",
	"ref",
	"ref",
	"bool",
}

var systemDocs = [...]string{
	"",
	"Wall-clock time of the transaction (UTC microseconds).",
	"Author of the transaction (person or agent).",
	"Provenance of the transaction (document, conversation, tool).",
	"Free-form JSON metadata on the transaction.",
	"Schema: attribute holds multiple values per entity.",
	"Schema: live values of this attribute are unique; enables upsert.",
	"Schema: superseded values are deleted instead of kept as history.",
	"Schema: enforced value type (bool,int,float,text,instant,bytes,vector,json,ref).",
	"Schema: vector dimensions for vector attributes.",
	"Schema: human/agent documentation for an attribute.",
	"Audit marker: entity was physically excised at this transaction.",
	"Audit marker: this transaction undoes another transaction.",
	"Original source timestamp retained when an import rebases transaction time.",
	"Schema: opaque identity of the embedding model used by a vector attribute.",
	"Validation: shape assigned to an entity.",
	"Validation: attribute required by a shape.",
	"Validation: attribute allowed by a closed shape.",
	"Validation: reject application attributes not allowed by the shape.",
}

const importedAtAttrID int64 = 13

type sqlRunner interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// finishRows preserves a query/scan failure while also surfacing a close
// failure. Callers use it on every early return so SQLite cursor errors are
// never silently discarded.
func finishRows(rows *sql.Rows, current error, description string) error {
	return joinErrors(current, wrapClose(rows.Close(), description))
}

type store struct {
	sql         *sql.DB
	clock       Clock
	eventIDs    EventIDFactory
	eventSeed   *string
	names       map[string]int64
	idNames     map[int64]string
	gids        map[int64]string
	path        string
	queryBudget int
	dataVersion int64
	mu          sync.Mutex
	closed      atomic.Bool
	readOnly    bool
}

// DB is a concurrency-safe connection or an immutable as-of view over one.
type DB struct {
	store *store
	asOf  *int64
	exec  sqlRunner
	owner bool
}

func Open(path string, options ...OpenOption) (*DB, error) {
	config := openConfig{
		clock:       func() int64 { return time.Now().UTC().UnixMicro() },
		queryBudget: DefaultQueryBudget,
	}
	for _, option := range options {
		if option == nil {
			return nil, fail(ErrType, "open option is nil; pass a concrete WithClock, WithEventIDFactory, WithReadOnly, or WithQueryBudget option")
		}
		option(&config)
	}
	if path == "" {
		return nil, fail(ErrFormat, "database path is empty; use a file path or :memory:")
	}
	if config.queryBudget <= 0 {
		return nil, fail(ErrType, "query budget %d is invalid; use a positive work-unit limit such as %d", config.queryBudget, DefaultQueryBudget)
	}
	if config.clock == nil {
		return nil, fail(ErrType, "clock is nil; provide a function that returns integer microseconds")
	}
	if config.eventIDSet && config.eventIDs == nil {
		return nil, fail(ErrType, "event ID factory is nil; provide a UUID factory")
	}
	if !config.eventIDSet {
		config.eventIDs = randomUUIDString
	}
	var eventSeed *string
	if !config.eventIDSet {
		if seed, present := os.LookupEnv("FGRAPH_EVENT_SEED"); present {
			eventSeed = &seed
		}
	}
	dsn, err := sqliteDSN(path, config.readOnly)
	if err != nil {
		return nil, err
	}
	db, err := openDatabase(path, dsn, config, eventSeed)
	if err == nil || !config.readOnly || !isReadOnlyDirectoryError(err) {
		return db, err
	}
	immutableDSN, ok := immutableSQLiteDSN(path)
	if !ok {
		return nil, err
	}
	return openDatabase(path, immutableDSN, config, eventSeed)
}

func openDatabase(path, dsn string, config openConfig, eventSeed *string) (*DB, error) {
	sqldb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot open %q; check the path and permissions", path)
	}
	// One physical connection plus the package lock gives predictable savepoint,
	// pragma, and multi-goroutine behavior without pretending SQLite is a server.
	sqldb.SetMaxOpenConns(1)
	sqldb.SetMaxIdleConns(1)
	root := &store{
		sql: sqldb, path: path, readOnly: config.readOnly, clock: config.clock,
		queryBudget: config.queryBudget, names: map[string]int64{}, idNames: map[int64]string{}, gids: map[int64]string{}, eventIDs: config.eventIDs,
		eventSeed: eventSeed,
	}
	db := &DB{store: root, owner: true}
	if err := root.configure(context.Background(), path == ":memory:"); err != nil {
		return nil, joinErrors(err, wrapClose(sqldb.Close(), "SQLite database after configuration failure"))
	}
	if err := root.checkOrInit(context.Background()); err != nil {
		return nil, joinErrors(err, wrapClose(sqldb.Close(), "SQLite database after format failure"))
	}
	if err := root.refreshNames(context.Background(), sqldb); err != nil {
		return nil, joinErrors(err, wrapClose(sqldb.Close(), "SQLite database after cache failure"))
	}
	return db, nil
}

func isReadOnlyDirectoryError(err error) bool {
	var sqliteErr *modernsqlite.Error
	return errors.As(err, &sqliteErr) && sqliteErr.Code() == sqlite3.SQLITE_READONLY_DIRECTORY
}

func immutableSQLiteDSN(path string) (string, bool) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", false
	}
	realPath, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", false
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, statErr := os.Lstat(realPath + suffix); statErr == nil || !errors.Is(statErr, os.ErrNotExist) {
			return "", false
		}
	}
	query := url.Values{"immutable": {"1"}, "mode": {"ro"}}
	return sqliteFileURI(realPath, query.Encode()), true
}

func sqliteFileURI(path, rawQuery string) string {
	uriPath := filepath.ToSlash(path)
	if !strings.HasPrefix(uriPath, "/") {
		// A Windows drive must be a URI path (/C:/...), not an authority (C:...).
		uriPath = "/" + uriPath
	}
	u := &url.URL{Scheme: "file", Path: uriPath, RawQuery: rawQuery}
	return u.String()
}

func sqliteDSN(path string, readOnly bool) (string, error) {
	if !readOnly || path == ":memory:" {
		return path, nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", wrap(ErrFormat, err, "cannot resolve database path %q; use a valid file path", path)
	}
	return sqliteFileURI(abs, "mode=ro"), nil
}

func (s *store) configure(ctx context.Context, memory bool) error {
	pragmas := []string{
		"PRAGMA foreign_keys = OFF",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA trusted_schema = OFF",
		"PRAGMA cell_size_check = ON",
	}
	if s.readOnly {
		pragmas = append(pragmas, "PRAGMA query_only = ON")
	} else {
		pragmas = append(pragmas, "PRAGMA synchronous = FULL")
	}
	if !memory && !s.readOnly {
		pragmas = append([]string{"PRAGMA journal_mode = WAL"}, pragmas...)
	}
	for _, pragma := range pragmas {
		if _, err := s.sql.ExecContext(ctx, pragma); err != nil {
			return wrap(ErrFormat, err, "cannot configure SQLite with %q; verify SQLite 3.37 or newer", pragma)
		}
	}
	return nil
}

func (s *store) checkOrInit(ctx context.Context) error {
	var applicationID, userVersion int64
	if err := s.sql.QueryRowContext(ctx, "PRAGMA application_id").Scan(&applicationID); err != nil {
		return wrap(ErrFormat, err, "cannot read application_id; verify this is a SQLite file")
	}
	if err := s.sql.QueryRowContext(ctx, "PRAGMA user_version").Scan(&userVersion); err != nil {
		return wrap(ErrFormat, err, "cannot read user_version; verify this is a SQLite file")
	}
	if applicationID == ApplicationID && userVersion == FormatVersion {
		return s.validateSchemaLayout(ctx)
	}
	if applicationID != 0 || userVersion != 0 {
		return fail(ErrFormat, "file format markers are application_id=%d user_version=%d, expected %d/%d; open the correct database or migrate it", applicationID, userVersion, ApplicationID, FormatVersion)
	}
	var objects int64
	if err := s.sql.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").Scan(&objects); err != nil {
		return wrap(ErrFormat, err, "cannot inspect existing SQLite objects")
	}
	if objects > 0 {
		return fail(ErrFormat, "unmarked SQLite file is not empty; initialize fgraph in a dedicated empty file")
	}
	if s.readOnly {
		return fail(ErrReadOnly, "database is not initialized and was opened read-only; run fgraph init without --read-only first")
	}
	return s.initializeIfPristine(ctx)
}

type schemaObject struct {
	name string
	kind string
	sql  string
}

var explicitSchemaObjects = [...]string{
	"fgraph_meta",
	"fgraph_ids",
	"fgraph_events",
	"fgraph_facts",
	"fgraph_blobs",
	"fgraph_fts",
	"fgraph_view",
	"fgraph_now",
	"fgraph_eavt",
	"fgraph_avet",
	"fgraph_vaet",
	"fgraph_hist",
	"fgraph_txin",
	"fgraph_txout",
	"fgraph_ids_created",
}

var ftsInternalPattern = regexp.MustCompile(`^fgraph_fts_(config|content|data|docsize|idx)$`)

func normalizeSchemaSQL(statement string) string {
	return strings.ToLower(strings.Join(strings.Fields(statement), " "))
}

func readSchemaObjects(ctx context.Context, runner sqlRunner) (objects map[string]schemaObject, resultErr error) {
	rows, err := runner.QueryContext(ctx, "SELECT name,type,COALESCE(sql,'') FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot inspect SQLite schema objects")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "SQLite schema rows")) }()
	objects = make(map[string]schemaObject)
	for rows.Next() {
		var object schemaObject
		if err := rows.Scan(&object.name, &object.kind, &object.sql); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode SQLite schema object")
		}
		objects[object.name] = object
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot enumerate SQLite schema objects")
	}
	return objects, nil
}

func referenceSchemaObjects(ctx context.Context) (objects map[string]schemaObject, resultErr error) {
	reference, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot open reference schema database")
	}
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(reference.Close(), "reference schema database"))
	}()
	if _, err := reference.ExecContext(ctx, schemaSQL); err != nil {
		return nil, wrap(ErrFormat, err, "cannot build reference format-v2 schema")
	}
	return readSchemaObjects(ctx, reference)
}

func (s *store) validateSchemaLayout(ctx context.Context) error {
	return s.validateSchemaLayoutOn(ctx, s.sql)
}

func (s *store) validateSchemaLayoutOn(ctx context.Context, runner sqlRunner) error {
	expectedAll, err := referenceSchemaObjects(ctx)
	if err != nil {
		return err
	}
	found, err := readSchemaObjects(ctx, runner)
	if err != nil {
		return err
	}
	required := make(map[string]struct{}, len(explicitSchemaObjects))
	for _, name := range explicitSchemaObjects {
		required[name] = struct{}{}
	}
	for name, object := range found {
		if ftsInternalPattern.MatchString(name) && object.kind == "table" {
			continue
		}
		expected, explicit := expectedAll[name]
		if !explicit {
			return fail(ErrFormat, "file %q contains non-format object %q; format v2 requires a dedicated SQLite file", s.path, name)
		}
		if _, requiredObject := required[name]; !requiredObject {
			return fail(ErrFormat, "file %q contains non-format object %q; format v2 requires a dedicated SQLite file", s.path, name)
		}
		delete(required, name)
		if object.kind != expected.kind || normalizeSchemaSQL(object.sql) != normalizeSchemaSQL(expected.sql) {
			return fail(ErrFormat, "file %q has a modified %s %q; restore the canonical format-v2 layout", s.path, object.kind, name)
		}
	}
	if len(required) != 0 {
		missing := make([]string, 0, len(required))
		for name := range required {
			missing = append(missing, name)
		}
		slices.Sort(missing)
		return fail(ErrFormat, "file %q is missing format-v2 objects %s; restore a valid backup or snapshot", s.path, strings.Join(missing, ", "))
	}
	return nil
}

func (s *store) initialize(ctx context.Context) (err error) {
	return s.initializeTransaction(ctx, false)
}

func (s *store) initializeIfPristine(ctx context.Context) error {
	return s.initializeTransaction(ctx, true)
}

func (s *store) initializeTransaction(ctx context.Context, recheck bool) (err error) {
	conn, err := s.sql.Conn(ctx)
	if err != nil {
		return wrap(ErrFormat, err, "cannot acquire SQLite connection for initialization")
	}
	defer func() { err = joinErrors(err, wrapClose(conn.Close(), "initialization database connection")) }()
	if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return wrap(ErrConflict, err, "cannot acquire the single-writer lock; retry after the other writer completes")
	}
	defer func() {
		if err != nil {
			if _, rollbackErr := conn.ExecContext(context.Background(), "ROLLBACK"); rollbackErr != nil {
				err = joinErrors(err, wrap(ErrFormat, rollbackErr, "cannot roll back failed initialization"))
			}
		}
	}()
	if recheck {
		initialized, recheckErr := s.recheckInitializationOn(ctx, conn)
		if recheckErr != nil {
			return recheckErr
		}
		if initialized {
			if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
				return wrap(ErrFormat, err, "cannot close concurrent initialization check")
			}
			return nil
		}
	}
	if _, err = conn.ExecContext(ctx, schemaSQL); err != nil {
		return wrap(ErrFormat, err, "cannot create fgraph format objects; verify SQLite has STRICT tables and FTS5")
	}
	at := s.clock()
	if err = validateInstantMicros(at); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, "INSERT INTO fgraph_meta(key,value) VALUES ('next_id',65),('created_at',?)", at); err != nil {
		return wrap(ErrFormat, err, "cannot initialize fgraph metadata")
	}
	for id := int64(1); id < int64(len(systemNames)); id++ {
		if _, err = conn.ExecContext(ctx, "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,?,NULL,?)", id, systemNames[id], GenesisTx); err != nil {
			return wrap(ErrFormat, err, "cannot initialize system name %q", systemNames[id])
		}
	}
	genesisUUID, parseErr := parseUUID(genesisEventID)
	if parseErr != nil {
		return wrap(ErrFormat, parseErr, "invalid compiled genesis event UUID")
	}
	if _, err = conn.ExecContext(ctx, "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,NULL,?,?)", GenesisTx, genesisUUID[:], GenesisTx); err != nil {
		return wrap(ErrFormat, err, "cannot initialize genesis event identity")
	}
	if _, err = conn.ExecContext(ctx, "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (64,1,?,5,64,NULL)", at); err != nil {
		return wrap(ErrFormat, err, "cannot initialize genesis timestamp")
	}
	for id := int64(1); id < int64(len(systemTypes)); id++ {
		result, execErr := conn.ExecContext(ctx, "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (?,8,?,4,64,NULL)", id, systemTypes[id])
		if execErr != nil {
			return wrap(ErrFormat, execErr, "cannot initialize type declaration for %q", systemNames[id])
		}
		factID, idErr := result.LastInsertId()
		if idErr != nil {
			return wrap(ErrFormat, idErr, "cannot resolve genesis type fact for %q", systemNames[id])
		}
		if _, err = conn.ExecContext(ctx, "INSERT INTO fgraph_fts(rowid,text) VALUES (?,?)", factID, systemTypes[id]); err != nil {
			return wrap(ErrFormat, err, "cannot initialize FTS type row for %q", systemNames[id])
		}
	}
	for id := int64(1); id < int64(len(systemDocs)); id++ {
		result, execErr := conn.ExecContext(ctx, "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (?,10,?,4,64,NULL)", id, systemDocs[id])
		if execErr != nil {
			return wrap(ErrFormat, execErr, "cannot initialize documentation for %q", systemNames[id])
		}
		factID, idErr := result.LastInsertId()
		if idErr != nil {
			return wrap(ErrFormat, idErr, "cannot resolve genesis documentation fact for %q", systemNames[id])
		}
		if _, err = conn.ExecContext(ctx, "INSERT INTO fgraph_fts(rowid,text) VALUES (?,?)", factID, systemDocs[id]); err != nil {
			return wrap(ErrFormat, err, "cannot initialize FTS documentation row for %q", systemNames[id])
		}
	}
	for _, id := range []int64{16, 17} {
		if _, err = conn.ExecContext(ctx, "INSERT INTO fgraph_facts(e,a,v,t,tx,rx) VALUES (?,5,1,1,64,NULL)", id); err != nil {
			return wrap(ErrFormat, err, "cannot initialize many declaration for %q", systemNames[id])
		}
	}
	genesisData, genesisHash, hashErr := genesisEventData(at)
	if hashErr != nil {
		return hashErr
	}
	if _, err = conn.ExecContext(ctx, "INSERT INTO fgraph_events(tx,event_hash,event_data,operation_id,request_hash) VALUES (?,?,?,NULL,NULL)", GenesisTx, genesisHash[:], genesisData); err != nil {
		return wrap(ErrFormat, err, "cannot initialize genesis event receipt")
	}
	if _, err = conn.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id = %d", ApplicationID)); err != nil {
		return wrap(ErrFormat, err, "cannot set fgraph application_id")
	}
	if _, err = conn.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", FormatVersion)); err != nil {
		return wrap(ErrFormat, err, "cannot set fgraph user_version")
	}
	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
		return wrap(ErrFormat, err, "cannot commit fgraph initialization")
	}
	return nil
}

func (s *store) recheckInitializationOn(ctx context.Context, runner sqlRunner) (bool, error) {
	var applicationID, userVersion int64
	if err := runner.QueryRowContext(ctx, "PRAGMA application_id").Scan(&applicationID); err != nil {
		return false, wrap(ErrFormat, err, "cannot recheck application_id under the initialization lock")
	}
	if err := runner.QueryRowContext(ctx, "PRAGMA user_version").Scan(&userVersion); err != nil {
		return false, wrap(ErrFormat, err, "cannot recheck user_version under the initialization lock")
	}
	if applicationID == ApplicationID && userVersion == FormatVersion {
		if err := s.validateSchemaLayoutOn(ctx, runner); err != nil {
			return false, err
		}
		return true, nil
	}
	if applicationID != 0 || userVersion != 0 {
		return false, fail(
			ErrFormat,
			"file format markers changed during initialization to application_id=%d user_version=%d, expected %d/%d; open the correct database or migrate it",
			applicationID, userVersion, ApplicationID, FormatVersion,
		)
	}
	var objects int64
	if err := runner.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").Scan(&objects); err != nil {
		return false, wrap(ErrFormat, err, "cannot recheck SQLite objects under the initialization lock")
	}
	if objects > 0 {
		return false, fail(ErrFormat, "unmarked SQLite file changed during initialization; use a dedicated empty file")
	}
	return false, nil
}

func (db *DB) Close() error {
	if db == nil || db.store == nil || !db.owner {
		return nil
	}
	db.store.mu.Lock()
	defer db.store.mu.Unlock()
	if db.store.closed.Load() {
		return nil
	}
	db.store.closed.Store(true)
	if err := db.store.sql.Close(); err != nil {
		return wrap(ErrFormat, err, "cannot close SQLite database")
	}
	return nil
}

func (db *DB) checkUsable(write bool) error {
	if db == nil || db.store == nil || db.store.closed.Load() {
		return fail(ErrFormat, "database is closed; open a new connection before using it")
	}
	if write && (db.store.readOnly || db.asOf != nil) {
		return fail(ErrReadOnly, "this database view is read-only; transact through the current writable connection")
	}
	return nil
}

func (s *store) refreshNames(ctx context.Context, runner sqlRunner) (resultErr error) {
	var version int64
	if err := runner.QueryRowContext(ctx, "PRAGMA data_version").Scan(&version); err != nil {
		return wrap(ErrFormat, err, "cannot check SQLite data_version")
	}
	if version == s.dataVersion && len(s.names) > 0 {
		return nil
	}
	rows, err := runner.QueryContext(ctx, "SELECT id,name,gid FROM fgraph_ids ORDER BY id")
	if err != nil {
		return wrap(ErrFormat, err, "cannot refresh entity-name cache")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "entity-name cache rows")) }()
	names := map[string]int64{}
	idNames := map[int64]string{}
	gids := map[int64]string{}
	for rows.Next() {
		var id int64
		var name sql.NullString
		var gid []byte
		if err := rows.Scan(&id, &name, &gid); err != nil {
			return wrap(ErrFormat, err, "cannot read entity-name cache row")
		}
		if name.Valid {
			names[name.String] = id
			idNames[id] = name.String
		} else if len(gid) == 16 {
			var uuid [16]byte
			copy(uuid[:], gid)
			gids[id] = formatUUID(uuid)
		} else {
			return fail(ErrFormat, "identity %d has neither a name nor a 16-byte UUID", id)
		}
	}
	if err := rows.Err(); err != nil {
		return wrap(ErrFormat, err, "cannot finish entity-name cache refresh")
	}
	s.names = names
	s.idNames = idNames
	s.gids = gids
	s.dataVersion = version
	return nil
}

func validateName(name string, attribute bool) error {
	if !utf8.ValidString(name) || len(name) < 1 || len(name) > 512 {
		return fail(ErrType, "name %q must be valid UTF-8 between 1 and 512 bytes; shorten or correct it", name)
	}
	for _, char := range name {
		if unicode.IsControl(char) {
			return fail(ErrType, "name %q contains a control character; use printable text", name)
		}
	}
	if attribute && !attributePattern.MatchString(name) {
		return fail(ErrSchema, "attribute %q is invalid; use lowercase namespace/name with exactly one slash", name)
	}
	if strings.HasPrefix(name, "fgraph/") {
		for _, system := range systemNames {
			if name == system {
				return nil
			}
		}
		return fail(ErrSchema, "name %q uses the reserved fgraph/ namespace; choose an application namespace", name)
	}
	return nil
}

type allocator struct {
	runner  sqlRunner
	store   *store
	ids     map[string]int64
	gids    map[int64]string
	pending []pendingIdentity
	first   int64
	next    int64
	dirty   bool
}

type identityKind uint8

const (
	identityAnonymous identityKind = iota
	identityNamed
	identityEvent
)

type pendingIdentity struct {
	name string
	id   int64
	kind identityKind
}

func newAllocator(ctx context.Context, runner sqlRunner, s *store) (*allocator, error) {
	var next int64
	if err := runner.QueryRowContext(ctx, "SELECT value FROM fgraph_meta WHERE key='next_id'").Scan(&next); err != nil {
		return nil, wrap(ErrFormat, err, "next_id metadata is missing; restore a valid fgraph file")
	}
	if next < FirstUserID {
		return nil, fail(ErrFormat, "next_id metadata is %d; restore a valid fgraph file with next_id at least %d", next, FirstUserID)
	}
	return &allocator{runner: runner, store: s, first: next, next: next, ids: map[string]int64{}, gids: map[int64]string{}}, nil
}

func (a *allocator) allocate(kind identityKind, name string) (int64, error) {
	if a.next == math.MaxInt64 {
		return 0, fail(ErrTooLarge, "the int64 identity allocator is exhausted; start a new fgraph file and import retained data")
	}
	id := a.next
	a.next++
	a.dirty = true
	a.pending = append(a.pending, pendingIdentity{id: id, kind: kind, name: name})
	return id, nil
}

func (a *allocator) anonymous() (int64, error) { return a.allocate(identityAnonymous, "") }

func (a *allocator) name(ctx context.Context, name string, attribute, create bool) (int64, bool, error) {
	if err := validateName(name, attribute); err != nil {
		return 0, false, err
	}
	if id, ok := a.ids[name]; ok {
		return id, true, nil
	}
	if id, ok := a.store.names[name]; ok {
		return id, true, nil
	}
	var existing int64
	if err := a.runner.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE name=?", name).Scan(&existing); err == nil {
		return existing, true, nil
	} else if err != sql.ErrNoRows {
		return 0, false, wrap(ErrFormat, err, "cannot resolve name %q", name)
	}
	if !create {
		return 0, false, nil
	}
	id, err := a.allocate(identityNamed, name)
	if err != nil {
		return 0, false, err
	}
	a.ids[name] = id
	return id, true, nil
}

func (a *allocator) tx() (int64, error) { return a.allocate(identityEvent, "") }

func (a *allocator) finalize(ctx context.Context, tx int64, event [16]byte) error {
	ordinal := uint64(0)
	for _, identity := range a.pending {
		switch identity.kind {
		case identityNamed:
			if _, err := a.runner.ExecContext(ctx, "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,?,NULL,?)", identity.id, identity.name, tx); err != nil {
				return wrap(ErrFormat, err, "cannot finalize named identity %d", identity.id)
			}
			a.store.names[identity.name] = identity.id
			if a.store.idNames == nil {
				a.store.idNames = map[int64]string{}
			}
			a.store.idNames[identity.id] = identity.name
			ordinal++
		case identityAnonymous:
			gid := anonymousUUID(event, ordinal)
			ordinal++
			if requested, exists := a.gids[identity.id]; exists {
				parsed, err := parseUUID(requested)
				if err != nil {
					return wrap(ErrType, err, "cannot finalize imported identity %d", identity.id)
				}
				gid = parsed
			}
			if _, err := a.runner.ExecContext(ctx, "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,NULL,?,?)", identity.id, gid[:], tx); err != nil {
				return wrap(ErrFormat, err, "cannot register anonymous identity %d", identity.id)
			}
			a.gids[identity.id] = formatUUID(gid)
			a.store.gids[identity.id] = a.gids[identity.id]
		case identityEvent:
			if identity.id != tx {
				return fail(ErrFormat, "event identity %d does not match transaction %d", identity.id, tx)
			}
			if _, err := a.runner.ExecContext(ctx, "INSERT INTO fgraph_ids(id,name,gid,created_tx) VALUES (?,NULL,?,?)", tx, event[:], tx); err != nil {
				return wrap(ErrFormat, err, "cannot register event identity %d", tx)
			}
			a.gids[tx] = formatUUID(event)
			a.store.gids[tx] = a.gids[tx]
		}
	}
	return nil
}

func (a *allocator) flush(ctx context.Context) error {
	if !a.dirty {
		return nil
	}
	if _, err := a.runner.ExecContext(ctx, "UPDATE fgraph_meta SET value=? WHERE key='next_id'", a.next); err != nil {
		return wrap(ErrFormat, err, "cannot persist next entity id")
	}
	return nil
}

func (s *store) fileSize() int64 {
	if s.path == ":memory:" {
		return 0
	}
	info, err := os.Stat(s.path)
	if err != nil {
		return 0
	}
	return info.Size()
}
