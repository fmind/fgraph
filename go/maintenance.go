package fgraph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"unicode/utf8"

	"modernc.org/sqlite"
)

const backupStepPages int32 = 256

type sqliteBackupStepper func(*sqlite.Backup, int32) (bool, error)

type sqliteOnlineBackuper interface {
	NewBackup(string) (*sqlite.Backup, error)
}

func (db *DB) Backup(ctx context.Context, destination string) (resultErr error) {
	return db.backup(ctx, destination, (*sqlite.Backup).Step)
}

func (db *DB) backup(ctx context.Context, destination string, step sqliteBackupStepper) (resultErr error) {
	if destination == "" || destination == ":memory:" {
		return fail(ErrType, "backup destination %q is invalid; use a new file path", destination)
	}
	if err := db.checkUsable(false); err != nil {
		return err
	}
	destAbs, destinationErr := filepath.Abs(destination)
	if destinationErr != nil {
		return wrap(ErrFormat, destinationErr, "cannot resolve backup destination %q", destination)
	}
	if db.store.path != ":memory:" {
		sourceAbs, sourceErr := filepath.Abs(db.store.path)
		if sourceErr != nil {
			return wrap(ErrFormat, sourceErr, "cannot resolve open database path %q", db.store.path)
		}
		if sourceAbs == destAbs {
			return fail(ErrConflict, "backup destination is the open database; choose another file path")
		}
	}
	if _, statErr := os.Lstat(destAbs); statErr == nil {
		return fail(ErrConflict, "backup destination %q already exists; choose a new file path", destination)
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return wrap(ErrFormat, statErr, "cannot inspect backup destination %q", destination)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return wrap(ErrFormat, contextErr, "cannot create backup %q because the operation was canceled", destination)
	}

	directory := filepath.Dir(destAbs)
	temporary, createErr := os.CreateTemp(directory, "."+filepath.Base(destAbs)+".*.fgraph-backup")
	if createErr != nil {
		return wrap(ErrFormat, createErr, "cannot create a temporary backup beside %q; check the parent directory permissions", destination)
	}
	temporaryPath := temporary.Name()
	defer func() {
		if temporaryPath == "" {
			return
		}
		if removeErr := os.Remove(temporaryPath); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			resultErr = joinErrors(resultErr, wrap(ErrFormat, removeErr, "cannot remove temporary backup %q", temporaryPath))
		}
	}()
	if err := temporary.Close(); err != nil {
		return wrap(ErrFormat, err, "cannot close temporary backup %q before copying", temporaryPath)
	}

	db.store.mu.Lock()
	defer db.store.mu.Unlock()
	if err := db.copyOnlineBackupWithStep(ctx, temporaryPath, step); err != nil {
		return err
	}
	if err := verifyBackup(ctx, temporaryPath); err != nil {
		return err
	}
	if err := syncBackupFile(temporaryPath); err != nil {
		return err
	}
	if err := publishBackup(temporaryPath, destAbs); err != nil {
		return err
	}
	if err := os.Remove(temporaryPath); err != nil {
		return wrap(ErrFormat, err, "backup %q was published but its temporary sibling %q could not be removed", destination, temporaryPath)
	}
	temporaryPath = ""
	if err := syncBackupDirectory(directory); err != nil {
		return wrap(ErrFormat, err, "backup %q was published but its parent directory could not be synchronized", destination)
	}
	return nil
}

func publishBackup(source, destination string) error {
	if err := os.Link(source, destination); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fail(ErrConflict, "backup destination %q was created concurrently; existing data was not overwritten", destination)
		}
		return wrap(ErrFormat, err, "cannot atomically publish backup %q", destination)
	}
	return nil
}

func (db *DB) copyOnlineBackup(ctx context.Context, destination string) (resultErr error) {
	return db.copyOnlineBackupWithStep(ctx, destination, (*sqlite.Backup).Step)
}

func (db *DB) copyOnlineBackupWithStep(
	ctx context.Context,
	destination string,
	step sqliteBackupStepper,
) (resultErr error) {
	conn, err := db.store.sql.Conn(ctx)
	if err != nil {
		return wrap(ErrFormat, err, "cannot acquire the SQLite source connection for backup")
	}
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(conn.Close(), "SQLite source connection after backup"))
	}()

	if err := conn.Raw(func(driverConn any) (callbackErr error) {
		backuper, ok := driverConn.(sqliteOnlineBackuper)
		if !ok {
			return fail(ErrUnsupported, "installed SQLite driver does not expose the online backup API")
		}
		backup, backupErr := backuper.NewBackup(destination)
		if backupErr != nil {
			return wrap(ErrFormat, backupErr, "cannot initialize SQLite online backup")
		}
		finished := false
		defer func() {
			if !finished {
				if finishErr := backup.Finish(); finishErr != nil {
					callbackErr = joinErrors(callbackErr, wrap(ErrFormat, finishErr, "cannot finish SQLite online backup after failure"))
				}
			}
		}()
		for {
			if err := ctx.Err(); err != nil {
				return wrap(ErrFormat, err, "SQLite online backup was canceled")
			}
			more, stepErr := step(backup, backupStepPages)
			if stepErr != nil {
				return wrap(ErrFormat, stepErr, "cannot copy SQLite pages into the temporary backup")
			}
			if !more {
				break
			}
		}
		finishErr := backup.Finish()
		finished = true
		if finishErr != nil {
			return wrap(ErrFormat, finishErr, "cannot finalize SQLite online backup")
		}
		return nil
	}); err != nil {
		return err
	}
	return resultErr
}

func verifyBackup(ctx context.Context, path string) (resultErr error) {
	if err := ctx.Err(); err != nil {
		return wrap(ErrFormat, err, "cannot verify temporary backup because the operation was canceled")
	}
	backup, err := Open(path, WithReadOnly())
	if err != nil {
		return wrap(ErrFormat, err, "cannot open temporary backup for verification")
	}
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(backup.Close(), "temporary backup after verification"))
	}()
	report, err := backup.Doctor(ctx)
	if err != nil {
		return wrap(ErrFormat, err, "cannot verify temporary backup")
	}
	if !report.OK {
		return fail(ErrFormat, "temporary backup failed doctor verification: %v", report.Problems)
	}
	return nil
}

func syncBackupFile(path string) (resultErr error) {
	file, err := openBackupFileForSync(path)
	if err != nil {
		return wrap(ErrFormat, err, "cannot open temporary backup for synchronization")
	}
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(file.Close(), "temporary backup after synchronization"))
	}()
	if err := file.Sync(); err != nil {
		return wrap(ErrFormat, err, "cannot synchronize temporary backup contents")
	}
	return nil
}

func openBackupFileForSync(path string) (*os.File, error) {
	// Windows FlushFileBuffers requires a handle with GENERIC_WRITE access.
	return os.OpenFile(filepath.Clean(path), os.O_RDWR, 0)
}

func syncBackupDirectory(path string) (resultErr error) {
	// Windows cannot portably open a directory with the write access required
	// by FlushFileBuffers. The verified file is flushed before publication.
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(filepath.Clean(path))
	if err != nil {
		return wrap(ErrFormat, err, "cannot open backup parent directory for synchronization")
	}
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(directory.Close(), "backup parent directory after synchronization"))
	}()
	return directory.Sync()
}

func (db *DB) Doctor(ctx context.Context, repair ...bool) (result DoctorReport, resultErr error) {
	if len(repair) > 1 {
		return DoctorReport{}, fail(ErrType, "doctor accepts at most one repair flag")
	}
	repairRequested := len(repair) == 1 && repair[0]
	if err := db.checkUsable(repairRequested); err != nil {
		return DoctorReport{}, err
	}
	db.store.mu.Lock()
	defer db.store.mu.Unlock()
	conn, connErr := db.store.sql.Conn(ctx)
	if connErr != nil {
		return DoctorReport{}, wrap(ErrFormat, connErr, "cannot acquire SQLite connection for doctor")
	}
	committed := false
	defer func() {
		closeErr := wrapClose(conn.Close(), "doctor database connection")
		if !committed {
			resultErr = joinErrors(resultErr, closeErr)
		}
	}()
	if err := db.store.validateSchemaLayoutOn(ctx, conn); err != nil {
		return DoctorReport{}, err
	}
	if !repairRequested {
		report, _, reportErr := db.doctorReport(ctx, conn)
		if reportErr != nil {
			return DoctorReport{}, reportErr
		}
		return report, nil
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return DoctorReport{}, wrap(ErrConflict, err, "doctor cannot acquire the writer lock; retry after the writer completes")
	}
	defer func() {
		if !committed {
			resultErr = joinErrors(resultErr, rollbackSQLite(conn, "doctor repairs"))
		}
	}()
	before, fatal, reportErr := db.doctorReport(ctx, conn)
	if reportErr != nil {
		return DoctorReport{}, reportErr
	}
	if len(fatal) > 0 {
		return before, fail(ErrFormat, "doctor found non-rebuildable format problems %v; restore from a valid backup", fatal)
	}
	if _, execErr := conn.ExecContext(ctx, "DELETE FROM fgraph_fts"); execErr != nil {
		return DoctorReport{}, wrap(ErrFormat, execErr, "cannot clear the rebuildable FTS index")
	}
	if _, execErr := conn.ExecContext(ctx, `INSERT INTO fgraph_fts(rowid,text)
		SELECT f.id,CASE WHEN f.t=8 THEN b.data ELSE f.v END
		FROM fgraph_facts f LEFT JOIN fgraph_blobs b ON b.hash=f.v AND f.t=8
		WHERE f.rx IS NULL AND f.t IN (4,8) ORDER BY f.id`); execErr != nil {
		return DoctorReport{}, wrap(ErrFormat, execErr, "cannot rebuild FTS from live text facts")
	}
	removed, removeErr := conn.ExecContext(ctx, `DELETE FROM fgraph_blobs WHERE NOT EXISTS (
		SELECT 1 FROM fgraph_facts WHERE t IN (7,8,9) AND v=fgraph_blobs.hash
	)`)
	if removeErr != nil {
		return DoctorReport{}, wrap(ErrFormat, removeErr, "cannot remove orphaned blobs")
	}
	removedCount, countErr := removed.RowsAffected()
	if countErr != nil {
		return DoctorReport{}, wrap(ErrFormat, countErr, "cannot count removed orphaned blobs")
	}
	if _, execErr := conn.ExecContext(ctx, "ANALYZE"); execErr != nil {
		return DoctorReport{}, wrap(ErrFormat, execErr, "cannot analyze repaired database")
	}
	report, fatal, reportErr := db.doctorReport(ctx, conn)
	if reportErr != nil {
		return DoctorReport{}, reportErr
	}
	if len(fatal) > 0 || report.RepairNeeded {
		return report, fail(ErrFormat, "doctor repair left format problems %v; restore from a valid backup", report.Problems)
	}
	report.Repaired = true
	report.FTSRowsRebuilt = before.ExpectedFTSRows
	report.OrphanedBlobsRemoved = removedCount
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return DoctorReport{}, wrap(ErrFormat, err, "cannot commit doctor repairs")
	}
	committed = true
	return report, nil
}

func (db *DB) doctorReport(ctx context.Context, conn *sql.Conn) (DoctorReport, []string, error) {
	report := DoctorReport{Problems: []string{}}
	fatal := []string{}
	messages, integrityErr := readIntegrityCheck(ctx, conn)
	if integrityErr != nil {
		return DoctorReport{}, nil, integrityErr
	}
	report.Integrity = "missing result"
	if len(messages) > 0 {
		report.Integrity = messages[0]
	}
	for _, message := range messages {
		if message != "ok" {
			fatal = append(fatal, "integrity_check: "+message)
		}
	}
	var maximum sql.NullInt64
	if queryErr := conn.QueryRowContext(ctx, `SELECT MAX(identifier) FROM (
		SELECT id AS identifier FROM fgraph_ids
		UNION ALL SELECT e FROM fgraph_facts
		UNION ALL SELECT a FROM fgraph_facts
		UNION ALL SELECT tx FROM fgraph_facts
		UNION ALL SELECT rx FROM fgraph_facts WHERE rx IS NOT NULL
		UNION ALL SELECT CAST(v AS INTEGER) FROM fgraph_facts WHERE t=0
	)`).Scan(&maximum); queryErr != nil {
		return DoctorReport{}, nil, wrap(ErrFormat, queryErr, "cannot validate allocator high-water mark")
	}
	var next any
	if queryErr := conn.QueryRowContext(ctx, "SELECT value FROM fgraph_meta WHERE key='next_id'").Scan(&next); queryErr != nil && !errors.Is(queryErr, sql.ErrNoRows) {
		return DoctorReport{}, nil, wrap(ErrFormat, queryErr, "cannot validate next_id metadata")
	}
	if maximum.Valid && maximum.Int64 == math.MaxInt64 {
		fatal = append(fatal, "allocator exhausted: maximum identifier is int64 max; migrate retained data to a new fgraph file")
	} else {
		wantNext := int64(GenesisTx + 1)
		if maximum.Valid {
			wantNext = maximum.Int64 + 1
		}
		if value, ok := next.(int64); !ok || value != wantNext {
			fatal = append(fatal, fmt.Sprintf("next_id: expected %d, found %s", wantNext, doctorValue(next)))
		}
	}
	invalidIdentities, invalidIdentityErr := doctorCount(ctx, conn,
		"SELECT COUNT(*) FROM fgraph_ids WHERE id<=0 OR (id>? AND id<?)", len(systemNames)-1, GenesisTx)
	if invalidIdentityErr != nil {
		return DoctorReport{}, nil, invalidIdentityErr
	}
	if invalidIdentities > 0 {
		fatal = append(fatal, fmt.Sprintf("invalid identity ids: %d", invalidIdentities))
	}
	invalidSystemIdentities, systemIdentityErr := countInvalidSystemIdentities(ctx, conn)
	if systemIdentityErr != nil {
		return DoctorReport{}, nil, systemIdentityErr
	}
	if invalidSystemIdentities > 0 {
		fatal = append(fatal, fmt.Sprintf("invalid system identities: %d", invalidSystemIdentities))
	}
	invalidFactIDs, invalidFactErr := doctorCount(ctx, conn, `SELECT COUNT(*) FROM fgraph_facts
		WHERE id<=0 OR e<=0 OR a<=0 OR tx<? OR (rx IS NOT NULL AND rx<?) OR (t=0 AND CAST(v AS INTEGER)<=0)`, GenesisTx, GenesisTx)
	if invalidFactErr != nil {
		return DoctorReport{}, nil, invalidFactErr
	}
	if invalidFactIDs > 0 {
		fatal = append(fatal, fmt.Sprintf("invalid fact identifiers: %d", invalidFactIDs))
	}
	namedTransactions, namedTransactionErr := doctorCount(ctx, conn, `SELECT COUNT(*) FROM fgraph_ids i
		WHERE i.name IS NOT NULL AND EXISTS (SELECT 1 FROM fgraph_events event WHERE event.tx=i.id)`)
	if namedTransactionErr != nil {
		return DoctorReport{}, nil, namedTransactionErr
	}
	if namedTransactions > 0 {
		fatal = append(fatal, fmt.Sprintf("named identities overlap transaction receipts: %d", namedTransactions))
	}
	missingTransactions, missingTransactionErr := doctorCount(ctx, conn, `SELECT COUNT(*) FROM fgraph_facts f
		WHERE NOT EXISTS (SELECT 1 FROM fgraph_events event WHERE event.tx=f.tx)`)
	if missingTransactionErr != nil {
		return DoctorReport{}, nil, missingTransactionErr
	}
	if missingTransactions > 0 {
		fatal = append(fatal, fmt.Sprintf("facts reference missing asserting transactions: %d", missingTransactions))
	}
	missingRetractions, missingRetractionErr := doctorCount(ctx, conn, `SELECT COUNT(*) FROM fgraph_facts f
		WHERE f.rx IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fgraph_events event WHERE event.tx=f.rx)`)
	if missingRetractionErr != nil {
		return DoctorReport{}, nil, missingRetractionErr
	}
	if missingRetractions > 0 {
		fatal = append(fatal, fmt.Sprintf("facts reference missing retracting transactions: %d", missingRetractions))
	}
	var created any
	if queryErr := conn.QueryRowContext(ctx, "SELECT value FROM fgraph_meta WHERE key='created_at'").Scan(&created); queryErr != nil && !errors.Is(queryErr, sql.ErrNoRows) {
		return DoctorReport{}, nil, wrap(ErrFormat, queryErr, "cannot validate created_at metadata")
	}
	genesisCount, genesisAt, validGenesis, genesisErr := readGenesisReceipt(ctx, conn)
	if genesisErr != nil {
		return DoctorReport{}, nil, genesisErr
	}
	if genesisCount != 1 || !validGenesis {
		fatal = append(fatal, "genesis receipt: expected one live format-v2 self-receipt")
	} else if createdAt, ok := created.(int64); !ok || createdAt != genesisAt {
		fatal = append(fatal, fmt.Sprintf("created_at: expected genesis timestamp %d, found %s", genesisAt, doctorValue(created)))
	}
	invalidGenesisFacts, genesisFactsErr := countInvalidGenesisFacts(ctx, conn)
	if genesisFactsErr != nil {
		return DoctorReport{}, nil, genesisFactsErr
	}
	if invalidGenesisFacts > 0 {
		fatal = append(fatal, fmt.Sprintf("invalid genesis facts: %d", invalidGenesisFacts))
	}
	missingRegistry, registryErr := doctorCount(ctx, conn, `SELECT COUNT(*) FROM (
		SELECT e AS id FROM fgraph_facts UNION SELECT a FROM fgraph_facts
		UNION SELECT tx FROM fgraph_facts UNION SELECT rx FROM fgraph_facts WHERE rx IS NOT NULL
		UNION SELECT CAST(v AS INTEGER) FROM fgraph_facts WHERE t=0
	) referenced WHERE NOT EXISTS (SELECT 1 FROM fgraph_ids i WHERE i.id=referenced.id)`)
	if registryErr != nil {
		return DoctorReport{}, nil, registryErr
	}
	if missingRegistry > 0 {
		fatal = append(fatal, fmt.Sprintf("identifiers missing from registry: %d", missingRegistry))
	}
	// Aggregate first use once instead of probing the fact table for every
	// identity. The equivalent correlated OR plan grows quadratically in SQLite.
	invalidRegistryTime, registryTimeErr := doctorCount(ctx, conn, `WITH first_use AS (
		SELECT id,MIN(tx) AS tx FROM (
			SELECT e AS id,tx FROM fgraph_facts
			UNION ALL SELECT a AS id,tx FROM fgraph_facts
			UNION ALL SELECT CAST(v AS INTEGER) AS id,tx FROM fgraph_facts WHERE t=0
		) GROUP BY id
	)
	SELECT COUNT(*) FROM fgraph_ids i
	LEFT JOIN fgraph_events event ON event.tx=i.created_tx
	LEFT JOIN first_use use ON use.id=i.id
	WHERE event.tx IS NULL OR use.tx<i.created_tx`)
	if registryTimeErr != nil {
		return DoctorReport{}, nil, registryTimeErr
	}
	if invalidRegistryTime > 0 {
		fatal = append(fatal, fmt.Sprintf("invalid temporal identities: %d", invalidRegistryTime))
	}
	invalidEvents, eventErr := doctorCount(ctx, conn, `SELECT COUNT(*) FROM fgraph_events event
		LEFT JOIN fgraph_ids i ON i.id=event.tx
		WHERE i.id IS NULL OR i.name IS NOT NULL OR length(i.gid)!=16 OR i.created_tx!=event.tx
		OR length(event.event_hash)!=32 OR ((event.operation_id IS NULL)!=(event.request_hash IS NULL))
		OR (event.event_data IS NOT NULL AND (typeof(event.event_data)!='text' OR length(CAST(event.event_data AS BLOB))>?))
		OR (event.operation_id IS NOT NULL AND (typeof(event.operation_id)!='text' OR length(CAST(event.operation_id AS BLOB))<1 OR length(CAST(event.operation_id AS BLOB))>512))
		OR (event.request_hash IS NOT NULL AND length(event.request_hash)!=32)`, maxPortableLineBytes)
	if eventErr != nil {
		return DoctorReport{}, nil, eventErr
	}
	if invalidEvents > 0 {
		fatal = append(fatal, fmt.Sprintf("invalid event registry rows: %d", invalidEvents))
	}
	var danglingAttributes int64
	if queryErr := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM fgraph_facts f
		LEFT JOIN fgraph_ids i ON i.id=f.a WHERE i.id IS NULL`).Scan(&danglingAttributes); queryErr != nil {
		return DoctorReport{}, nil, wrap(ErrFormat, queryErr, "cannot validate fact attributes")
	}
	if danglingAttributes > 0 {
		fatal = append(fatal, fmt.Sprintf("dangling attributes: %d", danglingAttributes))
	}
	invalidTags, invalidPhysicalValues, valueErr := countInvalidPhysicalValues(ctx, conn)
	if valueErr != nil {
		return DoctorReport{}, nil, valueErr
	}
	if invalidTags > 0 {
		fatal = append(fatal, fmt.Sprintf("invalid value tags: %d", invalidTags))
	}
	if invalidPhysicalValues > 0 {
		fatal = append(fatal, fmt.Sprintf("invalid physical values: %d", invalidPhysicalValues))
	}
	var missingBlobs int64
	if queryErr := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM fgraph_facts f
		LEFT JOIN fgraph_blobs b ON b.hash=f.v WHERE f.t IN (7,8,9) AND b.hash IS NULL`).Scan(&missingBlobs); queryErr != nil {
		return DoctorReport{}, nil, wrap(ErrFormat, queryErr, "cannot validate indirect facts")
	}
	if missingBlobs > 0 {
		fatal = append(fatal, fmt.Sprintf("missing blobs: %d", missingBlobs))
	}
	invalidBlobs, invalidBlobErr := countInvalidIndirectBlobs(ctx, conn)
	if invalidBlobErr != nil {
		return DoctorReport{}, nil, invalidBlobErr
	}
	if invalidBlobs > 0 {
		fatal = append(fatal, fmt.Sprintf("invalid indirect blobs: %d", invalidBlobs))
	}
	var invalidIntervals int64
	if queryErr := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM fgraph_facts WHERE rx IS NOT NULL AND rx<=tx").Scan(&invalidIntervals); queryErr != nil {
		return DoctorReport{}, nil, wrap(ErrFormat, queryErr, "cannot validate transaction intervals")
	}
	if invalidIntervals > 0 {
		fatal = append(fatal, fmt.Sprintf("invalid transaction intervals: %d", invalidIntervals))
	}
	report.UnverifiableEvents, messages, valueErr = db.inspectEventHashes(ctx, conn)
	if valueErr != nil {
		return DoctorReport{}, nil, valueErr
	}
	fatal = append(fatal, messages...)
	report.SchemaProblems, valueErr = db.countGlobalSchemaProblems(ctx, conn)
	if valueErr != nil {
		return DoctorReport{}, nil, valueErr
	}
	if report.SchemaProblems > 0 {
		fatal = append(fatal, fmt.Sprintf("schema invariants violated: %d", report.SchemaProblems))
	}
	report.ShapeViolations, valueErr = db.countGlobalShapeViolations(ctx, conn)
	if valueErr != nil {
		return DoctorReport{}, nil, valueErr
	}
	if report.ShapeViolations > 0 {
		fatal = append(fatal, fmt.Sprintf("shape invariants violated: %d", report.ShapeViolations))
	}
	if queryErr := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM fgraph_blobs WHERE NOT EXISTS (
		SELECT 1 FROM fgraph_facts WHERE t IN (7,8,9) AND v=fgraph_blobs.hash
	)`).Scan(&report.OrphanedBlobs); queryErr != nil {
		return DoctorReport{}, nil, wrap(ErrFormat, queryErr, "cannot count orphaned blobs")
	}
	actual, ftsErr := readFTSRows(ctx, conn)
	if ftsErr != nil {
		return DoctorReport{}, nil, ftsErr
	}
	report.FTSRows = int64(len(actual))
	var expectedCount int64
	if queryErr := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM fgraph_facts WHERE rx IS NULL AND t IN (4,8)").Scan(&expectedCount); queryErr != nil {
		return DoctorReport{}, nil, wrap(ErrFormat, queryErr, "cannot count live text facts")
	}
	report.ExpectedFTSRows = expectedCount
	expected := []ftsRow{}
	unsafeValues := invalidTags > 0 || invalidPhysicalValues > 0 || missingBlobs > 0 || invalidBlobs > 0
	if !unsafeValues {
		var expectedErr error
		expected, expectedErr = db.readExpectedFTSRows(ctx, conn)
		if expectedErr != nil {
			return DoctorReport{}, nil, expectedErr
		}
	}
	repairProblems := []string{}
	if unsafeValues || !equalFTSRows(actual, expected) {
		repairProblems = append(repairProblems, "full-text index differs from live text facts")
	}
	if report.OrphanedBlobs > 0 {
		repairProblems = append(repairProblems, fmt.Sprintf("orphaned blobs: %d", report.OrphanedBlobs))
	}
	report.Problems = append(fatal, repairProblems...)
	report.RepairNeeded = len(repairProblems) > 0
	report.OK = len(report.Problems) == 0
	return report, fatal, nil
}

func (db *DB) inspectEventHashes(ctx context.Context, runner sqlRunner) (count int64, problems []string, resultErr error) {
	rows, err := runner.QueryContext(ctx, `SELECT ev.tx,ev.event_hash,ev.event_data,ev.operation_id,i.gid
		FROM fgraph_events ev JOIN fgraph_ids i ON i.id=ev.tx ORDER BY ev.tx`)
	if err != nil {
		return 0, nil, wrap(ErrFormat, err, "cannot enumerate event hashes")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "event hash rows")) }()
	type eventDigest struct {
		hash []byte
		gid  []byte
		data sql.NullString
		op   sql.NullString
		tx   int64
	}
	events := []eventDigest{}
	for rows.Next() {
		var event eventDigest
		if err := rows.Scan(&event.tx, &event.hash, &event.data, &event.op, &event.gid); err != nil {
			return 0, nil, finishRows(rows, wrap(ErrFormat, err, "cannot decode event hash"), "event hash rows")
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, finishRows(rows, wrap(ErrFormat, err, "cannot finish event hash enumeration"), "event hash rows")
	}
	if err := rows.Close(); err != nil {
		return 0, nil, wrap(ErrFormat, err, "cannot close event hash rows")
	}
	nullPayloads := map[string]int64{}
	redactedTargets := map[string][]int64{}
	for _, event := range events {
		if len(event.gid) != 16 {
			problems = append(problems, fmt.Sprintf("event %d has no 16-byte UUID identity", event.tx))
			continue
		}
		var uuid [16]byte
		copy(uuid[:], event.gid)
		eventID := formatUUID(uuid)
		if event.op.Valid {
			if operationErr := validateOperationID(event.op.String); operationErr != nil {
				problems = append(problems, fmt.Sprintf("event %d has an invalid operation id", event.tx))
			}
		}
		if !event.data.Valid {
			nullPayloads[eventID] = event.tx
			continue
		}
		record, decodeErr := decodeStoredEventData(event.data.String, event.hash)
		if decodeErr != nil {
			problems = append(problems, fmt.Sprintf("event %d payload is invalid: %v", event.tx, decodeErr))
			continue
		}
		if record["event"] != eventID {
			problems = append(problems, fmt.Sprintf("event %d payload names another event identity", event.tx))
			continue
		}
		var at int64
		if queryErr := runner.QueryRowContext(ctx, "SELECT v FROM fgraph_facts WHERE e=? AND a=1 AND tx=e", event.tx).Scan(&at); queryErr != nil {
			problems = append(problems, fmt.Sprintf("event %d cannot be reconstructed: missing timestamp", event.tx))
			continue
		}
		originAt := at
		var imported int64
		if queryErr := runner.QueryRowContext(ctx, "SELECT v FROM fgraph_facts WHERE e=? AND a=? AND tx=e", event.tx, importedAtAttrID).Scan(&imported); queryErr == nil {
			originAt = imported
		} else if !errors.Is(queryErr, sql.ErrNoRows) {
			problems = append(problems, fmt.Sprintf("event %d cannot read original timestamp: %v", event.tx, queryErr))
			continue
		}
		if record["at"] != originAt {
			problems = append(problems, fmt.Sprintf("event %d payload timestamp differs from its receipt", event.tx))
			continue
		}
		if redacted, redactedOK := record["redacted"].(bool); redactedOK && redacted {
			targets, valid := validateExcisionEventRecord(record)
			if !valid {
				problems = append(problems, fmt.Sprintf("event %d has a malformed redacted excision payload", event.tx))
				continue
			}
			var marker int
			if queryErr := runner.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM fgraph_facts WHERE e=? AND a=11 AND tx=e AND rx IS NULL)", event.tx).Scan(&marker); queryErr != nil || marker == 0 {
				problems = append(problems, fmt.Sprintf("event %d redaction has no live fgraph/excised audit marker", event.tx))
				continue
			}
			for _, target := range targets {
				redactedTargets[target] = append(redactedTargets[target], event.tx)
			}
			continue
		}
		if event.tx == GenesisTx {
			continue
		}
		reconstructed, reconstructErr := db.exportTransaction(ctx, runner, event.tx, at)
		if reconstructErr != nil {
			problems = append(problems, fmt.Sprintf("event %d cannot be reconstructed: %v", event.tx, reconstructErr))
			continue
		}
		reconstructedData, _, reconstructErr := canonicalEventData(reconstructed)
		if reconstructErr != nil {
			problems = append(problems, fmt.Sprintf("event %d cannot be canonicalized: %v", event.tx, reconstructErr))
			continue
		}
		if reconstructedData != event.data.String {
			mayLoseHistory, historyErr := db.eventMayLoseHistory(ctx, runner, record)
			if historyErr != nil {
				problems = append(problems, fmt.Sprintf("event %d history policy cannot be verified: %v", event.tx, historyErr))
			} else if !mayLoseHistory {
				problems = append(problems, fmt.Sprintf("event %d retained facts differ from its canonical payload", event.tx))
			}
		}
	}
	for target, redactors := range redactedTargets {
		targetTx, exists := nullPayloads[target]
		if !exists {
			problems = append(problems, fmt.Sprintf("redacted event target %s still has canonical event data", target))
			continue
		}
		for _, redactorTx := range redactors {
			if targetTx >= redactorTx {
				problems = append(problems, fmt.Sprintf("redaction event %d targets non-prior event %s", redactorTx, target))
			}
		}
	}
	for target := range nullPayloads {
		if len(redactedTargets[target]) == 0 {
			problems = append(problems, fmt.Sprintf("event %s has NULL payload without an audited excision", target))
		}
	}
	return int64(len(nullPayloads)), problems, nil
}

func validateExcisionEventRecord(record map[string]any) ([]string, bool) {
	if !exactKeys(record, "fgraph", "event", "at", "created", "asserted", "retracted", "redacted", "redacts") {
		return nil, false
	}
	if record["fgraph"] != "event/1" || record["redacted"] != true {
		return nil, false
	}
	for _, field := range []string{"created", "asserted", "retracted"} {
		values, ok := record[field].([]any)
		if !ok || len(values) != 0 {
			return nil, false
		}
	}
	raw, ok := record["redacts"].([]any)
	if !ok {
		return nil, false
	}
	targets := make([]string, 0, len(raw))
	for _, item := range raw {
		target, ok := item.(string)
		if !ok {
			return nil, false
		}
		if _, err := parseUUID(target); err != nil {
			return nil, false
		}
		if len(targets) > 0 && target <= targets[len(targets)-1] {
			return nil, false
		}
		targets = append(targets, target)
	}
	return targets, true
}

func eventSelectorKey(value any) (string, bool) {
	encoded, err := canonicalJSON(value)
	if err != nil {
		return "", false
	}
	return string(encoded), true
}

func eventValueReferencesSelector(value any, target string) bool {
	fields, ok := objectFields(value)
	if !ok || len(fields) != 1 || fields[0].Name != "ref" {
		return false
	}
	key, ok := eventSelectorKey(fields[0].Value)
	return ok && key == target
}

func eventRecordReferencesSelector(record map[string]any, target string) (bool, error) {
	created, ok := record["created"].([]any)
	if !ok {
		return false, fail(ErrFormat, "stored event has no created identity array")
	}
	for _, selector := range created {
		if key, valid := eventSelectorKey(selector); valid && key == target {
			return true, nil
		}
	}
	for _, field := range []string{"asserted", "retracted"} {
		tuples, ok := record[field].([]any)
		if !ok {
			return false, fail(ErrFormat, "stored event has no %s tuple array", field)
		}
		for _, item := range tuples {
			tuple, ok := item.([]any)
			if !ok || len(tuple) != 4 {
				return false, fail(ErrFormat, "stored event %s tuple is malformed", field)
			}
			for _, selector := range tuple[:2] {
				if key, valid := eventSelectorKey(selector); valid && key == target {
					return true, nil
				}
			}
			if eventValueReferencesSelector(tuple[2], target) {
				return true, nil
			}
		}
	}
	if raw, present := record["tx_facts"]; present {
		tuples, ok := raw.([]any)
		if !ok {
			return false, fail(ErrFormat, "stored event tx_facts is not a tuple array")
		}
		for _, item := range tuples {
			tuple, ok := item.([]any)
			if !ok || len(tuple) != 3 {
				return false, fail(ErrFormat, "stored event tx_facts tuple is malformed")
			}
			if key, valid := eventSelectorKey(tuple[0]); valid && key == target {
				return true, nil
			}
			if eventValueReferencesSelector(tuple[1], target) {
				return true, nil
			}
		}
	}
	return false, nil
}

func (db *DB) eventPayloadTransactionsForSelector(
	ctx context.Context,
	runner sqlRunner,
	selector any,
	before int64,
) (result map[int64]bool, resultErr error) {
	target, ok := eventSelectorKey(selector)
	if !ok {
		return nil, fail(ErrFormat, "cannot canonicalize excision identity selector")
	}
	rows, err := runner.QueryContext(ctx, `SELECT tx,event_hash,event_data FROM fgraph_events
		WHERE tx<? AND event_data IS NOT NULL ORDER BY tx`, before)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot enumerate retained event payloads for excision")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "excision event payload rows")) }()
	result = map[int64]bool{}
	for rows.Next() {
		var tx int64
		var eventHash []byte
		var eventData string
		if err := rows.Scan(&tx, &eventHash, &eventData); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode retained event payload for excision")
		}
		record, err := decodeStoredEventData(eventData, eventHash)
		if err != nil {
			return nil, wrap(ErrFormat, err, "cannot inspect retained event %d during excision", tx)
		}
		matched, err := eventRecordReferencesSelector(record, target)
		if err != nil {
			return nil, wrap(ErrFormat, err, "cannot inspect retained event %d during excision", tx)
		}
		if matched {
			result[tx] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish inspecting retained event payloads for excision")
	}
	return result, nil
}

func (db *DB) eventMayLoseHistory(ctx context.Context, runner sqlRunner, record map[string]any) (bool, error) {
	attributes := map[string]bool{}
	for _, field := range []string{"asserted", "retracted"} {
		tuples, tuplesOK := record[field].([]any)
		if !tuplesOK {
			continue
		}
		for _, raw := range tuples {
			tuple, ok := raw.([]any)
			if ok && len(tuple) == 4 {
				if attr, ok := tuple[1].(string); ok {
					attributes[attr] = true
				}
			}
		}
	}
	if tuples, tuplesOK := record["tx_facts"].([]any); tuplesOK {
		for _, raw := range tuples {
			tuple, ok := raw.([]any)
			if ok && len(tuple) == 3 {
				if attr, ok := tuple[0].(string); ok {
					attributes[attr] = true
				}
			}
		}
	}
	for attr := range attributes {
		var attrID int64
		if err := runner.QueryRowContext(ctx, "SELECT id FROM fgraph_ids WHERE name=?", attr).Scan(&attrID); err != nil {
			return false, wrap(ErrFormat, err, "cannot inspect history policy for %q", attr)
		}
		var nohistory int
		if err := runner.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fgraph_facts
			WHERE e=? AND ((a=7 AND t=1 AND v=1) OR (a=8 AND t IN (4,10) AND v='vector')))`, attrID).Scan(&nohistory); err != nil {
			return false, wrap(ErrFormat, err, "cannot inspect history policy for %q", attr)
		}
		if nohistory != 0 {
			return true, nil
		}
	}
	return false, nil
}

func (db *DB) countGlobalSchemaProblems(ctx context.Context, runner sqlRunner) (result int64, resultErr error) {
	rows, err := runner.QueryContext(ctx, "SELECT DISTINCT a FROM fgraph_facts ORDER BY a")
	if err != nil {
		return 0, wrap(ErrFormat, err, "cannot enumerate attributes for global schema validation")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "global schema attribute rows")) }()
	attributeIDs := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, finishRows(rows, wrap(ErrFormat, err, "cannot decode global schema attribute"), "global schema attribute rows")
		}
		attributeIDs = append(attributeIDs, id)
	}
	if err := rows.Err(); err != nil {
		return 0, finishRows(rows, wrap(ErrFormat, err, "cannot finish global schema attribute enumeration"), "global schema attribute rows")
	}
	if err := rows.Close(); err != nil {
		return 0, wrap(ErrFormat, err, "cannot close global schema attribute rows")
	}

	problems := int64(0)
	for _, attr := range attributeIDs {
		schema, err := db.schemaFor(ctx, runner, attr, nil)
		if err != nil {
			problems++
			continue
		}
		factRows, err := runner.QueryContext(ctx, "SELECT id,e,a,v,t,tx,rx FROM fgraph_facts WHERE a=? AND rx IS NULL ORDER BY id", attr)
		if err != nil {
			return 0, wrap(ErrFormat, err, "cannot read live facts for global schema attribute %d", attr)
		}
		facts, err := scanRawFacts(factRows)
		if err != nil {
			return 0, err
		}

		if schema.typeName != "" {
			_, declaredTypeValid := parseTagName(schema.typeName)
			declaredTypeValid = declaredTypeValid && schema.typeName != "text_ref" && schema.typeName != "bytes_ref"
			if declaredTypeValid {
				for _, fact := range facts {
					if !tagCompatible(schema.typeName, fact.t) {
						declaredTypeValid = false
						break
					}
				}
			}
			if !declaredTypeValid {
				problems++
			}
		}
		if !schema.many {
			counts := map[int64]int{}
			multiple := false
			for _, fact := range facts {
				counts[fact.e]++
				multiple = multiple || counts[fact.e] > 1
			}
			if multiple {
				problems++
			}
		}
		if schema.unique {
			if schema.typeName == "" || schema.typeName == "json" || schema.typeName == "vector" {
				problems++
			}
			owners := map[string]map[int64]bool{}
			duplicate := false
			for _, fact := range facts {
				key := fmt.Sprintf("%d:%v", fact.t, storageKey(fact.v))
				if owners[key] == nil {
					owners[key] = map[int64]bool{}
				}
				owners[key][fact.e] = true
				duplicate = duplicate || len(owners[key]) > 1
			}
			if duplicate {
				problems++
			}
		}
		if schema.dimsSet {
			dimsInvalid := schema.typeName != "vector" || schema.dims <= 0
			if !dimsInvalid {
				for _, fact := range facts {
					if fact.t != TagVector {
						continue
					}
					logical, logicalErr := db.logicalValue(ctx, runner, fact.v, fact.t)
					vector, ok := logical.([]float32)
					if logicalErr != nil || !ok || int64(len(vector)) != schema.dims {
						dimsInvalid = true
						break
					}
				}
			}
			if dimsInvalid {
				problems++
			}
		}
		if schema.vectorModel != "" && schema.typeName != "vector" {
			problems++
		}
	}
	return problems, nil
}

func (db *DB) countGlobalShapeViolations(ctx context.Context, runner sqlRunner) (result int64, resultErr error) {
	rows, err := runner.QueryContext(ctx, "SELECT DISTINCT e FROM fgraph_facts WHERE a=15 AND rx IS NULL ORDER BY e")
	if err != nil {
		return 0, wrap(ErrFormat, err, "cannot enumerate shaped entities for global validation")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "shaped entity rows")) }()
	entities := []int64{}
	for rows.Next() {
		var entity int64
		if err := rows.Scan(&entity); err != nil {
			return 0, finishRows(rows, wrap(ErrFormat, err, "cannot decode shaped entity"), "shaped entity rows")
		}
		entities = append(entities, entity)
	}
	if err := rows.Err(); err != nil {
		return 0, finishRows(rows, wrap(ErrFormat, err, "cannot finish shaped entity enumeration"), "shaped entity rows")
	}
	if err := rows.Close(); err != nil {
		return 0, wrap(ErrFormat, err, "cannot close shaped entity rows")
	}
	violations := int64(0)
	for _, entity := range entities {
		issues, err := db.shapeIssues(ctx, runner, map[int64]bool{entity: true})
		if err != nil {
			violations++
			continue
		}
		violations += int64(len(issues))
	}
	return violations, nil
}

func countInvalidSystemIdentities(ctx context.Context, runner sqlRunner) (count int64, resultErr error) {
	rows, err := runner.QueryContext(ctx,
		"SELECT id,CAST(name AS BLOB),gid,created_tx FROM fgraph_ids WHERE id BETWEEN 1 AND ? ORDER BY id",
		len(systemNames)-1,
	)
	if err != nil {
		return 0, wrap(ErrFormat, err, "cannot validate system identity mapping")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "system identity rows")) }()
	actual := map[int64][]byte{}
	for rows.Next() {
		var id int64
		var name []byte
		var gid []byte
		var createdTx int64
		if scanErr := rows.Scan(&id, &name, &gid, &createdTx); scanErr != nil {
			return 0, wrap(ErrFormat, scanErr, "cannot decode system identity mapping")
		}
		if len(gid) != 0 || createdTx != GenesisTx {
			count++
		}
		actual[id] = name
	}
	if iterationErr := rows.Err(); iterationErr != nil {
		return 0, wrap(ErrFormat, iterationErr, "cannot finish validating system identity mapping")
	}
	for id := int64(1); id < int64(len(systemNames)); id++ {
		if !bytes.Equal(actual[id], []byte(systemNames[id])) {
			count++
		}
	}
	return count, nil
}

type genesisFactValue struct {
	storageClass string
	value        []byte
	rx           sql.NullInt64
	e            int64
	a            int64
	tag          int64
	tx           int64
}

func countInvalidGenesisFacts(ctx context.Context, runner sqlRunner) (count int64, resultErr error) {
	lastFactID := int64(GenesisFactCount)
	rows, err := runner.QueryContext(ctx, `SELECT id,e,a,CAST(v AS BLOB),typeof(v),t,tx,rx
		FROM fgraph_facts WHERE id BETWEEN 2 AND ? OR (tx=? AND id NOT BETWEEN 1 AND ?) ORDER BY id`,
		lastFactID, GenesisTx, lastFactID,
	)
	if err != nil {
		return 0, wrap(ErrFormat, err, "cannot validate immutable genesis facts")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "genesis fact rows")) }()
	actual := map[int64]genesisFactValue{}
	for rows.Next() {
		var id int64
		var fact genesisFactValue
		if scanErr := rows.Scan(
			&id,
			&fact.e,
			&fact.a,
			&fact.value,
			&fact.storageClass,
			&fact.tag,
			&fact.tx,
			&fact.rx,
		); scanErr != nil {
			return 0, wrap(ErrFormat, scanErr, "cannot decode immutable genesis fact")
		}
		actual[id] = fact
	}
	if iterationErr := rows.Err(); iterationErr != nil {
		return 0, wrap(ErrFormat, iterationErr, "cannot finish validating immutable genesis facts")
	}
	for id := int64(1); id < int64(len(systemTypes)); id++ {
		factID := id + 1
		if !matchesGenesisFact(actual[factID], id, 8, systemTypes[id]) {
			count++
		}
		delete(actual, factID)
	}
	for id := int64(1); id < int64(len(systemDocs)); id++ {
		factID := id + int64(len(systemTypes))
		if !matchesGenesisFact(actual[factID], id, 10, systemDocs[id]) {
			count++
		}
		delete(actual, factID)
	}
	for factID, entity := range map[int64]int64{38: 16, 39: 17} {
		fact := actual[factID]
		if fact.e != entity || fact.a != 5 || !bytes.Equal(fact.value, []byte("1")) || fact.storageClass != "integer" || fact.tag != int64(TagBool) || fact.tx != GenesisTx || fact.rx.Valid {
			count++
		}
		delete(actual, factID)
	}
	return count + int64(len(actual)), nil
}

func matchesGenesisFact(fact genesisFactValue, entity, attribute int64, value string) bool {
	return fact.e == entity && fact.a == attribute && bytes.Equal(fact.value, []byte(value)) &&
		fact.storageClass == "text" && fact.tag == int64(TagText) && fact.tx == GenesisTx && !fact.rx.Valid
}

func countInvalidPhysicalValues(
	ctx context.Context,
	runner sqlRunner,
) (invalidTags, invalidValues int64, resultErr error) {
	rows, err := runner.QueryContext(ctx, `SELECT t,typeof(v),
		CASE WHEN t IN (4,10) THEN NULL ELSE v END,CAST(v AS BLOB) FROM fgraph_facts ORDER BY id`)
	if err != nil {
		return 0, 0, wrap(ErrFormat, err, "cannot validate physical fact values")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "physical value rows")) }()
	for rows.Next() {
		var tag int64
		var storageClass string
		var scalar any
		var raw []byte
		if scanErr := rows.Scan(&tag, &storageClass, &scalar, &raw); scanErr != nil {
			return 0, 0, wrap(ErrFormat, scanErr, "cannot decode physical fact value")
		}
		if tag < int64(TagRef) || tag > int64(TagJSON) {
			invalidTags++
			continue
		}
		if !validPhysicalValue(Tag(tag), storageClass, scalar, raw) {
			invalidValues++
		}
	}
	if iterationErr := rows.Err(); iterationErr != nil {
		return 0, 0, wrap(ErrFormat, iterationErr, "cannot finish validating physical fact values")
	}
	return invalidTags, invalidValues, nil
}

func validPhysicalValue(tag Tag, storageClass string, scalar any, raw []byte) bool {
	switch tag {
	case TagRef:
		value, ok := scalar.(int64)
		return storageClass == "integer" && ok && value > 0
	case TagBool:
		value, ok := scalar.(int64)
		return storageClass == "integer" && ok && (value == 0 || value == 1)
	case TagInt:
		_, ok := scalar.(int64)
		return storageClass == "integer" && ok
	case TagFloat:
		value, ok := scalar.(float64)
		return storageClass == "real" && ok && !math.IsNaN(value) && !math.IsInf(value, 0)
	case TagText:
		return storageClass == "text" && len(raw) <= BlobThreshold && utf8.Valid(raw)
	case TagInstant:
		value, ok := scalar.(int64)
		return storageClass == "integer" && ok && value >= minInstantMicros && value <= maxInstantMicros
	case TagBytes:
		return storageClass == "blob" && len(raw) <= BlobThreshold
	case TagVector, TagTextRef, TagBytesRef:
		// The indirect validator owns key, blob-domain, length, and digest checks.
		return true
	case TagJSON:
		if storageClass != "text" || len(raw) > MaxValueBytes || !utf8.Valid(raw) {
			return false
		}
		decoded, err := decodeInternalJSON(bytes.NewReader(raw))
		if err != nil {
			return false
		}
		canonical, err := canonicalJSON(decoded)
		return err == nil && bytes.Equal(canonical, raw)
	default:
		return false
	}
}

func countInvalidIndirectBlobs(ctx context.Context, runner sqlRunner) (count int64, resultErr error) {
	rows, err := runner.QueryContext(ctx, `SELECT b.rowid,b.hash,b.data,f.t FROM fgraph_blobs b
		JOIN fgraph_facts f ON f.t IN (7,8,9) AND f.v=b.hash ORDER BY b.rowid,f.t`)
	if err != nil {
		return 0, wrap(ErrFormat, err, "cannot validate indirect blob contents")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "indirect blob rows")) }()
	invalid := map[int64]bool{}
	for rows.Next() {
		var id int64
		var key, data any
		var tag int64
		if scanErr := rows.Scan(&id, &key, &data, &tag); scanErr != nil {
			return 0, wrap(ErrFormat, scanErr, "cannot decode indirect blob")
		}
		if !validIndirectBlob(Tag(tag), key, data) {
			invalid[id] = true
		}
	}
	if iterationErr := rows.Err(); iterationErr != nil {
		return 0, wrap(ErrFormat, iterationErr, "cannot finish validating indirect blobs")
	}
	return int64(len(invalid)), nil
}

func validIndirectBlob(tag Tag, rawKey, data any) bool {
	key, ok := rawKey.([]byte)
	if !ok || len(key) != 32 {
		return false
	}
	var raw []byte
	switch tag {
	case TagTextRef:
		text, valid := data.(string)
		if !valid || !utf8.ValidString(text) {
			return false
		}
		raw = []byte(text)
		if len(raw) <= BlobThreshold || len(raw) > MaxValueBytes {
			return false
		}
	case TagBytesRef:
		value, valid := data.([]byte)
		if !valid || len(value) <= BlobThreshold || len(value) > MaxValueBytes {
			return false
		}
		raw = value
	case TagVector:
		value, valid := data.([]byte)
		if !valid || len(value) == 0 || len(value) > MaxValueBytes || len(value)%4 != 0 {
			return false
		}
		raw = value
	default:
		return false
	}
	digest := indirectDigest(tag, raw)
	return bytes.Equal(key, digest[:])
}

func doctorCount(ctx context.Context, runner sqlRunner, query string, args ...any) (int64, error) {
	var count int64
	if err := runner.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, wrap(ErrFormat, err, "cannot validate identifier invariants")
	}
	return count, nil
}

func readGenesisReceipt(
	ctx context.Context,
	runner sqlRunner,
) (count int, at int64, valid bool, resultErr error) {
	rows, queryErr := runner.QueryContext(
		ctx,
		"SELECT v,t,tx,rx FROM fgraph_facts WHERE e=? AND a=? ORDER BY id",
		GenesisTx,
		int64(1),
	)
	if queryErr != nil {
		return 0, 0, false, wrap(ErrFormat, queryErr, "cannot validate the genesis receipt")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "genesis receipt rows")) }()
	for rows.Next() {
		count++
		var value any
		var tag, tx int64
		var rx sql.NullInt64
		if scanErr := rows.Scan(&value, &tag, &tx, &rx); scanErr != nil {
			return 0, 0, false, wrap(ErrFormat, scanErr, "cannot decode the genesis receipt")
		}
		at, valid = value.(int64)
		valid = valid && tag == int64(TagInstant) && tx == GenesisTx && !rx.Valid
	}
	if iterationErr := rows.Err(); iterationErr != nil {
		return 0, 0, false, wrap(ErrFormat, iterationErr, "cannot finish validating the genesis receipt")
	}
	return count, at, valid, nil
}

type ftsRow struct {
	text string
	id   int64
}

func readFTSRows(ctx context.Context, runner sqlRunner) (result []ftsRow, resultErr error) {
	rows, err := runner.QueryContext(ctx, "SELECT rowid,text FROM fgraph_fts ORDER BY rowid")
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot read the full-text index")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "full-text index rows")) }()
	result = []ftsRow{}
	for rows.Next() {
		var item ftsRow
		if err := rows.Scan(&item.id, &item.text); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode the full-text index")
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish reading the full-text index")
	}
	return result, nil
}

func (db *DB) readExpectedFTSRows(ctx context.Context, runner sqlRunner) (result []ftsRow, resultErr error) {
	rows, err := runner.QueryContext(ctx, `SELECT f.id,CASE WHEN f.t=8 THEN b.data ELSE f.v END
		FROM fgraph_facts f LEFT JOIN fgraph_blobs b ON b.hash=f.v AND f.t=8
		WHERE f.rx IS NULL AND f.t IN (4,8) ORDER BY f.id`)
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot read live text facts")
	}
	defer func() { resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "live text fact rows")) }()
	result = []ftsRow{}
	for rows.Next() {
		var id int64
		var value any
		if err := rows.Scan(&id, &value); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode live text fact")
		}
		text, ok := value.(string)
		if !ok {
			return nil, fail(ErrFormat, "text fact %d has logical type %T", id, value)
		}
		result = append(result, ftsRow{id: id, text: text})
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish reading live text facts")
	}
	return result, nil
}

func equalFTSRows(left, right []ftsRow) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func doctorValue(value any) string {
	if value == nil {
		return "None"
	}
	if text, ok := value.(string); ok {
		return fmt.Sprintf("%q", text)
	}
	return fmt.Sprintf("%v", value)
}

func readIntegrityCheck(ctx context.Context, conn *sql.Conn) (messages []string, resultErr error) {
	rows, err := conn.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return nil, wrap(ErrFormat, err, "cannot run SQLite integrity_check")
	}
	defer func() {
		resultErr = joinErrors(resultErr, wrapClose(rows.Close(), "integrity result rows"))
	}()
	for rows.Next() {
		var message string
		if err := rows.Scan(&message); err != nil {
			return nil, wrap(ErrFormat, err, "cannot decode SQLite integrity result")
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap(ErrFormat, err, "cannot finish SQLite integrity_check")
	}
	return messages, nil
}

func (db *DB) Excise(ctx context.Context, ref any, options ...TxOption) (result TxReport, resultErr error) {
	if err := db.checkUsable(true); err != nil {
		return TxReport{}, err
	}
	config := txOptions{}
	for _, option := range options {
		if option == nil {
			return TxReport{}, fail(ErrType, "excision option is nil; pass WithOperationID and IfBasis values directly")
		}
		option(&config)
	}
	if config.by != nil || config.source != nil || config.metaSet || config.txFactsSet || config.at != nil ||
		config.declaration != nil || config.force || config.eventID != nil || config.eventHash != nil ||
		config.originAt != nil || len(config.preallocated) != 0 {
		return TxReport{}, fail(ErrType, "excision only accepts WithOperationID and IfBasis options")
	}
	if config.operationID != nil {
		if err := validateOperationID(*config.operationID); err != nil {
			return TxReport{}, err
		}
	}
	requestJSON, requestErr := canonicalJSON(map[string]any{"operation": "excise", "ref": wireValue(ref)})
	if requestErr != nil {
		return TxReport{}, wrap(ErrType, requestErr, "cannot canonicalize excision request")
	}
	requestHash := sha256.Sum256(requestJSON)
	db.store.mu.Lock()
	defer db.store.mu.Unlock()
	conn, connErr := db.store.sql.Conn(ctx)
	if connErr != nil {
		return TxReport{}, wrap(ErrFormat, connErr, "cannot acquire SQLite writer for excision")
	}
	committed := false
	defer func() {
		closeErr := wrapClose(conn.Close(), "excision database connection")
		if !committed {
			resultErr = joinErrors(resultErr, closeErr)
		}
	}()
	if beginErr := func() error {
		_, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE")
		return err
	}(); beginErr != nil {
		return TxReport{}, wrap(ErrConflict, beginErr, "cannot acquire the single-writer lock for excision")
	}
	defer func() {
		if !committed {
			resultErr = joinErrors(resultErr, rollbackSQLite(conn, "entity excision"))
		}
	}()
	if refreshErr := db.store.refreshNames(ctx, conn); refreshErr != nil {
		return TxReport{}, refreshErr
	}
	basis, basisErr := db.basisOn(ctx, conn)
	if basisErr != nil {
		return TxReport{}, basisErr
	}
	// Receipt lookup intentionally precedes CAS: an exact retry must succeed
	// even though the original commit made its supplied basis stale.
	if config.operationID != nil {
		prior, found, receiptErr := db.operationReceipt(ctx, conn, *config.operationID, requestHash)
		if receiptErr != nil {
			return TxReport{}, receiptErr
		}
		if found {
			if _, rollbackErr := conn.ExecContext(ctx, "ROLLBACK"); rollbackErr != nil {
				return TxReport{}, wrap(ErrFormat, rollbackErr, "cannot close idempotent excision retry")
			}
			committed = true
			return prior, nil
		}
	}
	if config.ifBasisTx != nil && *config.ifBasisTx != basis {
		return TxReport{}, fail(ErrConflict, "basis is %d, not requested %d; refresh state and retry the excision", basis, *config.ifBasisTx)
	}
	entity, found, resolveErr := db.resolveReadEntity(ctx, conn, ref)
	if resolveErr != nil {
		return TxReport{}, resolveErr
	}
	if !found {
		return TxReport{}, fail(ErrNotFound, "entity %v does not exist; use a known name or id", ref)
	}
	if entity <= GenesisTx {
		return TxReport{}, fail(ErrUnsupported, "system entity %v cannot be excised; retract application facts instead", ref)
	}
	var transaction int
	if classifyErr := conn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM fgraph_events WHERE tx=?)", entity).Scan(&transaction); classifyErr != nil {
		return TxReport{}, wrap(ErrFormat, classifyErr, "cannot classify entity %v for excision", ref)
	}
	if transaction != 0 {
		return TxReport{}, fail(ErrUnsupported, "transaction entity %v cannot be excised; history must retain its audit receipt", ref)
	}
	alloc, allocatorErr := newAllocator(ctx, conn, db.store)
	if allocatorErr != nil {
		return TxReport{}, allocatorErr
	}
	tx, txErr := alloc.tx()
	if txErr != nil {
		return TxReport{}, txErr
	}
	at, atErr := db.nextTimestamp(ctx, conn, nil)
	if atErr != nil {
		return TxReport{}, atErr
	}
	eventID, eventIDErr := db.store.nextEventID(tx)
	if eventIDErr != nil {
		return TxReport{}, eventIDErr
	}
	eventUUID, uuidErr := parseUUID(eventID)
	if uuidErr != nil {
		return TxReport{}, uuidErr
	}
	if eventUUID[6]>>4 != 4 || eventUUID[8]&0xc0 != 0x80 {
		return TxReport{}, fail(ErrType, "event id %q is not an RFC 9562 UUIDv4", eventID)
	}
	if finalizeErr := alloc.finalize(ctx, tx, eventUUID); finalizeErr != nil {
		return TxReport{}, finalizeErr
	}
	rows, rowsErr := conn.QueryContext(ctx, `SELECT id,e,a,v,t,tx,rx FROM fgraph_facts
		WHERE e=? OR a=? OR (t=0 AND v=?) ORDER BY id`, entity, entity, entity)
	if rowsErr != nil {
		return TxReport{}, wrap(ErrFormat, rowsErr, "cannot enumerate facts for entity %v", ref)
	}
	facts, factsErr := scanRawFacts(rows)
	if factsErr != nil {
		return TxReport{}, factsErr
	}
	retracted := make([]Fact, 0, len(facts))
	redactedTransactions := map[int64]bool{}
	for _, fact := range facts {
		redactedTransactions[fact.tx] = true
		if fact.rx.Valid {
			redactedTransactions[fact.rx.Int64] = true
		}
		rendered, renderErr := db.renderRaw(ctx, conn, fact, &tx)
		if renderErr != nil {
			return TxReport{}, renderErr
		}
		retracted = append(retracted, rendered)
		if fact.t == TagText || fact.t == TagTextRef {
			if _, err := conn.ExecContext(ctx, "DELETE FROM fgraph_fts WHERE rowid=?", fact.id); err != nil {
				return TxReport{}, wrap(ErrFormat, err, "cannot remove excised fact %d from FTS", fact.id)
			}
		}
	}
	selector, selectorErr := db.identitySelector(ctx, conn, entity)
	if selectorErr != nil {
		return TxReport{}, selectorErr
	}
	payloadTransactions, payloadErr := db.eventPayloadTransactionsForSelector(ctx, conn, selector, tx)
	if payloadErr != nil {
		return TxReport{}, payloadErr
	}
	for priorTx := range payloadTransactions {
		redactedTransactions[priorTx] = true
	}
	redacts := make([]string, 0, len(redactedTransactions))
	for priorTx := range redactedTransactions {
		priorEvent, priorErr := db.eventIDForTx(ctx, conn, priorTx)
		if priorErr != nil {
			return TxReport{}, priorErr
		}
		redacts = append(redacts, priorEvent)
		if _, err := conn.ExecContext(ctx, "UPDATE fgraph_events SET event_data=NULL WHERE tx=?", priorTx); err != nil {
			return TxReport{}, wrap(ErrFormat, err, "cannot redact prior event %s during excision", priorEvent)
		}
	}
	sort.Strings(redacts)
	redactValues := make([]any, len(redacts))
	for index, redactedEvent := range redacts {
		redactValues[index] = redactedEvent
	}
	if _, err := conn.ExecContext(ctx, "DELETE FROM fgraph_facts WHERE e=? OR a=? OR (t=0 AND v=?)", entity, entity, entity); err != nil {
		return TxReport{}, wrap(ErrFormat, err, "cannot physically excise entity %v", ref)
	}
	if _, err := conn.ExecContext(ctx, `DELETE FROM fgraph_blobs WHERE NOT EXISTS (
		SELECT 1 FROM fgraph_facts WHERE t IN (7,8,9) AND v=fgraph_blobs.hash
	)`); err != nil {
		return TxReport{}, wrap(ErrFormat, err, "cannot garbage-collect blobs after excision")
	}
	metadata := []plannedFact{
		{e: tx, a: 1, attr: systemNames[1], value: storedValue{logical: at, storage: at, tag: TagInstant}},
		{e: tx, a: 11, attr: systemNames[11], value: storedValue{logical: entity, storage: entity, tag: TagRef}},
	}
	asserted := make([]Fact, 0, len(metadata))
	for _, fact := range metadata {
		id, insertErr := db.insertFact(ctx, conn, fact, tx)
		if insertErr != nil {
			return TxReport{}, insertErr
		}
		asserted = append(asserted, db.renderPlanned(id, fact, tx, nil, alloc))
	}
	if flushErr := alloc.flush(ctx); flushErr != nil {
		return TxReport{}, flushErr
	}
	report := TxReport{Status: "applied", Tx: tx, At: at, EventID: eventID, BasisTx: basis, IDs: map[string]int64{}, Asserted: asserted, Retracted: retracted}
	// Excision is deliberately non-replicable: its durable event proves a
	// redaction occurred without retaining the erased values in the receipt.
	eventRecord := map[string]any{
		"fgraph": "event/1", "event": eventID, "at": at,
		"created": []any{}, "asserted": []any{}, "retracted": []any{},
		"redacted": true, "redacts": redactValues,
	}
	eventData, eventHash, eventDataErr := canonicalEventData(eventRecord)
	if eventDataErr != nil {
		return TxReport{}, eventDataErr
	}
	var operationID any
	var storedRequest any
	if config.operationID != nil {
		operationID = *config.operationID
		storedRequest = requestHash[:]
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO fgraph_events(tx,event_hash,event_data,operation_id,request_hash) VALUES (?,?,?,?,?)", tx, eventHash[:], eventData, operationID, storedRequest); err != nil {
		return TxReport{}, wrap(ErrFormat, err, "cannot record excision event %s", eventID)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return TxReport{}, wrap(ErrFormat, err, "cannot commit entity excision")
	}
	committed = true
	db.store.dataVersion = -1
	return report, nil
}
