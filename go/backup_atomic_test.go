package fgraph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"modernc.org/sqlite"
)

func TestBackupRejectsEveryExistingTargetWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := fixedDB(t, filepath.Join(dir, "source.db"))

	for name, content := range map[string][]byte{
		"empty.db":    {},
		"occupied.db": []byte("preserve me"),
	} {
		t.Run(name, func(t *testing.T) {
			target := filepath.Join(dir, name)
			if err := os.WriteFile(target, content, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := db.Backup(ctx, target); !errors.Is(err, ErrConflict) {
				t.Fatalf("Backup() error = %v, want Conflict", err)
			}
			got, err := fs.ReadFile(os.DirFS(dir), name)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, content) {
				t.Fatalf("existing target changed: got %q, want %q", got, content)
			}
		})
	}
}

func TestBackupVerificationFailureLeavesNoPublishedOrTemporaryFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := fixedDB(t, filepath.Join(dir, "source.db"))
	if _, err := db.store.sql.ExecContext(ctx,
		"INSERT INTO fgraph_blobs(hash,data) VALUES (?,?)",
		bytes.Repeat([]byte{0x42}, sha256.Size), []byte("orphan")); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(dir, "rejected.db")
	if err := db.Backup(ctx, target); !errors.Is(err, ErrFormat) {
		t.Fatalf("Backup() error = %v, want FormatError", err)
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed backup published target: %v", err)
	}
	temporary, err := filepath.Glob(filepath.Join(dir, ".rejected.db.*.fgraph-backup"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temporary) != 0 {
		t.Fatalf("failed backup left temporary files: %v", temporary)
	}
}

func TestBackupFromReadOnlyConnectionIsVerified(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	writable := fixedDB(t, sourcePath)
	if _, err := writable.Transact(ctx, E{"id": "preserved", "item/value": "yes"}); err != nil {
		t.Fatal(err)
	}
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}

	readOnly, openErr := Open(sourcePath, WithReadOnly())
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { closeTest(t, readOnly) })
	target := filepath.Join(dir, "backup.db")
	if err := readOnly.Backup(ctx, target); err != nil {
		t.Fatalf("Backup() from read-only connection: %v", err)
	}

	backup, openErr := Open(target, WithReadOnly())
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { closeTest(t, backup) })
	if report, err := backup.Doctor(ctx); err != nil || !report.OK {
		t.Fatalf("backup Doctor() = %+v, %v", report, err)
	}
	entity, err := backup.Entity(ctx, "preserved")
	if err != nil || entity["item/value"] != "yes" {
		t.Fatalf("backup Entity() = %#v, %v", entity, err)
	}
}

func TestCanceledBackupLeavesNoArtifacts(t *testing.T) {
	dir := t.TempDir()
	db := fixedDB(t, filepath.Join(dir, "source.db"))
	target := filepath.Join(dir, "backup.db")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := db.Backup(ctx, target); err == nil {
		t.Fatal("Backup() with canceled context succeeded")
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled backup published target: %v", err)
	}
	temporary, err := filepath.Glob(filepath.Join(dir, ".backup.db.*.fgraph-backup"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temporary) != 0 {
		t.Fatalf("canceled backup left temporary files: %v", temporary)
	}
}

func TestBackupOnNilDatabaseReturnsTypedError(t *testing.T) {
	var db *DB
	target := filepath.Join(t.TempDir(), "backup.db")
	if err := db.Backup(context.Background(), target); !errors.Is(err, ErrFormat) {
		t.Fatalf("Backup() error = %v, want FormatError", err)
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("nil-database backup created a target: %v", err)
	}
}

func TestBackupPublicationPrimitiveIsAtomicAndNoOverwrite(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "temporary.db")
	if err := os.WriteFile(source, []byte("verified backup"), 0o600); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dir, "published.db")
	if err := publishBackup(source, destination); err != nil {
		t.Fatal(err)
	}
	if got, err := fs.ReadFile(os.DirFS(dir), filepath.Base(destination)); err != nil || string(got) != "verified backup" {
		t.Fatalf("published backup = %q, %v", got, err)
	}

	occupied := filepath.Join(dir, "occupied.db")
	if err := os.WriteFile(occupied, []byte("existing data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := publishBackup(source, occupied); !errors.Is(err, ErrConflict) {
		t.Fatalf("occupied publication error = %v", err)
	}
	if got, err := fs.ReadFile(os.DirFS(dir), filepath.Base(occupied)); err != nil || string(got) != "existing data" {
		t.Fatalf("occupied destination changed to %q: %v", got, err)
	}
	if err := publishBackup(filepath.Join(dir, "missing.db"), filepath.Join(dir, "never.db")); !errors.Is(err, ErrFormat) {
		t.Fatalf("missing-source publication error = %v", err)
	}
}

func TestBackupDurabilitySubstepsFailClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := verifyBackup(ctx, filepath.Join(t.TempDir(), "unused.db")); !errors.Is(err, ErrFormat) {
		t.Fatalf("canceled verification error = %v", err)
	}

	dir := t.TempDir()
	invalid := filepath.Join(dir, "invalid.db")
	if err := os.WriteFile(invalid, []byte("not SQLite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verifyBackup(context.Background(), invalid); !errors.Is(err, ErrFormat) {
		t.Fatalf("invalid backup verification error = %v", err)
	}
	missing := filepath.Join(dir, "missing")
	if err := syncBackupFile(missing); !errors.Is(err, ErrFormat) {
		t.Fatalf("missing file synchronization error = %v", err)
	}
	if runtime.GOOS != "windows" {
		if err := syncBackupDirectory(missing); !errors.Is(err, ErrFormat) {
			t.Fatalf("missing directory synchronization error = %v", err)
		}
	}

	db := fixedDB(t, ":memory:")
	if err := db.copyOnlineBackup(context.Background(), dir); !errors.Is(err, ErrFormat) {
		t.Fatalf("directory online-backup target error = %v", err)
	}
}

func TestBackupFileSyncUsesWriteCapableHandle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup.db")
	if err := os.WriteFile(path, []byte("verified backup"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := openBackupFileForSync(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			t.Errorf("close synchronization handle: %v", closeErr)
		}
	}()
	// Rewriting the same byte proves the handle carries write access without
	// changing the fixture's logical contents.
	if _, err := file.WriteAt([]byte("v"), 0); err != nil {
		t.Fatalf("backup synchronization handle is read-only: %v", err)
	}
}

func TestOnlineBackupAllowsSecondHandleCommitBetweenSteps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	source := fixedDB(t, sourcePath)
	items := make([]any, 0, 512)
	for index := range 512 {
		items = append(items, E{
			"id":         fmt.Sprintf("backup/item-%d", index),
			"item/value": fmt.Sprintf("%04d-%s", index, strings.Repeat("x", 4096)),
		})
	}
	if _, err := source.Transact(ctx, items); err != nil {
		t.Fatal(err)
	}
	writer, err := Open(sourcePath, WithClock(func() int64 { return 1_767_225_600_000_001 }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTest(t, writer) })

	reached := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	step := func(backup *sqlite.Backup, pages int32) (bool, error) {
		more, stepErr := backup.Step(pages)
		if stepErr == nil && more {
			once.Do(func() {
				close(reached)
				<-resume
			})
		}
		return more, stepErr
	}

	target := filepath.Join(dir, "backup.db")
	done := make(chan error, 1)
	go func() { done <- source.backup(ctx, target, step) }()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		close(resume)
		t.Fatal("online backup did not expose a multi-step snapshot boundary")
	}
	_, writeErr := writer.Transact(ctx, E{"id": "backup/during", "item/value": "during"})
	close(resume)
	if writeErr != nil {
		t.Fatalf("second-handle transaction during backup: %v", writeErr)
	}
	if backupErr := <-done; backupErr != nil {
		t.Fatalf("Backup() after concurrent commit: %v", backupErr)
	}

	backup, err := Open(target, WithReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, backup)
	entity, err := backup.Entity(ctx, "backup/during")
	if err != nil || entity["item/value"] != "during" {
		t.Fatalf("backup concurrent entity = %#v, %v", entity, err)
	}
	if report, err := backup.Doctor(ctx); err != nil || !report.OK {
		t.Fatalf("backup Doctor() = %+v, %v", report, err)
	}
}

func TestBackupRejectsUnavailableOnlineBackupSources(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// A driver without SQLite's online-backup extension must fail explicitly;
	// silently falling back to a live file copy could produce a torn database.
	runner := openScriptedSQL(t, scriptedSQL{})
	unsupported := &DB{store: &store{sql: runner}}
	if err := unsupported.copyOnlineBackup(ctx, filepath.Join(dir, "unsupported.db")); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unsupported online-backup source error = %v", err)
	}

	closed := openScriptedSQL(t, scriptedSQL{})
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	unavailable := &DB{store: &store{sql: closed}}
	if err := unavailable.copyOnlineBackup(ctx, filepath.Join(dir, "closed.db")); !errors.Is(err, ErrFormat) {
		t.Fatalf("closed online-backup source error = %v", err)
	}
}
