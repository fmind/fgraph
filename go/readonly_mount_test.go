package fgraph

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func chmodOpenFile(t *testing.T, path string, mode os.FileMode) *os.File {
	t.Helper()
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := root.Open(filepath.Base(path))
	if closeErr := root.Close(); closeErr != nil {
		if handle != nil {
			if handleCloseErr := handle.Close(); handleCloseErr != nil {
				t.Errorf("close %q after root close failure: %v", path, handleCloseErr)
			}
		}
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = handle.Chmod(mode); err != nil {
		if closeErr := handle.Close(); closeErr != nil {
			t.Errorf("close %q after chmod failure: %v", path, closeErr)
		}
		t.Fatal(err)
	}
	return handle
}

func restoreOpenFileMode(t *testing.T, handle *os.File, mode os.FileMode) {
	t.Helper()
	if err := handle.Chmod(mode); err != nil {
		t.Errorf("restore %q mode: %v", handle.Name(), err)
	}
	if err := handle.Close(); err != nil {
		t.Errorf("close %q after mode restore: %v", handle.Name(), err)
	}
}

func TestReadOnlyOpenOnReadOnlyStorage(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX directory modes are required")
	}
	directory := filepath.Join(t.TempDir(), "media")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "graph.db")
	db, err := Open(path, WithClock(func() int64 { return 1_767_225_600_000_000 }))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err = db.Transact(ctx, E{"id": "readonly/item", "readonly/value": "available"}); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	pathHandle := chmodOpenFile(t, path, 0o444)
	directoryHandle := chmodOpenFile(t, directory, 0o555)
	t.Cleanup(func() {
		restoreOpenFileMode(t, directoryHandle, 0o700)
		restoreOpenFileMode(t, pathHandle, 0o600)
	})

	probe, probeErr := os.CreateTemp(directory, "write-probe")
	if probeErr == nil {
		if closeErr := probe.Close(); closeErr != nil {
			t.Errorf("close unexpected write probe: %v", closeErr)
		}
		t.Fatal("read-only fixture directory accepted a sidecar write")
	}
	if !errors.Is(probeErr, os.ErrPermission) {
		t.Fatalf("write probe error = %v, want permission denied", probeErr)
	}

	reader, err := Open(path, WithReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, reader)
	entity, err := reader.Entity(ctx, "readonly/item")
	if err != nil {
		t.Fatal(err)
	}
	if entity["readonly/value"] != "available" {
		t.Fatalf("read-only entity = %#v", entity)
	}
	report, err := reader.Doctor(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK {
		t.Fatalf("read-only doctor = %+v", report)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "graph.db" {
		t.Fatalf("read-only open created sidecars: %v", entries)
	}
}

func TestReadOnlyOpenNeverIgnoresExistingWAL(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX directory modes are required")
	}
	directory := filepath.Join(t.TempDir(), "media")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "graph.db")
	writer, err := Open(path, WithClock(func() int64 { return 1_767_225_600_000_000 }))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err = writer.store.sql.ExecContext(ctx, "PRAGMA wal_autocheckpoint = 0"); err != nil {
		t.Fatal(err)
	}
	if _, err = writer.store.sql.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Transact(ctx, E{"id": "wal/item", "wal/value": "uncheckpointed"}); err != nil {
		t.Fatal(err)
	}
	wal, shm := path+"-wal", path+"-shm"
	for _, entry := range []string{wal, shm} {
		if info, statErr := os.Stat(entry); statErr != nil || !info.Mode().IsRegular() {
			t.Fatalf("WAL sidecar %q = %v, %v", entry, info, statErr)
		}
	}
	pathHandle := chmodOpenFile(t, path, 0o444)
	walHandle := chmodOpenFile(t, wal, 0o444)
	shmHandle := chmodOpenFile(t, shm, 0o444)
	directoryHandle := chmodOpenFile(t, directory, 0o555)
	t.Cleanup(func() {
		restoreOpenFileMode(t, directoryHandle, 0o700)
		restoreOpenFileMode(t, pathHandle, 0o600)
		restoreOpenFileMode(t, walHandle, 0o600)
		restoreOpenFileMode(t, shmHandle, 0o600)
		closeTest(t, writer)
	})

	reader, openErr := Open(path, WithReadOnly())
	if openErr != nil {
		if !errors.Is(openErr, ErrFormat) {
			t.Fatalf("read-only WAL open error = %v, want FormatError", openErr)
		}
		return
	}
	defer closeTest(t, reader)
	entity, readErr := reader.Entity(ctx, "wal/item")
	if readErr != nil {
		t.Fatal(readErr)
	}
	if entity["wal/value"] != "uncheckpointed" {
		t.Fatalf("read-only WAL entity = %#v", entity)
	}
}

func TestReadOnlyOpenObservesLiveWALCommits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live.db")
	writer, err := Open(path, WithClock(func() int64 { return 1_767_225_600_000_000 }))
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, writer)
	reader, err := Open(path, WithReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, reader)

	ctx := context.Background()
	if _, err = writer.Transact(ctx, E{"id": "live/item", "live/value": "new"}); err != nil {
		t.Fatal(err)
	}
	entity, err := reader.Entity(ctx, "live/item")
	if err != nil {
		t.Fatal(err)
	}
	if entity["live/value"] != "new" {
		t.Fatalf("live read-only entity = %#v", entity)
	}
}

func TestImmutableSQLiteDSNRejectsMissingFilesAndDanglingSidecars(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.db")
	if _, ok := immutableSQLiteDSN(path); ok {
		t.Fatal("missing database produced an immutable SQLite DSN")
	}
	if err := os.WriteFile(path, []byte("sqlite fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing-wal-target", path+"-wal"); err != nil {
		t.Fatal(err)
	}
	if _, ok := immutableSQLiteDSN(path); ok {
		t.Fatal("dangling WAL sidecar produced an immutable SQLite DSN")
	}
}
