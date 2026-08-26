import {
  chmodSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, expect, it } from "vitest";

import { FormatError } from "../src/errors.js";
import { connect } from "../src/store.js";

const directories: string[] = [];

afterEach(() => {
  while (directories.length > 0)
    rmSync(directories.pop() as string, { recursive: true, force: true });
});

it.skipIf(process.platform === "win32")(
  "opens a read-only database without a writable sidecar directory",
  () => {
    const directory = mkdtempSync(join(tmpdir(), "fgraph-readonly-mount-"));
    directories.push(directory);
    const path = join(directory, "graph.db");
    using writer = connect(path, { clock: 1_767_225_600_000_000n });
    writer.transact({
      id: "readonly/item",
      "readonly/value": "available",
    });
    writer.close();

    chmodSync(path, 0o444);
    chmodSync(directory, 0o555);
    try {
      let writeError: NodeJS.ErrnoException | undefined;
      try {
        writeFileSync(join(directory, "write-probe"), "");
      } catch (error) {
        writeError = error as NodeJS.ErrnoException;
      }
      expect(writeError?.code).toBe("EACCES");

      const previousUriSetting = process.env.SQLITE_USE_URI;
      process.env.SQLITE_USE_URI = "0";
      try {
        expect(() => connect(path, { readOnly: true })).toThrowError(
          /set SQLITE_USE_URI=1/u,
        );
      } finally {
        if (previousUriSetting === undefined) delete process.env.SQLITE_USE_URI;
        else process.env.SQLITE_USE_URI = previousUriSetting;
      }

      using reader = connect(path, { readOnly: true });
      expect(reader.entity("readonly/item")).toEqual({
        "readonly/value": "available",
      });
      expect(reader.doctor().ok).toBe(true);
      reader.close();

      expect(readdirSync(directory)).toEqual(["graph.db"]);
    } finally {
      chmodSync(directory, 0o700);
      chmodSync(path, 0o600);
    }
  },
);

it("preserves an explicit SQLite URI setting", () => {
  const previousUriSetting = process.env.SQLITE_USE_URI;
  process.env.SQLITE_USE_URI = "1";
  try {
    using database = connect(":memory:");
    expect(database.stats().transactions).toBe(1);
    expect(process.env.SQLITE_USE_URI).toBe("1");
  } finally {
    if (previousUriSetting === undefined) delete process.env.SQLITE_USE_URI;
    else process.env.SQLITE_USE_URI = previousUriSetting;
  }
});

it.skipIf(process.platform === "win32")(
  "never ignores an existing WAL during a read-only open",
  () => {
    const directory = mkdtempSync(join(tmpdir(), "fgraph-readonly-wal-"));
    directories.push(directory);
    const path = join(directory, "graph.db");
    const wal = `${path}-wal`;
    const shm = `${path}-shm`;
    const writer = connect(path, { clock: 1_767_225_600_000_000n });
    writer._connection.pragma("wal_autocheckpoint = 0");
    writer._connection.pragma("wal_checkpoint(TRUNCATE)");
    writer.transact({ id: "wal/item", "wal/value": "uncheckpointed" });
    expect(readdirSync(directory).sort()).toEqual([
      "graph.db",
      "graph.db-shm",
      "graph.db-wal",
    ]);

    for (const entry of [path, wal, shm]) chmodSync(entry, 0o444);
    chmodSync(directory, 0o555);
    try {
      let reader: ReturnType<typeof connect> | undefined;
      try {
        reader = connect(path, { readOnly: true });
      } catch (error) {
        // Some VFSes require writable WAL locks. Refusing is correct;
        // immutable fallback would silently hide the uncheckpointed row.
        expect(error).toBeInstanceOf(FormatError);
      }
      try {
        if (reader !== undefined)
          expect(reader.entity("wal/item")).toEqual({
            "wal/value": "uncheckpointed",
          });
      } finally {
        reader?.close();
      }
    } finally {
      chmodSync(directory, 0o700);
      for (const entry of [path, wal, shm]) chmodSync(entry, 0o600);
      writer.close();
    }
  },
);
