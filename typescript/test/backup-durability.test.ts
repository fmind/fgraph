import { mkdtempSync, openSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, expect, it, vi } from "vitest";

import { connect } from "../src/store.js";

vi.mock("node:fs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs")>();
  return { ...actual, openSync: vi.fn(actual.openSync) };
});

const directories: string[] = [];

afterEach(() => {
  vi.mocked(openSync).mockClear();
  while (directories.length > 0)
    rmSync(directories.pop()!, { recursive: true });
});

it("uses a write-capable file handle and skips unsupported Windows directory fsync", async () => {
  const directory = mkdtempSync(join(tmpdir(), "fgraph-backup-durability-"));
  directories.push(directory);
  const source = join(directory, "source.db");
  const target = join(directory, "backup.db");
  using database = connect(source, { clock: 1_767_225_600_000_000n });
  database.transact({ id: "backup/item", "item/value": "preserved" });

  const platform = Object.getOwnPropertyDescriptor(process, "platform");
  Object.defineProperty(process, "platform", {
    configurable: true,
    value: "win32",
  });
  try {
    await database.backup(target);
  } finally {
    if (platform !== undefined)
      Object.defineProperty(process, "platform", platform);
  }

  const synchronizationOpens = vi
    .mocked(openSync)
    .mock.calls.filter(
      (call) =>
        call[0] === directory || String(call[0]).includes(".fgraph-backup"),
    );
  expect(synchronizationOpens).toHaveLength(1);
  expect(synchronizationOpens[0]?.[1]).toBe("r+");
});
