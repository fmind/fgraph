import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { main } from "../src/cli.js";
import { canonicalJson, parseJson } from "../src/jsonio.js";
import { MAX_EVENT_BYTES, connect } from "../src/store.js";

const MAX_SNAPSHOT_LINE_BYTES = 2 * MAX_EVENT_BYTES + 64 * 1024;
const directories: string[] = [];

function temporaryDirectory(): string {
  const path = mkdtempSync(join(tmpdir(), "fgraph-cli-stream-"));
  directories.push(path);
  return path;
}

async function invoke(...args: string[]): Promise<{
  code: number;
  stdout: string;
  stderr: string;
}> {
  let stdout = "";
  let stderr = "";
  const stdoutSpy = vi.spyOn(process.stdout, "write").mockImplementation(((
    chunk: string | Uint8Array,
  ) => {
    stdout += chunk.toString();
    return true;
  }) as typeof process.stdout.write);
  const stderrSpy = vi.spyOn(process.stderr, "write").mockImplementation(((
    chunk: string | Uint8Array,
  ) => {
    stderr += chunk.toString();
    return true;
  }) as typeof process.stderr.write);
  try {
    return { code: await main(args), stdout, stderr };
  } finally {
    stdoutSpy.mockRestore();
    stderrSpy.mockRestore();
  }
}

afterEach(() => {
  while (directories.length > 0)
    rmSync(directories.pop() as string, { recursive: true, force: true });
});

describe("CLI streamed input bounds", () => {
  it("accepts an exact-limit snapshot line and split UTF-8 code point", async () => {
    const directory = temporaryDirectory();
    using source = connect(":memory:", { clock: 1_767_225_600_000_000n });
    source.transact({ id: "stream/item", "stream/value": 1 });
    const snapshot = source.snapshot() as string;
    const newline = snapshot.indexOf("\n");
    const header = snapshot.slice(0, newline);
    const padding = " ".repeat(
      MAX_SNAPSHOT_LINE_BYTES - Buffer.byteLength(header, "utf8"),
    );
    const exactPath = join(directory, "exact.ndjson");
    writeFileSync(
      exactPath,
      `${header}${padding}\n${snapshot.slice(newline + 1)}`,
    );
    expect(
      await invoke(
        "restore",
        exactPath,
        "--db",
        join(directory, "exact.db"),
        "--json",
      ),
    ).toMatchObject({ code: 0, stderr: "" });

    const records = snapshot
      .trimEnd()
      .split("\n")
      .map(
        (line) =>
          parseJson(line, "snapshot fixture") as Record<string, unknown>,
      );
    const splitHeader = records[0] as Record<string, unknown>;
    splitHeader.padding = "😀";
    let encodedHeader = canonicalJson(splitHeader);
    const marker = encodedHeader.indexOf("😀");
    const prefixBytes = Buffer.byteLength(encodedHeader.slice(0, marker));
    splitHeader.padding = `${"a".repeat(64 * 1024 - 1 - prefixBytes)}😀`;
    encodedHeader = canonicalJson(splitHeader);
    expect(
      Buffer.byteLength(encodedHeader.slice(0, encodedHeader.indexOf("😀"))),
    ).toBe(64 * 1024 - 1);
    const footer = records.at(-1) as Record<string, unknown>;
    footer.sha256 = createHash("sha256")
      .update(
        `${records
          .slice(0, -1)
          .map((record) => canonicalJson(record))
          .join("\n")}\n`,
        "utf8",
      )
      .digest("hex");
    const splitPath = join(directory, "split.ndjson");
    writeFileSync(
      splitPath,
      `${records.map((record) => canonicalJson(record)).join("\n")}\n`,
    );
    expect(
      await invoke(
        "restore",
        splitPath,
        "--db",
        join(directory, "split.db"),
        "--json",
      ),
    ).toMatchObject({ code: 0, stderr: "" });
  });

  it("rejects oversized apply and restore lines before later invalid UTF-8", async () => {
    const directory = temporaryDirectory();
    for (const [command, limit] of [
      ["apply", MAX_EVENT_BYTES],
      ["restore", MAX_SNAPSHOT_LINE_BYTES],
    ] as const) {
      const input = join(directory, `${command}.ndjson`);
      writeFileSync(
        input,
        Buffer.concat([Buffer.alloc(limit + 1, 0x20), Buffer.from([0xff])]),
      );
      const result = await invoke(
        command,
        input,
        "--db",
        join(directory, `${command}.db`),
      );
      expect(result).toMatchObject({
        code: 1,
        stderr: expect.stringContaining("TooLarge:"),
      });
    }
  });
});
