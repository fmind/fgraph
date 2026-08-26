import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { main } from "../src/cli.js";
import { TypeError as FGraphTypeError } from "../src/errors.js";
import { connect } from "../src/store.js";
import { INT64_MAX, INT64_MIN } from "../src/values.js";

const directories: string[] = [];

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
  delete process.env.FGRAPH_CLOCK;
});

describe("historical selector integer bounds", () => {
  it("resolves an integer above transaction ids as an instant", () => {
    using db = connect(":memory:", { clock: 1_767_225_600_000_000n });
    const report = db.transact({
      id: "selector/subject",
      "item/value": "present",
    });

    expect(BigInt(report.at as number)).toBeGreaterThan(
      BigInt(report.tx as number),
    );
    expect(db.at(report.at).entity("selector/subject")["item/value"]).toBe(
      "present",
    );
  });

  it.each([INT64_MIN, INT64_MAX, INT64_MIN - 1n, INT64_MAX + 1n])(
    "rejects library selector %s with the typed taxonomy",
    (selector) => {
      using db = connect(":memory:", { clock: 1_767_225_600_000_000n });

      expect(() => db.at(selector)).toThrowError(FGraphTypeError);
    },
  );

  it("rejects CLI boundaries and adjacent overflows as typed errors", async () => {
    const directory = mkdtempSync(join(tmpdir(), "fgraph-selector-"));
    directories.push(directory);
    const path = join(directory, "selector.db");
    process.env.FGRAPH_CLOCK = "1767225600000000";
    expect(
      (
        await invoke(
          "add",
          '{"id":"selector/subject","item/value":"present"}',
          "--db",
          path,
        )
      ).code,
    ).toBe(0);
    const query = '{"find":["?e"],"where":[["?e","item/value","present"]]}';
    const selectors = [
      INT64_MIN.toString(),
      INT64_MAX.toString(),
      (INT64_MIN - 1n).toString(),
      (INT64_MAX + 1n).toString(),
    ];

    for (const command of [
      ["get", "selector/subject"],
      ["q", query],
    ]) {
      for (const selector of selectors) {
        expect(
          await invoke(...command, "--at", selector, "--db", path),
        ).toMatchObject({
          code: 1,
          stderr: expect.stringContaining("TypeError:"),
        });
      }
    }
  });
});
