import { afterEach, describe, expect, it, vi } from "vitest";

afterEach(() => {
  vi.doUnmock("../src/mcp.js");
  vi.resetModules();
});

describe("CLI startup", () => {
  it("keeps the optional MCP SDK lazy for non-MCP commands", async () => {
    let loaded = false;
    vi.doMock("../src/mcp.js", () => {
      loaded = true;
      return { embed: vi.fn(), runMcp: vi.fn() };
    });

    await import("../src/cli.js");

    expect(loaded).toBe(false);
  });
});
