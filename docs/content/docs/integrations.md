---
title: Integrations
weight: 4
---

`fgraph mcp` serves your fact store to any MCP-capable agent over stdio. Three modes: an agent's own memory (read-write), a project knowledge base (add `--read-only`), or both. Tools: `remember`, `recall`, `about`, `why`, `history`, `forget`, `query`.

## Claude Code

```bash
claude mcp add fgraph --scope project -- fgraph mcp --db ./project.db --read-only
```

## Codex

```bash
codex mcp add fgraph -- fgraph mcp --db ./project.db
```

## OpenCode

```json
"mcp": {
  "fgraph": {
    "type": "local",
    "command": ["fgraph", "mcp", "--db", "./project.db"],
    "enabled": true
  }
}
```

## Antigravity

```json
"fgraph": {
  "command": "fgraph",
  "args": ["mcp", "--db", "./project.db"]
}
```

## GitHub Copilot

```bash
copilot mcp add fgraph -- fgraph mcp --db ./project.db
```

{{< callout type="info" >}} No `fgraph` binary on PATH? Use `uvx fgraph mcp ...` (Python) or `go run github.com/fmind/fgraph/go/cmd/fgraph@latest mcp ...` (Go) as the command. Semantic recall needs an embedder: pass `--embed-cmd <command>` (text on stdin, JSON float array on stdout); keyword recall works out of the box. {{< /callout >}}

## Project memory tip

Add a line to your project's `AGENTS.md` so agents actually use the store: _"Durable project knowledge lives in the fgraph MCP server — `recall` before re-deriving facts, `remember` decisions with a `source`."_
