# AGENTS.md (Project)

Context and rules for AI agents working in this repository. Humans should start with `README.md`.

## Project overview

- **Name**: fgraph — an embedded temporal fact store in a single SQLite file (Datomic-inspired EAVT model, JSON-native, hybrid search, MCP server).
- **Normative source**: `SPEC.md` defines the file format, semantics, query language, APIs, CLI, MCP tools, and milestones. When SPEC.md and any other file (including this one) disagree, SPEC.md wins. Do not change SPEC.md casually — it is the contract between the twin implementations; propose spec changes explicitly.
- **Twin implementations**: `python/` (uv, stdlib-only core) and `go/` (module `github.com/fmind/fgraph/go`, `modernc.org/sqlite`, CGO-free). Both are primary tier: every behavior lands in both, verified by `conformance/`.

## Skills to load

- `~/.agents/skills/python-stack/SKILL.md` for all work in `python/` (uv, Ruff, ty, pytest; library+CLI profile, Typer).
- `~/.agents/skills/go-stack/SKILL.md` for all work in `go/` (golangci-lint, gofumpt/goimports, gotestsum, urfave/cli/v3).
- `~/.agents/skills/hugo/SKILL.md` for `docs/` (Hugo extended + Hextra + GitHub Pages).
- `~/.agents/skills/mise/SKILL.md`, `lefthook`, `dprint`, `github-actions`, `conventional-commit`, `readme-agents` as referenced by the stacks.
- Use the **latest stable** release of every tool and dependency; verify versions online before pinning.

## Setup & core commands

All work goes through `mise` (see `mise.toml`); git hooks and CI call the same tasks.

- Install: `mise trust && mise install`, then `mise run install`.
- Format: `mise run format` — dprint + ruff + goimports/gofumpt.
- Check: `mise run check` — dprint check, gitleaks, ruff/ty/pip-audit (python), golangci-lint/govulncheck (go), strict docs build.
- Test: `mise run test` — pytest and gotestsum with **95% coverage gates**, docs link check, and `test:cross` (cross-implementation file compatibility via `scripts/crosscheck.sh`).
- Build: `mise run build` — wheel/sdist, `go/bin/fgraph`, docs site.
- Everything: `mise run all`.

## Definition of done

A change is complete only when `mise run all` passes warning-free, the relevant `conformance/` cases are green in **both** implementations, and new or changed behavior has a test plus (when normative) a conformance case. Never weaken an assertion, skip a test, loosen a type, lower the coverage gate, or suppress a lint error to force green — fix the root cause. The `examples/` are acceptance tests: fix the implementation to match them, not the reverse (unless SPEC.md says otherwise).

## Conventions

- **Determinism is sacred**: allocation order, timestamps (integer microseconds, injectable clock), canonical JSON, and format constants in SPEC.md §4 produce byte-identical files across implementations. Any deviation is a bug.
- **Errors**: typed taxonomy from SPEC.md §10 in both languages; wrap with context; never swallow.
- **No network in core or tests**; embeddings are always caller-provided.
- **Commits**: Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`); never commit unless explicitly asked; no attribution or co-author trailers.
- **Simplicity**: implement what SPEC.md requires, nothing more. No speculative abstractions, options, or "enterprise" features.

## Repository layout

- `SPEC.md` — normative specification and implementation milestones (start here).
- `python/` — Python implementation (uv project; `src/fgraph/`, `tests/`).
- `go/` — Go implementation (`fgraph.go` library at module root, `cmd/fgraph/` CLI).
- `conformance/` — shared JSON test cases + `crosscheck.ndjson` scenario; both implementations run every case.
- `examples/` — runnable Python (PEP 723 scripts) and Go examples.
- `docs/` — Hugo + Hextra documentation site, deployed by `.github/workflows/pages.yml`.
- `scripts/crosscheck.sh` — cross-implementation file-compatibility check (`mise run test:cross`).
- `mise.toml` / `lefthook.yml` / `dprint.json` — task runner (single source of truth), git hooks, config formatting.
