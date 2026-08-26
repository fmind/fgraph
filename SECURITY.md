# Security policy

## Report a vulnerability privately

Do not put vulnerability details in a public issue. Use [GitHub private vulnerability reporting](https://github.com/fmind/fgraph/security/advisories/new) when the form is available. If it is unavailable, open a minimal issue titled `Security contact request` with no technical details so the maintainer can establish a private channel.

In the private report, include:

- the affected version/runtime and operating system;
- the smallest reproduction you can provide;
- the expected and observed security boundary;
- likely impact, prerequisites, and whether secrets or personal data are involved.

The maintainer aims to acknowledge reports within seven days. A fix, advisory, and release will be coordinated before public disclosure when the report is confirmed.

## Supported versions

The latest stable 1.x minor line and the current `main` branch receive security fixes; older minor lines may be asked to upgrade. The PyPI 0.0.1 name-reservation package is not a supported implementation.

## Trust boundary

fgraph is an embedded database, not an authorization service. Event and snapshot hashes detect accidental corruption but do not stop a process that can rewrite both data and hashes. Protect the SQLite file, backups, snapshots, MCP process, and any configured embedder with operating-system permissions and encryption appropriate to the data.

Read-only MCP, bounded queries/results, strict event/snapshot validation, and audited excision are defense-in-depth controls. They do not erase copies in backups or external collectors and do not make an untrusted writable file tamper-evident.
