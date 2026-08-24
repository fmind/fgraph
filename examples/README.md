# Examples

Runnable examples for both implementations. They are **acceptance tests**: once v0.1 lands they must run exactly as written (`SPEC.md` §15 M6 wires them into CI) — fix the implementation, not the examples.

- Python (PEP 723 scripts): `uv run examples/python/quickstart.py`
- Go: `go run -C examples/go ./quickstart` (a `replace` directive targets the local `go/` module)
