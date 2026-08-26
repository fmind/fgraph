# Examples

Runnable examples for all three implementations. They are **acceptance tests** and run through `mise run test:examples` — fix the implementation to match them, not the reverse.

- Python quickstart: `uv run --project python python examples/python/quickstart.py`
- Python retry-safe agent memory: run `examples/python/agent_memory.py` from a disposable directory with the same `uv run --project python python` prefix.
- Go quickstart: `go run -C examples/go ./quickstart` (a `replace` directive targets the local `go/` module)
- Go knowledge base: run `go run -C examples/go ./knowledgebase` from a disposable directory.
- TypeScript: `mise run build:typescript && node examples/typescript/quickstart.mjs`
