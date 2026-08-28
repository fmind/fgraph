"""Start the configured embedder after its Windows Job Object is ready."""

from __future__ import annotations

import json
import subprocess
import sys
from typing import cast

START_ERROR = b"fgraph:embed-start-error"


def main() -> int:
    """Forward stdin and stdout while preserving the embedder's exit code."""
    arguments = cast(list[str], json.loads(sys.argv[1]))
    try:
        completed = subprocess.run(  # noqa: S603
            arguments,
            input=sys.stdin.buffer.read(),
            stdout=sys.stdout.buffer,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        # The parent owns this private pipe; the configured embedder cannot
        # forge the signal because its stderr is disconnected.
        sys.stderr.buffer.write(START_ERROR)
        return 1
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
