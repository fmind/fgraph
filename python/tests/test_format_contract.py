"""Keep the normative format SQL aligned with the immutable released fixture."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

ROOT = Path(__file__).parents[2]
SPEC = ROOT / "docs" / "content" / "spec.md"
FIXTURE = ROOT / "conformance" / "fixtures" / "format-v2.db"
SCHEMA_QUERY = """
SELECT type, name, sql
FROM sqlite_schema
WHERE name LIKE 'fgraph_%'
  AND name NOT GLOB 'fgraph_fts_*'
  AND sql IS NOT NULL
ORDER BY type, name
"""


def _format_sql() -> str:
    document = SPEC.read_text(encoding="utf-8")
    try:
        return document.split("```sql", 1)[1].split("```", 1)[0]
    except IndexError as error:
        raise AssertionError("the specification has no format SQL block") from error


def _schema(connection: sqlite3.Connection) -> dict[tuple[str, str], str]:
    def normalize(statement: str) -> str:
        return re.sub(r"\s+", " ", statement.strip().removesuffix(";")).lower()

    return {(kind, name): normalize(statement) for kind, name, statement in connection.execute(SCHEMA_QUERY)}


def test_normative_format_sql_matches_immutable_fixture() -> None:
    documented = sqlite3.connect(":memory:")
    fixture = sqlite3.connect(f"file:{FIXTURE}?mode=ro&immutable=1", uri=True)
    try:
        documented.executescript(_format_sql())
        assert _schema(documented) == _schema(fixture)
    finally:
        documented.close()
        fixture.close()
