"""Historical selectors stay lossless and inside the shared integer domain."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

import fgraph
from fgraph.cli import app
from fgraph.values import INT64_MAX, INT64_MIN


@pytest.mark.parametrize("selector", [INT64_MIN, INT64_MAX, INT64_MIN - 1, INT64_MAX + 1])
def test_library_rejects_historical_selectors_outside_instant_domain(db: fgraph.Db, selector: int) -> None:
    with pytest.raises(fgraph.TypeError):
        db.at(selector)


def test_integer_historical_selector_resolves_as_an_instant_after_tx_lookup(db: fgraph.Db) -> None:
    report = db.transact({"id": "selector/subject", "item/value": "present"})

    assert report.at is not None
    assert report.tx is not None
    assert report.at > report.tx
    assert db.at(report.at).entity("selector/subject")["item/value"] == "present"


@pytest.mark.parametrize("command", ["get", "q"])
@pytest.mark.parametrize("selector", [INT64_MIN, INT64_MAX, INT64_MIN - 1, INT64_MAX + 1])
def test_cli_rejects_historical_selector_boundaries_as_typed_errors(
    tmp_path: Path,
    command: str,
    selector: int,
) -> None:
    path = tmp_path / "selector.db"
    with fgraph.connect(path, clock=1_767_225_600_000_000) as graph:
        graph.transact({"id": "selector/subject", "item/value": "present"})
    arguments = (
        ["get", "selector/subject"]
        if command == "get"
        else ["q", '{"find":["?e"],"where":[["?e","item/value","present"]]}']
    )

    result = CliRunner().invoke(app, [*arguments, "--at", str(selector), "--db", str(path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, fgraph.TypeError)
