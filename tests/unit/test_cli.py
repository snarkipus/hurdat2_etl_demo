"""Characterize accepted loader inputs without replaying database integration."""

from datetime import UTC, datetime

import pytest
from typer.testing import CliRunner

from etl_pipeline.cli import app
from etl_pipeline.transform.models import Observation, Point, Storm
from etl_pipeline.transform.transform import TransformStage
from tests.unit.transform.test_transform import (
    INVALID_OBS_ROW,
    SAMPLE_HEADER_1,
    SAMPLE_HEADER_2,
    SAMPLE_OBS_1_1,
    SAMPLE_OBS_1_2,
    SAMPLE_OBS_2_1,
)


@pytest.fixture
def cli_boundary(mocker, tmp_path):
    """Retain real transformation; replace I/O and presentation at the CLI edges."""
    source = tmp_path / "source.txt"
    source.touch()
    extract = mocker.patch("etl_pipeline.cli.ExtractStage").return_value
    transform = TransformStage()
    mocker.patch("etl_pipeline.cli.TransformStage", return_value=transform)
    transformed = mocker.spy(transform, "execute")
    load = mocker.patch("etl_pipeline.cli.LoadStage").return_value
    load.execute.return_value = (0, 0)  # Deliberately not an acceptance oracle.
    mocker.patch("etl_pipeline.cli.create_engine")
    mocker.patch("etl_pipeline.cli.ProgressManager")
    session = mocker.patch("etl_pipeline.cli.sessionmaker").return_value
    report_result = session.return_value.__enter__.return_value.execute.return_value
    report_result.scalar_one_or_none.return_value = 0
    report_result.fetchone.return_value = (None, None)
    report_result.fetchall.return_value = []
    return source, extract, transformed, load


def test_last_wins_and_exact_flattened_loader_inputs(cli_boundary, tmp_path):
    source, extract, transformed, load = cli_boundary
    extract.execute.return_value = [
        ["unrecognized before first header"],
        SAMPLE_HEADER_1,
        SAMPLE_OBS_1_1,
        SAMPLE_HEADER_2,
        SAMPLE_OBS_2_1,
        ["AL011851", "REVISED", "3", ""],
        SAMPLE_OBS_1_2,
        INVALID_OBS_ROW,
        ["malformed observation"],
    ]
    result = CliRunner().invoke(
        app, ["--input", str(source), "--output", str(tmp_path / "output.duckdb")]
    )
    assert result.exit_code == 0, result.output

    # Last valid duplicate replaces its earlier observations too, but retains
    # the first occurrence's storm ordering. Rejected rows never reach loading.
    revised_obs = Observation(
        storm_id="AL011851",
        date=datetime(1851, 6, 25, 6, tzinfo=UTC),
        status="HU",
        location=Point(latitude=28.0, longitude=-95.4),
        max_wind=80,
    )
    other_obs = Observation(
        storm_id="AL021851",
        date=datetime(1851, 7, 5, 12, tzinfo=UTC),
        status="TS",
        location=Point(latitude=22.2, longitude=-97.5),
        max_wind=40,
    )
    storms = [
        Storm(
            basin="AL",
            cyclone_number=1,
            year=1851,
            name="REVISED",
            observations=[revised_obs],
        ),
        Storm(
            basin="AL",
            cyclone_number=2,
            year=1851,
            name="STORM_TWO",
            observations=[other_obs],
        ),
    ]
    assert transformed.spy_return == storms
    load.execute.assert_called_once_with((storms, [revised_obs, other_obs]))


@pytest.mark.parametrize(
    "rows", [[], [["unrecognized"], SAMPLE_HEADER_1, INVALID_OBS_ROW, ["malformed"]]]
)
def test_zero_accepted_inputs(cli_boundary, tmp_path, rows):
    source, extract, transformed, load = cli_boundary
    extract.execute.return_value = rows
    result = CliRunner().invoke(
        app, ["--input", str(source), "--output", str(tmp_path / "empty.duckdb")]
    )
    assert result.exit_code == 0, result.output
    assert transformed.spy_return == []
    load.execute.assert_called_once_with(([], []))
