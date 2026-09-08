"""Characterize accepted loader inputs without replaying database integration."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from etl_pipeline.cli import _check_candidate_ready, _reserve_candidate, _warn, app
from etl_pipeline.exceptions import ETLError, LoadError
from etl_pipeline.publication import publish_candidate
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
    mocker.patch("etl_pipeline.cli.initialize_database")
    mocker.patch("etl_pipeline.cli.verify_persisted_dataset")
    # These input/resource boundary tests do not build a real database.
    mocker.patch("etl_pipeline.cli._check_candidate_ready")
    mocker.patch("etl_pipeline.cli.publish_candidate")
    mocker.patch("etl_pipeline.cli.ProgressManager")
    session = mocker.patch("etl_pipeline.cli.sessionmaker").return_value
    report_result = session.return_value.execute.return_value
    report_result.scalar_one_or_none.return_value = 0
    report_result.fetchone.return_value = (None, None)
    report_result.fetchall.return_value = []
    return source, extract, transformed, load


def test_last_wins_and_exact_flattened_loader_inputs(cli_boundary, tmp_path, mocker):
    source, extract, transformed, load = cli_boundary
    verify = mocker.patch("etl_pipeline.cli.verify_persisted_dataset")
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
    assert verify.call_args.args[1:] == (2, 2)


@pytest.mark.parametrize(
    "rows", [[], [["unrecognized"], SAMPLE_HEADER_1, INVALID_OBS_ROW, ["malformed"]]]
)
def test_zero_accepted_inputs(cli_boundary, tmp_path, rows, mocker):
    source, extract, transformed, load = cli_boundary
    verify = mocker.patch("etl_pipeline.cli.verify_persisted_dataset")
    extract.execute.return_value = rows
    result = CliRunner().invoke(
        app, ["--input", str(source), "--output", str(tmp_path / "empty.duckdb")]
    )
    assert result.exit_code == 0, result.output
    assert transformed.spy_return == []
    load.execute.assert_called_once_with(([], []))
    assert verify.call_args.args[1:] == (0, 0)


def test_migration_failure_prevents_loading_and_disposes(
    cli_boundary, mocker, tmp_path
):
    source, extract, _, load = cli_boundary
    primary = RuntimeError("migration failed")
    initialize = mocker.patch(
        "etl_pipeline.cli.initialize_database", side_effect=primary
    )
    engine = mocker.patch("etl_pipeline.cli.create_engine").return_value
    engine.dispose.side_effect = RuntimeError("dispose failed")
    result = CliRunner().invoke(
        app, ["--input", str(source), "--output", str(tmp_path / "failed.duckdb")]
    )
    assert result.exit_code == 1
    assert "migration failed" in result.output
    initialize.assert_called_once_with(engine.begin.return_value.__enter__.return_value)
    extract.execute.assert_not_called()
    load.execute.assert_not_called()
    engine.dispose.assert_called_once()


def test_load_failure_disposes_without_starting_report(cli_boundary, mocker, tmp_path):
    source, extract, _, load = cli_boundary
    extract.execute.return_value = []
    load.execute.side_effect = LoadError("load failed")
    create_engine = mocker.patch("etl_pipeline.cli.create_engine")
    result = CliRunner().invoke(
        app, ["--input", str(source), "--output", str(tmp_path / "failed.duckdb")]
    )
    assert result.exit_code == 1
    assert "load failed" in result.output
    create_engine.assert_called_once()
    create_engine.return_value.dispose.assert_called_once()


def test_verification_uses_snapshot_and_failure_prevents_success_and_report(
    cli_boundary, mocker, tmp_path
):
    source, extract, _, load = cli_boundary
    extract.execute.return_value = [SAMPLE_HEADER_1, SAMPLE_OBS_1_1]

    def misleading_load(data):
        data[0].clear()
        data[1].clear()
        return (999, 999)

    load.execute.side_effect = misleading_load
    verify = mocker.patch(
        "etl_pipeline.cli.verify_persisted_dataset",
        side_effect=ETLError("Persisted count mismatch"),
    )
    report = mocker.patch("etl_pipeline.cli._report_session")
    result = CliRunner().invoke(
        app, ["--input", str(source), "--output", str(tmp_path / "failed.duckdb")]
    )
    assert result.exit_code == 1
    assert verify.call_args.args[1:] == (1, 1)
    assert "Persisted count mismatch" in result.output
    assert "completed successfully" not in result.output
    report.assert_not_called()
    verify.call_args.args[0].dispose.assert_called_once()


@pytest.mark.parametrize("report_failure", [None, "factory", "query"])
def test_cli_owns_both_engines(cli_boundary, mocker, tmp_path, report_failure):
    source, extract, _, load = cli_boundary
    extract.execute.return_value = []
    load_engine, report_engine = mocker.MagicMock(), mocker.MagicMock()
    mocker.patch(
        "etl_pipeline.cli.create_engine", side_effect=[load_engine, report_engine]
    )
    load_factory, report_factory = mocker.MagicMock(), mocker.MagicMock()
    mocker.patch(
        "etl_pipeline.cli.sessionmaker", side_effect=[load_factory, report_factory]
    )
    report_session = report_factory.return_value
    result_rows = report_session.execute.return_value
    result_rows.scalar_one_or_none.return_value = 0
    result_rows.fetchone.return_value = (None, None)
    result_rows.fetchall.return_value = []
    if report_failure == "factory":
        report_factory.side_effect = RuntimeError("report factory failed")
    elif report_failure == "query":
        report_session.execute.side_effect = RuntimeError("report query failed")
        report_session.close.side_effect = RuntimeError("report close failed")
        report_engine.dispose.side_effect = RuntimeError("report disposal failed")
    result = CliRunner().invoke(
        app, ["--input", str(source), "--output", str(tmp_path / "output.duckdb")]
    )
    assert result.exit_code == 0, result.output
    load.execute.assert_called_once()
    load_engine.dispose.assert_called_once()
    report_engine.dispose.assert_called_once()
    if report_failure:
        assert f"report {report_failure} failed" in result.output
    if report_failure == "query":
        report_session.close.assert_called_once()


@pytest.mark.parametrize(
    "failure", ["migration", "verification", "dispose", "interrupt"]
)
def test_incomplete_candidate_cleanup_preserves_old_output(
    cli_boundary, mocker, tmp_path, failure
):
    source, extract, _, _ = cli_boundary
    extract.execute.return_value = []
    output = tmp_path / "old.duckdb"
    output.write_bytes(b"old database")
    sidecar = Path(f"{output}.tmp")
    sidecar.mkdir()
    (sidecar / "unrelated").write_bytes(b"old transient state")
    candidates = []
    engine = mocker.MagicMock()

    def create(url):
        candidate = Path(url.removeprefix("duckdb:///"))
        assert candidate.parent == output.parent
        assert not candidate.exists()  # Never give DuckDB the reservation file.
        candidates.append(candidate)
        candidate.write_bytes(b"incomplete")
        Path(f"{candidate}.wal").write_bytes(b"owned WAL")
        Path(f"{candidate}.tmp").mkdir()
        return engine

    mocker.patch("etl_pipeline.cli.create_engine", side_effect=create)
    if failure == "migration":
        mocker.patch(
            "etl_pipeline.cli.initialize_database", side_effect=ETLError(failure)
        )
    elif failure == "verification":
        mocker.patch(
            "etl_pipeline.cli.verify_persisted_dataset", side_effect=ETLError(failure)
        )
    elif failure == "dispose":
        engine.dispose.side_effect = RuntimeError(failure)
    else:
        extract.execute.side_effect = KeyboardInterrupt("interrupt")
    publish = mocker.patch("etl_pipeline.cli.publish_candidate")
    result = CliRunner().invoke(
        app, ["--input", str(source), "--output", str(output), "--replace"]
    )
    assert result.exit_code == 1, result.output
    assert failure in result.output
    assert output.read_bytes() == b"old database"
    assert (sidecar / "unrelated").read_bytes() == b"old transient state"
    assert not list(tmp_path.glob(f"{candidates[0].name}*"))
    engine.dispose.assert_called_once()
    publish.assert_not_called()


@pytest.mark.parametrize("late_wal", [False, True])
def test_publication_boundary_follows_verification_and_disposal(
    cli_boundary, mocker, tmp_path, late_wal
):
    source, extract, _, _ = cli_boundary
    extract.execute.return_value = []
    output = tmp_path / "output.duckdb"
    engine = mocker.MagicMock()
    events = []
    candidates = []

    def create(url):
        candidate = Path(url.removeprefix("duckdb:///"))
        assert not candidate.exists()
        candidate.write_bytes(b"candidate")
        candidates.append(candidate)
        return engine

    mocker.patch("etl_pipeline.cli.create_engine", side_effect=create)
    mocker.patch(
        "etl_pipeline.cli.initialize_database",
        side_effect=lambda _: events.append("migrate"),
    )
    mocker.patch(
        "etl_pipeline.cli.verify_persisted_dataset",
        side_effect=lambda *_: events.append("verify"),
    )
    engine.dispose.side_effect = lambda: events.append("dispose")

    def publish(candidate, destination, *, replace):
        assert events == ["migrate", "verify", "dispose"]
        assert not destination.exists()
        if late_wal:
            Path(f"{destination}.wal").write_bytes(b"external WAL")
        publish_candidate(candidate, destination, replace=replace)

    mocker.patch("etl_pipeline.cli.publish_candidate", side_effect=publish)
    report = mocker.patch(
        "etl_pipeline.cli._report_session", side_effect=RuntimeError("optional query")
    )
    result = CliRunner().invoke(app, ["--input", str(source), "--output", str(output)])
    candidate = candidates[0]
    if late_wal:
        assert result.exit_code == 1
        assert str(candidate) in result.output.replace("\n", "")
        assert candidate.read_bytes() == b"candidate"
        assert not output.exists()
        assert Path(f"{output}.wal").read_bytes() == b"external WAL"
        report.assert_not_called()
    else:
        assert result.exit_code == 0, result.output
        assert output.read_bytes() == b"candidate"
        assert not candidate.exists()
        report.assert_called_once_with(output)
        assert "optional query" in result.output


def test_cleanup_failure_preserves_primary_and_reports_leftover(
    cli_boundary, mocker, tmp_path
):
    source, _, _, _ = cli_boundary
    candidate = tmp_path / ".owned.duckdb"
    candidate.write_bytes(b"incomplete")
    mocker.patch("etl_pipeline.cli._reserve_candidate", return_value=candidate)
    mocker.patch(
        "etl_pipeline.cli.initialize_database",
        side_effect=ETLError("primary migration"),
    )
    mocker.patch.object(Path, "unlink", side_effect=PermissionError("denied cleanup"))
    result = CliRunner().invoke(
        app, ["--input", str(source), "--output", str(tmp_path / "out")]
    )
    assert result.exit_code == 1
    assert "primary migration" in result.output
    assert "denied cleanup" in result.output
    assert str(candidate) in result.output.replace("\n", "")
    assert candidate.exists()


@pytest.mark.parametrize("failure", ["unlink", "render"])
def test_postpublication_failure_keeps_success(cli_boundary, mocker, tmp_path, failure):
    source, extract, _, _ = cli_boundary
    extract.execute.return_value = []
    candidate = tmp_path / ".owned.duckdb"
    candidate.write_bytes(b"verified")
    output = tmp_path / "out.duckdb"
    mocker.patch("etl_pipeline.cli._reserve_candidate", return_value=candidate)
    mocker.patch("etl_pipeline.cli.publish_candidate", side_effect=publish_candidate)
    if failure == "unlink":
        mocker.patch.object(
            Path, "unlink", side_effect=PermissionError("cleanup denied")
        )
    else:
        from rich.console import Console

        original = Console.print

        def print_after_publication(console, *args, **kwargs):
            if output.exists():
                raise RuntimeError("render failed")
            return original(console, *args, **kwargs)

        mocker.patch.object(Console, "print", print_after_publication)
    result = CliRunner().invoke(app, ["--input", str(source), "--output", str(output)])
    assert result.exit_code == 0, result.output
    assert output.read_bytes() == b"verified"
    assert "Warning:" in result.output
    if failure == "unlink":
        assert candidate.exists()
        assert str(candidate) in result.output.replace("\n", "")


def test_candidate_reservation_and_standalone_gate(tmp_path, mocker):
    console = mocker.MagicMock()
    output = tmp_path / "out.duckdb"
    first = _reserve_candidate(output, console)
    second = _reserve_candidate(output, console)
    assert first != second
    assert first.parent == second.parent == output.parent
    assert not first.exists() and not second.exists()
    with pytest.raises(ETLError, match="standalone"):
        _check_candidate_ready(first)
    first.write_bytes(b"closed database")
    wal = Path(f"{first}.wal")
    wal.write_bytes(b"unfinalized WAL")
    with pytest.raises(ETLError, match="standalone"):
        _check_candidate_ready(first)
    wal.unlink()
    _check_candidate_ready(first)


def test_warning_sink_failures_do_not_change_outcome(mocker):
    console = mocker.Mock()
    console.print.side_effect = RuntimeError("render failed")
    fallback = mocker.patch(
        "etl_pipeline.cli.print", create=True, side_effect=BrokenPipeError("closed")
    )
    _warn(console, "Output already published")
    fallback.assert_called_once()
