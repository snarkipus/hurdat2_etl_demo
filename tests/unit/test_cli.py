"""Characterize accepted loader inputs without replaying database integration."""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest
import structlog
import typer
from rich.console import Console
from rich.panel import Panel
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from etl_pipeline.cli import (
    _check_candidate_ready,
    _reserve_candidate,
    _warn,
    app,
    run_etl,
)
from etl_pipeline.exceptions import ETLError, LoadError
from etl_pipeline.load.models import Observation as DbObservation
from etl_pipeline.load.models import Storm as DbStorm
from etl_pipeline.migrations import initialize_database
from etl_pipeline.publication import publish_candidate
from etl_pipeline.transform.models import Observation, Point, Storm, StormStatus
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
        status=StormStatus.HURRICANE,
        location=Point(latitude=28.0, longitude=-95.4),
        max_wind=80,
    )
    other_obs = Observation(
        storm_id="AL021851",
        date=datetime(1851, 7, 5, 12, tzinfo=UTC),
        status=StormStatus.TROPICAL_STORM,
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
    assert str(INVALID_OBS_ROW) not in Path("logs/pipeline.log").read_text()


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


@pytest.mark.parametrize(
    ("wind", "band", "color"),
    [
        (0, "<34 kt", "blue"),
        (33, "<34 kt", "blue"),
        (34, "34-63 kt", "blue"),
        (63, "34-63 kt", "blue"),
        (64, "64-82 kt", "green"),
        (82, "64-82 kt", "green"),
        (83, "83-95 kt", "yellow"),
        (95, "83-95 kt", "yellow"),
        (96, "96-112 kt", "orange1"),
        (112, "96-112 kt", "orange1"),
        (113, "113-136 kt", "red"),
        (136, "113-136 kt", "red"),
        (137, ">=137 kt", "red"),
        (180, ">=137 kt", "red"),
    ],
)
def test_summary_peak_wind_bands_and_literal_names(
    cli_boundary, mocker, tmp_path, wind, band, color
):
    """Exercise real summary SQL/Rich, not repeated full ETL/publication runs."""
    source, extract, _, _ = cli_boundary
    extract.execute.return_value = []
    name = "[red]A[/red][oops]B"
    console = Console(width=200, record=True, highlight=False)
    printed = mocker.spy(console, "print")
    mocker.patch("etl_pipeline.cli.Console", return_value=console)
    engine = create_engine("duckdb:///:memory:")
    try:
        with engine.begin() as connection:
            initialize_database(connection)
        with Session(engine) as session:
            session.add(
                DbStorm(
                    storm_id="AL012023",
                    basin="AL",
                    cyclone_number=1,
                    year=2023,
                    name=name,
                    observations=[
                        DbObservation(
                            id=i,
                            date=datetime(2023, 1, i),
                            status=status,
                            max_wind=speed,
                            geom="POINT(-75.0 25.0)",
                        )
                        for i, status, speed in [(1, "TS", 0), (2, "EX", wind)]
                    ],
                )
            )
            session.commit()
            report = mocker.patch("etl_pipeline.cli._report_session")
            report.return_value.__enter__.return_value = session
            result = CliRunner().invoke(
                app,
                ["--input", str(source), "--output", str(tmp_path / "summary.duckdb")],
            )
            assert result.exit_code == 0, result.output
            output = console.export_text(clear=False)
            assert "Could not generate summary" not in output
            assert "Peak Recorded Wind Distribution (all statuses)" in output
            assert "Wind-speed bands only; not storm classifications." in output
            assert f"{band}: 1 storms" in output
            assert output.count("kt: 1 storms") == 1
            assert f"{name} (2023): 1.0 days" in output
            assert "Hurricane Intensity" not in output
            assert "Category" not in output
            assert "Tropical Storm" not in output
            # Verify the actual summary markup retains the band palette.
            panels = [
                call.args[0]
                for call in printed.call_args_list
                if call.args and isinstance(call.args[0], Panel)
            ]
            assert f"[{color}]{band}:[/{color}]" in panels[-1].renderable
    finally:
        engine.dispose()


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
    events = [
        json.loads(line) for line in Path("logs/pipeline.log").read_text().splitlines()
    ]
    error = next(event for event in events if event["severity"] == "error")
    assert error["operation"] == "verification"
    assert error["stage"] is None  # Not the preceding load stage.
    assert error["error_type"] == "ETLError"
    assert error["error"] == "Persisted count mismatch"
    assert "publication_completed" not in [event["event"] for event in events]


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
        events = [
            json.loads(line)
            for line in Path("logs/pipeline.log").read_text().splitlines()
        ]
        warning = next(event for event in events if event["severity"] == "warning")
        assert warning["operation"] == "summary"
        assert warning["error_type"] == "RuntimeError"
        assert f"report {report_failure} failed" in warning["error"]
        assert any(event["event"] == "publication_completed" for event in events)
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

    def create(url, *, hide_parameters):
        assert hide_parameters is True
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

    def create(url, *, hide_parameters):
        assert hide_parameters is True
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
        assert "publication_completed" not in Path("logs/pipeline.log").read_text()
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
    logger = mocker.patch("etl_pipeline.cli.logging.getLogger").return_value
    logger.warning.side_effect = OSError("log unavailable")
    fallback = mocker.patch(
        "etl_pipeline.cli.print", create=True, side_effect=BrokenPipeError("closed")
    )
    _warn(console, "Output already published")
    fallback.assert_called_once()


def test_repeated_cli_runs_restore_context_handlers_and_filter_libraries(
    cli_boundary, mocker, tmp_path
):
    source, extract, _, _ = cli_boundary
    extract.execute.return_value = []
    root = logging.getLogger()
    handlers = root.handlers[:]
    level = root.level
    library = logging.getLogger("etl_test_library")
    # An explicitly verbose child bypasses root's logger threshold; the owned
    # handler must still enforce the CLI level on propagated records.
    mocker.patch.object(library, "level", logging.DEBUG)
    owned_handlers = []

    def migration(_):
        owned_handlers.extend(
            handler for handler in root.handlers if handler not in handlers
        )
        library.debug("library_debug %s", "detail")
        library.warning("library_warning")

    mocker.patch("etl_pipeline.cli.initialize_database", side_effect=migration)
    verify = mocker.patch(
        "etl_pipeline.cli.verify_persisted_dataset",
        side_effect=ETLError("first run failed"),
    )
    with structlog.contextvars.bound_contextvars(
        run_id="caller", stage="caller", private="not a run field"
    ):
        caller_context = structlog.contextvars.get_contextvars()
        first = CliRunner().invoke(
            app,
            [
                "--input",
                str(source),
                "--output",
                str(tmp_path / "first"),
                "--log-level",
                "WARNING",
            ],
        )
        assert first.exit_code == 1
        assert root.handlers == handlers and root.level == level
        assert structlog.contextvars.get_contextvars() == caller_context
        first_events = [
            json.loads(line)
            for line in Path("logs/pipeline.log").read_text().splitlines()
        ]
        assert {event["severity"] for event in first_events} == {"warning", "error"}
        assert [event["event"] for event in first_events].count("library_warning") == 1

        verify.side_effect = None
        second = CliRunner().invoke(
            app,
            [
                "--input",
                str(source),
                "--output",
                str(tmp_path / "second"),
                "--log-level",
                "DEBUG",
            ],
        )
        assert second.exit_code == 0, second.output
        assert "completed successfully" in second.output
        assert root.handlers == handlers and root.level == level
        assert structlog.contextvars.get_contextvars() == caller_context

    events = [
        json.loads(line) for line in Path("logs/pipeline.log").read_text().splitlines()
    ]
    second_events = events[len(first_events) :]
    assert len({event["run_id"] for event in first_events}) == 1
    assert len({event["run_id"] for event in second_events}) == 1
    assert first_events[0]["run_id"] != second_events[0]["run_id"]
    assert all(
        event["run_id"] != "caller" and "private" not in event for event in events
    )
    assert [event["event"] for event in second_events].count("library_warning") == 1
    debug = next(
        event for event in second_events if event["event"] == "library_debug detail"
    )
    assert debug["severity"] == "debug" and debug["operation"] == "migration"
    assert "stage" not in debug
    assert len(owned_handlers) == 2
    assert all(
        isinstance(handler, logging.FileHandler) and handler.stream is None
        for handler in owned_handlers
    )


def test_stage_construction_does_not_configure_logging(tmp_path):
    root = logging.getLogger()
    logger = logging.getLogger("etl_pipeline.transform")
    handlers, level = logger.handlers[:], logger.level
    root_handlers, root_level = root.handlers[:], root.level
    TransformStage(log_level=logging.DEBUG)
    TransformStage(log_level=logging.ERROR)
    assert not (tmp_path / "logs").exists()
    assert (logger.handlers, logger.level) == (handlers, level)
    assert (root.handlers, root.level) == (root_handlers, root_level)


@pytest.mark.parametrize("fail_load", [False, True])
def test_log_close_failure_preserves_outcome_and_caller(
    cli_boundary, mocker, tmp_path, capsys, fail_load
):
    source, extract, _, load = cli_boundary
    extract.execute.return_value = []
    primary = LoadError("primary load failure")
    if fail_load:
        load.execute.side_effect = primary
    candidate = tmp_path / ".owned.duckdb"
    candidate.write_bytes(b"verified")
    output = tmp_path / "published.duckdb"
    mocker.patch("etl_pipeline.cli._reserve_candidate", return_value=candidate)
    mocker.patch("etl_pipeline.cli.publish_candidate", side_effect=publish_candidate)
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    closed = []
    original_close = logging.FileHandler.close

    def failed_close(handler):
        closed.append(handler)
        original_close(handler)
        raise OSError("injected close failure")

    mocker.patch.object(logging.FileHandler, "close", failed_close)
    with structlog.contextvars.bound_contextvars(run_id="caller", stage="caller"):
        context = structlog.contextvars.get_contextvars()
        if fail_load:
            with pytest.raises(typer.Exit) as caught:
                run_etl(source, output, False, "DEBUG")
            assert caught.value.exit_code == 1
            assert caught.value.__cause__ is primary
            assert not output.exists()
        else:
            run_etl(source, output, False, "DEBUG")
            assert output.read_bytes() == b"verified"
        assert structlog.contextvars.get_contextvars() == context
        assert root.level == level and root.handlers == handlers
    assert len(closed) == 1 and closed[0] not in root.handlers
    assert (
        "Warning: Could not close pipeline log: injected close failure"
        in capsys.readouterr().err
    )
