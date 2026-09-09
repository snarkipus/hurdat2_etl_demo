import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError
from typer.testing import CliRunner

# Import the app instance from the new location within the package
from etl_pipeline.cli import app
from etl_pipeline.exceptions import ETLError, LoadError
from etl_pipeline.load.load import LoadStage
from tests.baseline import assert_baseline

# Create a CliRunner instance to invoke commands
runner = CliRunner()

# Define the path to the test input data
TEST_DATA_DIR = Path(__file__).parent.parent / "unit" / "data"
TEST_INPUT_FILE = TEST_DATA_DIR / "test_data.txt"
LOG_FILE = Path("logs/pipeline.log")


@pytest.fixture(autouse=True)
def cli_engines(monkeypatch, mocker):
    """Observe runtime disposal without closing engines on its behalf."""
    engines = []

    def tracked_create_engine(*args, **kwargs):
        engine = create_engine(*args, **kwargs)
        mocker.spy(engine, "dispose")
        engines.append(engine)
        return engine

    monkeypatch.setattr("etl_pipeline.cli.create_engine", tracked_create_engine)
    return engines


@pytest.mark.parametrize("replace", [False, True])
def test_run_etl_success(tmp_path, cli_engines, replace):
    """
    Test the 'run-etl' command with valid inputs, expecting success.
    """
    output_db_path = tmp_path / "test_output.duckdb"
    if replace:
        output_db_path.write_bytes(b"previous output must survive until publication")

    # Ensure the test input file exists
    assert TEST_INPUT_FILE.exists(), f"Test input file not found: {TEST_INPUT_FILE}"

    # --- Test --help first ---
    result_help = runner.invoke(app, ["--help"])
    print("Help Output:\n", result_help.stdout)
    assert result_help.exit_code == 0, "Invoking --help failed"
    # Adjust assertion to match the actual help output for a single-command app
    assert "Usage: run-etl [OPTIONS]" in result_help.stdout

    # --- Invoke the actual run-etl command ---
    # Revert to invoking the app with the command name string
    result = runner.invoke(
        app,
        [
            # Omit command name for single-command app test
            "--input",
            str(TEST_INPUT_FILE),
            "--output",
            str(output_db_path),
            "--log-level",
            "DEBUG",  # Use DEBUG for more detailed logs during testing
            *(["--replace"] if replace else []),
        ],
        # catch_exceptions=True is the default, allows stderr capture
    )

    # Print CLI output for debugging if the test fails
    if result.exit_code != 0:
        print("CLI Exit Code:", result.exit_code)
        print("CLI Stdout:\n", result.stdout)
        print("CLI Stderr:\n", result.stderr)  # Should be captured now
        if result.exception:
            print("CLI Exception:\n", result.exception)
            import traceback

            traceback.print_tb(result.exception.__traceback__)

    # 1. Assert successful execution
    assert result.exit_code == 0, (
        f"CLI command failed with exit code {result.exit_code}"
    )

    # 2. Assert output database file was created
    assert output_db_path.exists(), (
        f"Output database file not created at {output_db_path}"
    )

    # Runtime must release both pools, without test-assisted finalization.
    assert len(cli_engines) == 2
    for engine in cli_engines:
        engine.dispose.assert_called_once_with()
        assert engine.pool.checkedout() == 0
    assert not Path(f"{output_db_path}.wal").exists()
    assert not list(tmp_path.glob(".test_output.duckdb.*"))
    # A separate process cannot borrow a pooled writer or its in-memory state.
    reopened = subprocess.run(  # noqa: S603 - fixed Python code and test-owned path
        [
            sys.executable,
            "-c",
            "import duckdb, sys; "
            "c = duckdb.connect(sys.argv[1], read_only=True); "
            "assert c.execute('SELECT count(*) FROM observations').fetchone() == (34,); "
            "assert c.execute('SELECT version_num FROM alembic_version').fetchone() "
            "== ('6accd1b8062d',); c.close()",
            str(output_db_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        # This probe runs only third-party DuckDB, not application code. Avoid
        # pytest-cov starting a second collector without config in the isolated cwd.
        env={
            key: value
            for key, value in os.environ.items()
            if not key.startswith("COV_CORE_")
        },
    )
    assert reopened.returncode == 0, reopened.stderr

    # 3. Assert source-derived content, not summary/loader return values.
    assert_baseline(output_db_path)

    # 4. Assert log file was created
    assert LOG_FILE.exists(), f"Log file not found at {LOG_FILE}"

    # Reuse the independently reopened value run for the diagnostic contract.
    events = [json.loads(line) for line in LOG_FILE.read_text().splitlines()]
    assert len({event["run_id"] for event in events}) == 1
    for event in events:
        assert datetime.fromisoformat(event["timestamp"]).utcoffset() == timedelta(0)
        assert event["severity"] not in {"error", "critical"}
        assert event["operation"]
        assert "\x1b" not in event["event"]
    outcomes = [
        event["event"] for event in events if event["logger"] == "etl_pipeline.cli"
    ]
    assert outcomes == [
        "preflight_completed",
        "migration_completed",
        "extraction_completed",
        "transformation_completed",
        "loading_completed",
        "verification_completed",
        "finalization_completed",
        "publication_completed",
    ]
    for stage in ("extract", "transform", "load"):
        stage_events = [event for event in events if event.get("stage") == stage]
        assert any(
            event["event"] == f"Starting execution of stage: {stage}"
            for event in stage_events
        )
        assert any(
            event["event"] == f"Completed execution of stage: {stage}"
            for event in stage_events
        )
    assert any(
        event["logger"].startswith("alembic.") and event["operation"] == "migration"
        for event in events
    )
    assert all(engine.hide_parameters for engine in cli_engines)


def test_wrapped_observation_batch_failure_hides_bound_parameters(tmp_path, mocker):
    """Exercise real ORM batching and DBAPI wrapping, not a pre-hidden exception."""
    execute = mocker.spy(LoadStage, "execute")

    def fail_observation_insert(
        connection, cursor, statement, parameters, context, many
    ):
        # Keep the real bound observation batch; make the DBAPI reject its SQL.
        return statement.replace(
            "INSERT INTO observations", "INSERT INTO missing_observations"
        ), parameters

    sqlalchemy_event.listen(
        Engine, "before_cursor_execute", fail_observation_insert, retval=True
    )
    output = tmp_path / "output.duckdb"
    try:
        result = runner.invoke(
            app,
            [
                "--input",
                str(TEST_INPUT_FILE),
                "--output",
                str(output),
                "--log-level",
                "DEBUG",
            ],
        )
    finally:
        sqlalchemy_event.remove(
            Engine, "before_cursor_execute", fail_observation_insert
        )
    assert result.exit_code == 1
    assert not output.exists()
    wrapped = execute.spy_exception
    assert isinstance(wrapped, LoadError)
    database_error = wrapped.__cause__
    assert isinstance(database_error, DBAPIError)
    assert database_error.params and "AL122007" in str(database_error.params)
    assert len(database_error.params) > 1
    assert "POINT(-35.9 10.0)" in str(database_error.params)
    assert database_error.hide_parameters is True
    logs = LOG_FILE.read_text()
    assert "AL122007" not in logs + result.output
    assert "POINT(-35.9 10.0)" not in logs + result.output
    assert "SQL parameters hidden due to hide_parameters=True" in logs
    events = [json.loads(line) for line in logs.splitlines()]
    errors = [entry for entry in events if entry["severity"] == "error"]
    assert any(entry.get("error_type") == "LoadError" for entry in errors)
    assert any("missing_observations" in entry.get("exception", "") for entry in errors)


@pytest.mark.parametrize("failure", ["verification", "publication"])
def test_failed_replacement_preserves_old_output(
    tmp_path, cli_engines, mocker, failure
):
    output = tmp_path / "old.duckdb"
    output.write_bytes(b"old output")
    if failure == "verification":
        mocker.patch(
            "etl_pipeline.cli.verify_persisted_dataset",
            side_effect=ETLError("verification refused"),
        )
    else:
        mocker.patch(
            "etl_pipeline.publication.os.replace",
            side_effect=PermissionError("publication denied"),
        )
    result = runner.invoke(
        app, ["--input", str(TEST_INPUT_FILE), "--output", str(output), "--replace"]
    )
    assert result.exit_code == 1, result.output
    assert failure in result.output
    assert "completed successfully" not in result.output
    assert output.read_bytes() == b"old output"
    assert len(cli_engines) == 1
    cli_engines[0].dispose.assert_called_once_with()
    assert cli_engines[0].pool.checkedout() == 0
    candidates = list(tmp_path.glob(".old.duckdb.*"))
    if failure == "verification":
        assert candidates == []
    else:
        assert len(candidates) == 1  # Standalone database, no candidate WAL.
        candidate = candidates[0]
        assert str(candidate) in result.output.replace("\n", "")
        engine = create_engine(
            f"duckdb:///{candidate}", connect_args={"read_only": True}
        )
        try:
            with engine.connect() as connection:
                assert (
                    connection.execute(text("SELECT count(*) FROM storms")).scalar_one()
                    == 2
                )
                assert (
                    connection.execute(
                        text("SELECT count(*) FROM observations")
                    ).scalar_one()
                    == 34
                )
                assert (
                    connection.execute(
                        text("SELECT version_num FROM alembic_version")
                    ).scalar_one()
                    == "6accd1b8062d"
                )
        finally:
            engine.dispose()
