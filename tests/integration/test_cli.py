import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typer.testing import CliRunner

# Import the app instance from the new location within the package
from etl_pipeline.cli import app
from etl_pipeline.exceptions import ETLError

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
    engine = create_engine(f"duckdb:///{output_db_path}")
    try:
        test_session_factory = sessionmaker(bind=engine)
        with test_session_factory() as session:
            assert (
                session.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one()
                == "6accd1b8062d"
            )
            # Use text() for raw SQL execution via SQLAlchemy session
            storm_count_result = session.execute(
                text("SELECT COUNT(*) FROM storms")
            ).scalar_one_or_none()
            obs_count_result = session.execute(
                text("SELECT COUNT(*) FROM observations")
            ).scalar_one_or_none()

            storm_count = storm_count_result if storm_count_result is not None else 0
            obs_count = obs_count_result if obs_count_result is not None else 0

            # The fixture contains KAREN's 19 and OPHELIA's 15 observations.
            assert storm_count == 2, f"Expected 2 storms, found {storm_count}"
            assert obs_count == 34, f"Expected 34 observations, found {obs_count}"
            assert session.execute(
                text(
                    "SELECT storm_id, basin, cyclone_number, year, name "
                    "FROM storms ORDER BY storm_id"
                )
            ).all() == [
                ("AL122007", "AL", 12, 2007, "KAREN"),
                ("AL162023", "AL", 16, 2023, "OPHELIA"),
            ]

            # test_data.txt lines 8 and 30; ref/schema_def.md defines UTC,
            # knots, millibars, then NE/SE/SW/NW radii for 34/50/64 kt (nm).
            # SQL TIMESTAMP is documented as UTC in today's schema; interpret
            # it explicitly as UTC rather than using the host's local timezone.
            rows = session.execute(
                text("""
                SELECT storm_id, date, record_identifier,
                       status, max_wind, max_wind_mph, min_pressure,
                       ne34, se34, sw34, nw34, ne50, se50, sw50, nw50,
                       ne64, se64, sw64, nw64, max_wind_radius, geom
                FROM observations
                WHERE (storm_id = 'AL122007' AND geom = 'POINT(-42.4 11.7)')
                   OR (storm_id = 'AL162023' AND record_identifier = 'L')
                ORDER BY storm_id
            """)
            ).all()
            assert all(row[1].tzinfo is None for row in rows)
            utc_rows = [(row[0], row[1].replace(tzinfo=UTC), *row[2:]) for row in rows]
            # 65 * 1.15078 = 74.8007 -> 74.8; 60 * 1.15078 = 69.0468 -> 69.0.
            # FLOAT storage may introduce binary error, but not a rounding digit.
            assert utc_rows == [
                (
                    "AL122007",
                    datetime(2007, 9, 26, 12, tzinfo=UTC),
                    None,
                    "HU",
                    65,
                    pytest.approx(74.8, abs=0.00001),
                    988,
                    90,
                    60,
                    40,
                    45,
                    60,
                    40,
                    25,
                    30,
                    40,
                    30,
                    0,
                    15,
                    None,
                    "POINT(-42.4 11.7)",
                ),
                (
                    "AL162023",
                    datetime(2023, 9, 23, 10, 15, tzinfo=UTC),
                    "L",
                    "TS",
                    60,
                    pytest.approx(69.0, abs=0.00001),
                    981,
                    270,
                    120,
                    90,
                    80,
                    40,
                    40,
                    50,
                    60,
                    0,
                    0,
                    0,
                    0,
                    30,
                    "POINT(-77.1 34.7)",
                ),
            ]
    except Exception as db_err:
        pytest.fail(f"Failed to query output database: {db_err}")
    finally:
        engine.dispose()

    # 4. Assert log file was created
    assert LOG_FILE.exists(), f"Log file not found at {LOG_FILE}"

    # 5. Assert basic content in the log file (optional, but good practice)
    try:
        log_content = LOG_FILE.read_text()
        # Check for messages logged by the stages
        assert "Starting execution of stage: extract" in log_content
        assert "Completed execution of stage: extract" in log_content
        assert "Starting execution of stage: transform" in log_content
        assert "Completed execution of stage: transform" in log_content
        assert "Starting execution of stage: load" in log_content
        assert "Completed execution of stage: load" in log_content
        # Ensure no major errors were logged (simple check)
        assert "ERROR" not in log_content.upper()
        assert "CRITICAL" not in log_content.upper()
    except Exception as log_err:
        pytest.fail(f"Failed to read or verify log file content: {log_err}")


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
