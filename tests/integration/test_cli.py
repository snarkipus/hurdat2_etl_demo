import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typer.testing import CliRunner

# Import the app instance from the new location within the package
from etl_pipeline.cli import app

# Create a CliRunner instance to invoke commands
runner = CliRunner()

# Define the path to the test input data
TEST_DATA_DIR = Path(__file__).parent.parent / "unit" / "data"
TEST_INPUT_FILE = TEST_DATA_DIR / "test_data.txt"
LOG_FILE = Path("logs/pipeline.log")


@pytest.fixture(scope="function", autouse=True)
def ensure_log_file_removed():
    """Ensure log file is removed before and after each test function."""
    if LOG_FILE.exists():
        os.remove(LOG_FILE)
    yield  # Run the test
    if LOG_FILE.exists():
        os.remove(LOG_FILE)


def test_run_etl_success(tmp_path):
    """
    Test the 'run-etl' command with valid inputs, expecting success.
    """
    output_db_path = tmp_path / "test_output.duckdb"

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

            traceback.print_tb(result.exc_info[2])

    # 1. Assert successful execution
    assert result.exit_code == 0, (
        f"CLI command failed with exit code {result.exit_code}"
    )

    # 2. Assert output database file was created
    assert output_db_path.exists(), (
        f"Output database file not created at {output_db_path}"
    )

    # 3. Assert basic content in the output database using SQLAlchemy
    try:
        engine = create_engine(f"duckdb:///{output_db_path}")
        test_session_factory = sessionmaker(bind=engine)
        with test_session_factory() as session:
            # Use text() for raw SQL execution via SQLAlchemy session
            storm_count_result = session.execute(
                text("SELECT COUNT(*) FROM storms")
            ).scalar_one_or_none()
            obs_count_result = session.execute(
                text("SELECT COUNT(*) FROM observations")
            ).scalar_one_or_none()

            storm_count = storm_count_result if storm_count_result is not None else 0
            obs_count = obs_count_result if obs_count_result is not None else 0

            # Assert counts based on previous successful log output
            assert storm_count == 2, f"Expected 2 storms, found {storm_count}"
            assert obs_count == 34, f"Expected 34 observations, found {obs_count}"
    except Exception as db_err:
        pytest.fail(f"Failed to query output database: {db_err}")

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
