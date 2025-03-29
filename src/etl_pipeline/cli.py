import logging
from functools import partial
from pathlib import Path

import typer
from rich.console import Console
from sqlalchemy import create_engine, text  # Import text
from sqlalchemy.orm import sessionmaker

from .exceptions import (
    ETLError,
    ExtractionError,
    LoadError,
    TransformError,
    ValidationError,
)

# Import ETL stages and components using relative imports
from .extract.extract import ExtractStage
from .load.load import LoadStage
from .load.unit_of_work import SqlAlchemyUnitOfWork
from .transform.models import Storm as PydanticStorm  # Import Pydantic Storm model
from .transform.transform import TransformStage

# Create a Rich Console instance to be shared across stages for unified output
console = Console()

app = typer.Typer()


@app.command()
def run_etl(
    input_file: Path = typer.Option(  # noqa: B008
        ...,
        "--input",
        "-i",
        help="Path to the input HURDAT2 CSV file.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
    ),
    output_db: Path = typer.Option(  # noqa: B008
        ...,
        "--output",
        "-o",
        help="Path to the output DuckDB database file.",
        file_okay=True,
        dir_okay=False,
        writable=True,  # Check if the directory is writable
        resolve_path=True,
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
        case_sensitive=False,  # Allow lowercase level names
    ),
) -> None:
    """
    Runs the ETL pipeline to process HURDAT2 data and load it into a DuckDB database.
    """
    # Determine the numeric log level from the string option
    numeric_log_level = getattr(logging, log_level.upper(), logging.INFO)

    # Print initial messages to the console
    console.print(f"Starting ETL process with log level {log_level.upper()}...")
    console.print(f"Input file: [cyan]{input_file}[/]")
    console.print(f"Output database: [cyan]{output_db}[/]")
    # Log file location is fixed in BaseLogger (logs/pipeline.log)
    console.print("Logging to: [cyan]logs/pipeline.log[/]")

    # --- Delete existing output DB if it exists ---
    if output_db.exists():
        console.print(
            f"[yellow]Output database file exists. Deleting {output_db}...[/]"
        )
        try:
            output_db.unlink()  # Use unlink() for Path objects
        except OSError as e:
            console.print(f"[bold red]Error deleting existing database file:[/]\n{e}")
            raise typer.Exit(code=1) from e

    # --- Instantiate ETL Stages ---
    # Pass the shared console and the determined log level to each stage.
    # Each stage will configure its own logger via ETLStage -> BaseLogger.
    extract_stage = ExtractStage(console=console, log_level=numeric_log_level)
    transform_stage = TransformStage(console=console, log_level=numeric_log_level)

    # Create a session factory specifically for this run, using the output path
    engine = create_engine(f"duckdb:///{output_db}")
    dynamic_session_factory = sessionmaker(bind=engine)

    # Create the UoW factory using the dynamically created session factory
    uow_factory = partial(SqlAlchemyUnitOfWork, session_factory=dynamic_session_factory)
    load_stage = LoadStage(
        uow_factory=uow_factory, console=console, log_level=numeric_log_level
    )

    try:
        # --- Execute Extract Stage ---
        console.print("\n--- Running Extract Stage ---")
        # Pass input data as expected by ExtractStage.execute
        extract_input = {"file_path": str(input_file)}
        # The execute method handles logging start/end internally
        raw_data_iterator = extract_stage.execute(extract_input)
        # Materialize the iterator into a list for the Transform stage
        console.print("Materializing raw data from extractor...")
        raw_data_list = list(raw_data_iterator)
        console.print(f"Materialized {len(raw_data_list)} raw data rows.")
        # Note: Progress bar was handled during iteration by ExtractStage

        # --- Execute Transform Stage ---
        console.print("\n--- Running Transform Stage ---")
        # Pass the materialized list to TransformStage.execute
        transformed_storms = transform_stage.execute(raw_data_list)
        console.print(
            f"Transform stage completed. Produced {len(transformed_storms)} "
            f"storm objects (pre-deduplication)."
        )

        # --- Prepare Data for Load Stage ---
        # De-duplicate storms one final time using storm_id as the key
        # This ensures absolute uniqueness before loading.
        unique_storms_dict: dict[str, PydanticStorm] = {
            storm.storm_id: storm for storm in transformed_storms
        }
        unique_storms_list = list(unique_storms_dict.values())
        console.print(f"De-duplicated to {len(unique_storms_list)} unique storms.")

        # Extract observations ONLY from the unique storms
        all_observations = [
            obs for storm in unique_storms_list for obs in storm.observations
        ]
        load_input = (unique_storms_list, all_observations)
        console.print(
            f"Prepared {len(all_observations)} observations from unique storms "
            f"for loading."
        )

        # --- Execute Load Stage ---
        console.print("\n--- Running Load Stage ---")
        # LoadStage.execute expects the tuple (storms, observations)
        storm_count, observation_count = load_stage.execute(load_input)
        console.print(
            f"Load stage completed. Loaded {storm_count} storms and "
            f"{observation_count} observations."
        )

        console.print("\n[bold green]ETL pipeline finished successfully.[/]")

        # --- Generate Summary Report ---
        console.print("\n--- Generating Summary Report ---")
        try:
            # Use SQLAlchemy to connect for consistency
            report_engine = create_engine(f"duckdb:///{output_db}")
            report_session_factory = sessionmaker(bind=report_engine)
            with report_session_factory() as report_session:
                # Fetch counts safely using SQLAlchemy session
                storm_count_result = report_session.execute(
                    text("SELECT COUNT(*) FROM storms")
                ).scalar_one_or_none()
                total_storms = (
                    storm_count_result if storm_count_result is not None else 0
                )

                obs_count_result = report_session.execute(
                    text("SELECT COUNT(*) FROM observations")
                ).scalar_one_or_none()
                total_observations = (
                    obs_count_result if obs_count_result is not None else 0
                )

                # Fetch date range safely using SQLAlchemy session
                date_range_result = report_session.execute(
                    text("SELECT MIN(date), MAX(date) FROM observations")
                ).fetchone()

                console.print(f"Total storms loaded: {total_storms}")
                console.print(f"Total observations loaded: {total_observations}")

                # Check if date range was fetched and contains valid dates
                if date_range_result and date_range_result[0] and date_range_result[1]:
                    console.print(
                        f"Observation date range: {date_range_result[0]} to "
                        f"{date_range_result[1]}"
                    )
                else:
                    console.print(
                        "Observation date range: Not available (no observations found?)"
                    )
        except Exception as report_err:
            console.print(
                f"[bold yellow]Warning: Could not generate summary report:[/]\n"
                f"{report_err}"
            )

    except ETLError as e:
        # Determine stage name based on exception type
        stage_name = "unknown"
        if isinstance(e, ExtractionError):
            stage_name = "extract"
        elif isinstance(e, TransformError):
            stage_name = "transform"
        elif isinstance(e, LoadError):
            stage_name = "load"
        elif isinstance(e, ValidationError):  # Handle validation errors too
            stage_name = "transform"  # Validation happens during transform

        # Log the error using the logger associated with the determined stage
        # Use the package logger name now
        logger = logging.getLogger(f"etl_pipeline.{stage_name}")
        logger.error(f"ETL Error occurred in stage '{stage_name}': {e}", exc_info=True)
        console.print(f"\n[bold red]ETL Error in stage '{stage_name}':[/] {e}")
        raise typer.Exit(code=1) from e
    except Exception as e:
        # Log unexpected errors using a generic logger name (cli logger)
        logger = logging.getLogger("etl_pipeline.cli")  # Use package logger name
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        console.print(f"\n[bold red]An unexpected error occurred:[/]\n{e}")
        raise typer.Exit(code=1) from e


# This allows running the script directly for debugging, but the primary
# entry point should be configured in pyproject.toml
if __name__ == "__main__":
    app()
