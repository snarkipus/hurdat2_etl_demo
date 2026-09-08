import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .core import ProgressManager
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
from .load.verification import verify_persisted_dataset
from .migrations import initialize_database
from .transform.transform import TransformStage

app = typer.Typer()


def _dispose_engine(engine: Engine) -> None:
    """Close the owned pool without replacing an already active failure."""
    primary_error = sys.exception()
    try:
        engine.dispose()
    except Exception as error:
        if primary_error is None:
            raise
        primary_error.add_note(f"Database engine disposal failed: {error}")


@contextmanager
def _report_session(output_db: Path) -> Iterator[Session]:
    """Own summary resources without masking a query failure during cleanup."""
    engine = create_engine(f"duckdb:///{output_db}")
    try:
        session = sessionmaker(bind=engine)()
        try:
            yield session
        finally:
            primary_error = sys.exception()
            try:
                session.close()
            except Exception as error:
                if primary_error is None:
                    raise
                primary_error.add_note(f"Report session close failed: {error}")
    finally:
        _dispose_engine(engine)


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
        writable=True,
        resolve_path=True,
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
        case_sensitive=False,
    ),
) -> None:
    """
    Runs the ETL pipeline to process HURDAT2 data and load it into a DuckDB database.
    """
    # Create a clearly styled console for the entire pipeline
    console = Console(highlight=False)  # Disable syntax highlighting for cleaner output

    # Determine the numeric log level from the string option
    numeric_log_level = getattr(logging, log_level.upper(), logging.INFO)

    # Print initial messages to the console with cleaner formatting
    console.print(
        Panel(
            f"""[bold]Input file:[/bold] [cyan]{input_file}[/cyan]
[bold]Output database:[/bold] [cyan]{output_db}[/cyan]
[bold]Log level:[/bold] [cyan]{log_level.upper()}[/cyan]
[bold]Log file:[/bold] [cyan]logs/pipeline.log[/cyan]""",
            title="[bold]HURDAT2 ETL Pipeline[/bold]",
            border_style="blue",
            padding=(1, 2),
        )
    )

    # --- Delete existing output DB if it exists ---
    if output_db.exists():
        console.print(
            f"[yellow]Output database file exists. Deleting {output_db}...[/]"
        )
        try:
            output_db.unlink()
        except OSError as e:
            console.print(f"[bold red]Error deleting existing database file:[/]\n{e}")
            raise typer.Exit(code=1) from e

    # Create the progress manager with our console
    progress_manager = ProgressManager(console=console)

    # Create a session factory for this run
    engine = create_engine(f"duckdb:///{output_db}")
    try:
        with engine.begin() as connection:
            initialize_database(connection)
        dynamic_session_factory = sessionmaker(bind=engine)
        uow_factory = partial(
            SqlAlchemyUnitOfWork, session_factory=dynamic_session_factory
        )
        # Start the progress directly
        progress_manager.progress.start()

        # --- Execute Extract Stage ---
        progress_manager.add_task("extract_stage", "[bold]Extract Stage[/bold]", 100)
        extract_stage = ExtractStage(
            console=console,
            progress_manager=progress_manager,
            log_level=numeric_log_level,
        )

        extract_input = {"file_path": str(input_file)}
        progress_manager.update("extract_stage", advance=10)
        raw_data_iterator = extract_stage.execute(extract_input)

        # Materialize the iterator
        raw_data_list = list(raw_data_iterator)
        progress_manager.update("extract_stage", advance=90)
        progress_manager.complete_task(
            "extract_stage", f"Extract: {len(raw_data_list):,} rows processed"
        )

        # --- Execute Transform Stage ---
        progress_manager.add_task(
            "transform_stage", "[bold]Transform Stage[/bold]", 100
        )
        transform_stage = TransformStage(
            console=console,
            progress_manager=progress_manager,
            log_level=numeric_log_level,
        )

        progress_manager.update("transform_stage", advance=10)
        transformed_storms = transform_stage.execute(raw_data_list)
        progress_manager.update("transform_stage", advance=90)
        progress_manager.complete_task(
            "transform_stage",
            f"Transform: {len(transformed_storms):,} storms processed",
        )

        # --- Prepare Data for Load Stage ---
        # De-duplicate storms
        unique_storms_dict = {storm.storm_id: storm for storm in transformed_storms}
        unique_storms_list = list(unique_storms_dict.values())

        # Extract observations from unique storms
        all_observations = [
            obs for storm in unique_storms_list for obs in storm.observations
        ]
        load_input = (unique_storms_list, all_observations)

        # --- Execute Load Stage ---
        progress_manager.add_task("load_stage", "[bold]Load Stage[/bold]", 100)
        load_stage = LoadStage(
            uow_factory=uow_factory,
            console=console,
            progress_manager=progress_manager,
            log_level=numeric_log_level,
        )

        progress_manager.update("load_stage", advance=10)
        expected_storms = len(unique_storms_list)
        expected_observations = len(all_observations)
        storm_count, observation_count = load_stage.execute(load_input)
        verify_persisted_dataset(engine, expected_storms, expected_observations)
        progress_manager.update("load_stage", advance=90)
        progress_manager.complete_task(
            "load_stage",
            f"Load: {storm_count:,} storms, {observation_count:,} observations",
        )

        # Stop the progress before continuing with other output
        progress_manager.progress.stop()
        console.print("[bold green]✓ ETL pipeline completed successfully[/bold green]")

        # --- Generate Summary Report ---
        with console.status(
            "[bold cyan]✨ Generating final summary report...[/bold cyan]"
        ):
            try:
                # Use SQLAlchemy to connect for consistency
                with _report_session(output_db) as report_session:
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

                    # Get basin counts
                    basin_counts = report_session.execute(
                        text("""
                            SELECT 
                                basin, 
                                COUNT(*) 
                            FROM storms 
                            GROUP BY basin 
                            ORDER BY COUNT(*) DESC
                        """)
                    ).fetchall()

                    # Group storms by actual decade (1850s, 1860s, etc)
                    decade_query = text("""
                        WITH decades AS (
                            SELECT 
                                CAST(FLOOR(year / 10) * 10 AS INTEGER) AS decade_start,
                                COUNT(*) AS storm_count
                            FROM 
                                storms
                            GROUP BY 
                                decade_start
                            ORDER BY 
                                decade_start
                        )
                        SELECT 
                            decade_start,
                            storm_count
                        FROM 
                            decades
                    """)
                    decade_stats = report_session.execute(decade_query).fetchall()

                    # Get max wind speeds distribution
                    max_wind_query = text("""
                        WITH storm_max_winds AS (
                            SELECT 
                                s.storm_id,
                                s.name,
                                MAX(o.max_wind) AS max_wind
                            FROM 
                                storms s
                            JOIN 
                                observations o ON s.storm_id = o.storm_id
                            GROUP BY 
                                s.storm_id, s.name
                        )
                        SELECT 
                            CASE 
                                WHEN max_wind < 64 THEN 'Tropical Storm'
                                WHEN max_wind >= 64 AND max_wind < 83 THEN 'Category 1'
                                WHEN max_wind >= 83 AND max_wind < 96 THEN 'Category 2'
                                WHEN max_wind >= 96 AND max_wind < 113 THEN 'Category 3'
                                WHEN max_wind >= 113 AND max_wind < 137 THEN
                                    'Category 4'
                                WHEN max_wind >= 137 THEN 'Category 5'
                                ELSE 'Unknown'
                            END AS intensity_category,
                            COUNT(*) AS count
                        FROM 
                            storm_max_winds
                        GROUP BY 
                            intensity_category
                        ORDER BY 
                            CASE 
                                WHEN intensity_category = 'Tropical Storm' THEN 1
                                WHEN intensity_category = 'Category 1' THEN 2
                                WHEN intensity_category = 'Category 2' THEN 3
                                WHEN intensity_category = 'Category 3' THEN 4
                                WHEN intensity_category = 'Category 4' THEN 5
                                WHEN intensity_category = 'Category 5' THEN 6
                                ELSE 0
                            END
                    """)
                    intensity_stats = report_session.execute(max_wind_query).fetchall()

                    # Get longest-duration storms
                    longest_storms_query = text("""
                        WITH storm_durations AS (
                            SELECT 
                                s.storm_id, 
                                s.name,
                                s.year,
                                CAST(
                                    EXTRACT(
                                        EPOCH FROM (MAX(o.date) - MIN(o.date))
                                    ) / 86400 
                                    AS FLOAT
                                ) AS duration_days
                            FROM 
                                storms s
                            JOIN 
                                observations o ON s.storm_id = o.storm_id
                            GROUP BY 
                                s.storm_id, s.name, s.year
                        )
                        SELECT 
                            name, 
                            year,
                            duration_days
                        FROM 
                            storm_durations
                        ORDER BY 
                            duration_days DESC
                        LIMIT 5
                    """)
                    longest_storms = report_session.execute(
                        longest_storms_query
                    ).fetchall()

                    # Create a more visually appealing summary panel with sections
                    summary_text = f"""[bold green]HURDAT2 DATABASE SUMMARY[/bold green]

[bold]Basic Statistics[/bold]
    [bold]Total storms:[/bold]        [cyan]{total_storms:,}[/cyan]
    [bold]Total observations:[/bold]  [cyan]{total_observations:,}[/cyan]"""

                    # Add date range if available
                    if (
                        date_range_result
                        and date_range_result[0]
                        and date_range_result[1]
                    ):
                        summary_text += f"""
    [bold]Observation range:[/bold]   [cyan]{date_range_result[0]}[/cyan] to 
    [cyan]{date_range_result[1]}[/cyan]"""
                    else:
                        summary_text += """
    [bold]Observation range:[/bold]   [yellow]Not available[/yellow]"""

                    # Add basin distribution if available
                    if basin_counts:
                        summary_text += """

[bold]Basin Distribution[/bold]"""
                        for basin, count in basin_counts:
                            summary_text += f"""
    [magenta]{basin}:[/magenta] {count:,} storms"""

                    # Add decade distribution
                    if decade_stats:
                        summary_text += """

[bold]Storms by Decade[/bold]"""
                        # Group decades into larger blocks
                        # (combine years for cleaner display)
                        by_larger_groups: dict[int, list[tuple[int, int]]] = {}
                        for decade, count in decade_stats:
                            # Group into 30-year periods
                            period = (decade // 30) * 30
                            if period not in by_larger_groups:
                                by_larger_groups[period] = []
                            by_larger_groups[period].append((decade, count))

                        # Display decade stats grouped for better readability
                        for period in sorted(by_larger_groups.keys()):
                            decades_in_period = by_larger_groups[period]
                            for decade, count in decades_in_period:
                                summary_text += f"""
    [yellow]{decade}s:[/yellow] {count:,} storms"""

                    # Add intensity categories
                    if intensity_stats:
                        summary_text += """

[bold]Hurricane Intensity Distribution[/bold]"""
                        for category, count in intensity_stats:
                            # Color-code categories based on severity
                            if category == "Tropical Storm":
                                color = "blue"
                            elif category == "Category 1":
                                color = "green"
                            elif category == "Category 2":
                                color = "yellow"
                            elif category == "Category 3":
                                color = "orange"
                            elif category == "Category 4" or category == "Category 5":
                                color = "red"
                            else:
                                color = "white"

                            summary_text += f"""
    [{color}]{category}:[/{color}] {count:,} storms"""

                    # Add longest lasting storms
                    if longest_storms:
                        summary_text += """

[bold]Longest-Duration Storms[/bold]"""
                        for name, year, duration in longest_storms:
                            # Handle unnamed storms
                            storm_name = (
                                name
                                if name and name.strip() != "UNNAMED"
                                else "UNNAMED"
                            )
                            summary_text += f"""
    [cyan]{storm_name} ({year}):[/cyan] {duration:.1f} days"""

                    # Print the summary panel with expanded sizing
                    console.print(
                        Panel(
                            summary_text,
                            title="[bold]✅ HURDAT2 ETL Summary[/bold]",
                            border_style="green",
                            padding=(1, 2),
                            expand=False,
                            width=90,
                        )
                    )

            except Exception as report_err:
                console.print(
                    "[bold yellow]Warning: Could not generate summary report:[/]\n"
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
        logger = logging.getLogger(f"etl_pipeline.{stage_name}")
        logger.error(f"ETL Error occurred in stage '{stage_name}': {e}", exc_info=True)
        console.print(f"\n[bold red]ETL Error in stage '{stage_name}':[/] {e}")
        raise typer.Exit(code=1) from e

    except Exception as e:
        # Log unexpected errors using a generic logger name (cli logger)
        logger = logging.getLogger("etl_pipeline.cli")
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        console.print(f"\n[bold red]An unexpected error occurred:[/]\n{e}")
        raise typer.Exit(code=1) from e
    finally:
        # Closing the last DuckDB connection checkpoints committed state; no
        # separate CHECKPOINT is needed for this single-writer lifecycle.
        _dispose_engine(engine)


# This allows running the script directly for debugging, but the primary
# entry point should be configured in pyproject.toml
if __name__ == "__main__":
    app()
