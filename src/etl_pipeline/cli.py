import logging
import os
import shutil
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial
from pathlib import Path

import typer
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from structlog.contextvars import bind_contextvars, bound_contextvars

from .core import ProgressManager
from .diagnostics import run_diagnostics
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
from .publication import preflight_output, publish_candidate
from .transform.transform import TransformStage

app = typer.Typer()


def _warn(console: Console, message: str) -> None:
    """Keep optional presentation from changing the persistence outcome."""
    try:
        logging.getLogger(__name__).warning(
            message, exc_info=sys.exc_info()[0] is not None
        )
    except Exception:  # noqa: S110 - warnings must not replace the persistence outcome
        pass
    try:
        console.print(f"Warning: {message}", style="yellow", markup=False)
    except Exception:
        try:
            print(f"Warning: {message}", file=sys.stderr)
        except Exception:  # noqa: S110 - both diagnostic sinks have failed
            pass


def _cleanup_candidate(candidate: Path, console: Console) -> None:
    """Remove only this run's database and known transient artifacts."""
    for path in (Path(f"{candidate}.wal"), candidate):
        try:
            path.unlink(missing_ok=True)
        except OSError as error:
            _warn(console, f"Could not remove run-owned artifact {path}: {error}")
    temp_dir = Path(f"{candidate}.tmp")
    try:
        shutil.rmtree(temp_dir)
    except FileNotFoundError:
        pass
    except OSError as error:
        _warn(console, f"Could not remove run-owned artifact {temp_dir}: {error}")


def _reserve_candidate(output_db: Path, console: Console) -> Path:
    """Reserve a unique sibling name, then remove the invalid zero-byte file.

    The handle is closed before DuckDB opens the path. As with publication,
    this assumes an ordinary local directory, not hostile path mutation.
    """
    fd, name = tempfile.mkstemp(
        prefix=f".{output_db.name}.", suffix=".duckdb", dir=output_db.parent
    )
    candidate = Path(name)
    try:
        os.close(fd)
        candidate.unlink()
    except BaseException:
        _cleanup_candidate(candidate, console)
        raise
    return candidate


def _check_candidate_ready(candidate: Path) -> None:
    if not candidate.is_file() or os.path.lexists(f"{candidate}.wal"):
        raise ETLError(f"Candidate is not a finalized standalone database: {candidate}")


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
    engine = create_engine(
        f"duckdb:///{output_db}", connect_args={"read_only": True}, hide_parameters=True
    )
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
        dir_okay=True,  # Preflight diagnoses unsuitable lexical destination entries.
        resolve_path=False,
    ),
    replace: bool = typer.Option(
        False,
        "--replace",
        help="Allow replacement of an existing output database.",
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
    numeric_log_level = getattr(logging, log_level.upper(), logging.INFO)
    with run_diagnostics(numeric_log_level):
        _run_etl(input_file, output_db, replace, log_level)


def _run_etl(
    input_file: Path,
    output_db: Path,
    replace: bool,
    log_level: str,
) -> None:
    logger = logging.getLogger(__name__)
    # Create a clearly styled console for the entire pipeline
    console = Console(highlight=False)  # Disable syntax highlighting for cleaner output

    try:
        preflight_output(input_file, output_db, replace=replace)
    except ETLError as error:
        logger.error("preflight_failed", exc_info=True)
        console.print(f"Output preflight failed: {error}", style="red", markup=False)
        raise typer.Exit(code=1) from error

    logger.info("preflight_completed")

    # Print initial messages to the console with cleaner formatting
    console.print(
        Panel(
            f"""[bold]Input file:[/bold] [cyan]{escape(str(input_file))}[/cyan]
[bold]Output database:[/bold] [cyan]{escape(str(output_db))}[/cyan]
[bold]Log level:[/bold] [cyan]{escape(log_level.upper())}[/cyan]
[bold]Log file:[/bold] [cyan]logs/pipeline.log[/cyan]""",
            title="[bold]HURDAT2 ETL Pipeline[/bold]",
            border_style="blue",
            padding=(1, 2),
        )
    )

    # Create the progress manager with our console
    progress_manager = ProgressManager(console=console)

    candidate: Path | None = None
    engine: Engine | None = None
    ready = False
    published = False
    try:
        bind_contextvars(operation="candidate")
        candidate = _reserve_candidate(output_db, console)
        engine = create_engine(f"duckdb:///{candidate}", hide_parameters=True)
        bind_contextvars(operation="migration")
        with engine.begin() as connection:
            initialize_database(connection)
        logger.info("migration_completed")
        dynamic_session_factory = sessionmaker(bind=engine)
        uow_factory = partial(
            SqlAlchemyUnitOfWork, session_factory=dynamic_session_factory
        )
        # Start the progress directly
        progress_manager.progress.start()

        # --- Execute Extract Stage ---
        bind_contextvars(operation="extract", stage="extract")
        progress_manager.add_task("extract_stage", "[bold]Extract Stage[/bold]", 100)
        extract_stage = ExtractStage(
            console=console,
            progress_manager=progress_manager,
        )

        extract_input = {"file_path": str(input_file)}
        progress_manager.update("extract_stage", advance=10)
        raw_data_iterator = extract_stage.execute(extract_input)

        # Materialize the iterator
        raw_data_list = list(raw_data_iterator)
        logger.info("extraction_completed", extra={"rows": len(raw_data_list)})
        progress_manager.update("extract_stage", advance=90)
        progress_manager.complete_task(
            "extract_stage", f"Extract: {len(raw_data_list):,} rows processed"
        )

        # --- Execute Transform Stage ---
        bind_contextvars(operation="transform", stage="transform")
        progress_manager.add_task(
            "transform_stage", "[bold]Transform Stage[/bold]", 100
        )
        transform_stage = TransformStage(
            console=console,
            progress_manager=progress_manager,
        )

        progress_manager.update("transform_stage", advance=10)
        transformed_storms = transform_stage.execute(raw_data_list)
        logger.info(
            "transformation_completed", extra={"storms": len(transformed_storms)}
        )
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
        bind_contextvars(operation="load", stage="load")
        progress_manager.add_task("load_stage", "[bold]Load Stage[/bold]", 100)
        load_stage = LoadStage(
            uow_factory=uow_factory,
            console=console,
            progress_manager=progress_manager,
        )

        progress_manager.update("load_stage", advance=10)
        expected_storms = len(unique_storms_list)
        expected_observations = len(all_observations)
        storm_count, observation_count = load_stage.execute(load_input)
        logger.info(
            "loading_completed",
            extra={"storms": storm_count, "observations": observation_count},
        )
        bind_contextvars(operation="verification", stage=None)
        verify_persisted_dataset(engine, expected_storms, expected_observations)
        logger.info("verification_completed")
        progress_manager.update("load_stage", advance=90)
        progress_manager.complete_task(
            "load_stage",
            f"Load: {storm_count:,} storms, {observation_count:,} observations",
        )

        # Closing the last connection checkpoints committed DuckDB state.
        # Clear ownership before disposal so a failed close is not retried.
        owned_engine, engine = engine, None
        bind_contextvars(operation="finalization")
        _dispose_engine(owned_engine)
        _check_candidate_ready(candidate)
        ready = True
        logger.info("finalization_completed")
        bind_contextvars(operation="publication")
        publish_candidate(candidate, output_db, replace=replace)
        published = True
        logger.info("publication_completed", extra={"output": str(output_db)})
        with bound_contextvars(operation="cleanup"):
            _cleanup_candidate(candidate, console)

        # All remaining presentation is optional after atomic publication.
        bind_contextvars(operation="summary")
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

                    # Peak recorded wind per storm, without filtering source statuses.
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
                                WHEN max_wind < 34 THEN '<34 kt'
                                WHEN max_wind < 64 THEN '34-63 kt'
                                WHEN max_wind >= 64 AND max_wind < 83 THEN '64-82 kt'
                                WHEN max_wind >= 83 AND max_wind < 96 THEN '83-95 kt'
                                WHEN max_wind >= 96 AND max_wind < 113 THEN '96-112 kt'
                                WHEN max_wind >= 113 AND max_wind < 137 THEN
                                    '113-136 kt'
                                WHEN max_wind >= 137 THEN '>=137 kt'
                                ELSE 'Unknown'
                            END AS wind_band,
                            COUNT(*) AS count
                        FROM 
                            storm_max_winds
                        GROUP BY 
                            wind_band
                        ORDER BY 
                            CASE 
                                WHEN wind_band = '<34 kt' THEN 1
                                WHEN wind_band = '34-63 kt' THEN 2
                                WHEN wind_band = '64-82 kt' THEN 3
                                WHEN wind_band = '83-95 kt' THEN 4
                                WHEN wind_band = '96-112 kt' THEN 5
                                WHEN wind_band = '113-136 kt' THEN 6
                                WHEN wind_band = '>=137 kt' THEN 7
                                ELSE 0
                            END
                    """)
                    wind_stats = report_session.execute(max_wind_query).fetchall()

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

                    # Bands describe wind only, not tropical-cyclone classification.
                    if wind_stats:
                        summary_text += """

[bold]Peak Recorded Wind Distribution (all statuses)[/bold]
    Wind-speed bands only; not storm classifications."""
                        for band, count in wind_stats:
                            # Retain the existing wind-threshold colors.
                            if band in ("<34 kt", "34-63 kt"):
                                color = "blue"
                            elif band == "64-82 kt":
                                color = "green"
                            elif band == "83-95 kt":
                                color = "yellow"
                            elif band == "96-112 kt":
                                color = "orange1"
                            elif band in ("113-136 kt", ">=137 kt"):
                                color = "red"
                            else:
                                color = "white"

                            summary_text += f"""
    [{color}]{band}:[/{color}] {count:,} storms"""

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
    [cyan]{escape(storm_name)} ({year}):[/cyan] {duration:.1f} days"""

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
                _warn(console, f"Could not generate summary report: {report_err}")

    except (Exception, KeyboardInterrupt) as e:
        if published:
            _warn(console, f"Output published successfully; presentation failed: {e}")
            return
        if ready:
            _warn(
                console, f"Publication failed; verified candidate retained: {candidate}"
            )
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
        try:
            logger.error(
                f"ETL Error occurred in stage '{stage_name}': {e}",
                exc_info=True,
                extra={"candidate": str(candidate) if candidate else None},
            )
            console.print(
                f"ETL Error in stage '{stage_name}': {e}", style="red", markup=False
            )
        except Exception:
            print(f"ETL failed: {e}", file=sys.stderr)
        raise typer.Exit(code=1) from e
    finally:
        if engine is not None:
            _dispose_engine(engine)
        if candidate is not None and not ready:
            with bound_contextvars(operation="cleanup", stage=None):
                _cleanup_candidate(candidate, console)
        try:
            progress_manager.progress.stop()
        except Exception as error:
            _warn(console, f"Could not stop progress display: {error}")


# This allows running the script directly for debugging, but the primary
# entry point should be configured in pyproject.toml
if __name__ == "__main__":
    app()
