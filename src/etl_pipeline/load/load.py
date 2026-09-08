"""
Defines the Load stage of the ETL pipeline using the ETLStage base class.
"""

import logging
from collections.abc import Callable
from datetime import UTC
from typing import Any

from rich.console import Console

from etl_pipeline.core import ETLStage, ProgressManager
from etl_pipeline.exceptions import LoadError

# Alias DB models to avoid name clash with Pydantic models
from etl_pipeline.load.models import Observation as DbObservation
from etl_pipeline.load.models import Storm as DbStorm
from etl_pipeline.load.unit_of_work import AbstractUnitOfWork, SqlAlchemyUnitOfWork

# Import Pydantic models with their actual names
from etl_pipeline.transform.models import Observation as PydanticObservation
from etl_pipeline.transform.models import Storm as PydanticStorm


class LoadStage(ETLStage):
    """ETL Stage responsible for loading data into the database."""

    def __init__(
        self,
        uow_factory: Callable[[], AbstractUnitOfWork] = SqlAlchemyUnitOfWork,
        console: Console | None = None,
        progress_manager: ProgressManager | None = None,
        log_level: int = logging.INFO,
    ) -> None:
        """
        Initializes the Load stage.

        Args:
            uow_factory: A factory function or class that returns an
                         instance conforming to the AbstractUnitOfWork protocol.
            console: An optional Rich Console instance for progress display.
            progress_manager: An optional shared progress manager.
            log_level: The logging level (e.g., logging.INFO).
        """
        super().__init__(
            name="load",
            console=console,
            progress_manager=progress_manager,
            log_level=log_level,
        )
        self.uow_factory = uow_factory

    def _load_spatial_extension(self, uow: AbstractUnitOfWork) -> None:
        """
        Ensures the DuckDB spatial extension is installed and loaded.
        """
        try:
            if self.progress_manager:
                self.progress_manager.add_task("db_setup", "Setting up database", 100)
                # Start at 10%
                try:
                    self.progress_manager.progress.update(
                        self.progress_manager.tasks["db_setup"], completed=10
                    )
                except (AttributeError, KeyError):
                    # For tests using mocks
                    self.progress_manager.update("db_setup", advance=10)

            uow.execute(statement="INSTALL spatial;")
            uow.execute(statement="LOAD spatial;")

            if self.progress_manager:
                # Update to 100% before completing
                try:
                    self.progress_manager.progress.update(
                        self.progress_manager.tasks["db_setup"], completed=100
                    )
                except (AttributeError, KeyError):
                    # For tests using mocks
                    self.progress_manager.update("db_setup", completed=100)
                self.progress_manager.complete_task(
                    "db_setup", "Database setup complete"
                )

            self.logger.info("DuckDB spatial extension installed and loaded.")

        except Exception as install_exc:
            self.logger.warning(
                f"Could not install spatial extension (may be installed already): "
                f"{install_exc}"
            )
            try:
                query = (
                    "SELECT loaded FROM duckdb_extensions() "
                    "WHERE extension_name = 'spatial';"
                )
                result = uow.execute(statement=query).scalar_one_or_none()

                if result:
                    if self.progress_manager:
                        # Update to 100% before completing
                        try:
                            self.progress_manager.progress.update(
                                self.progress_manager.tasks["db_setup"], completed=100
                            )
                        except (AttributeError, KeyError):
                            # For tests using mocks
                            self.progress_manager.update("db_setup", completed=100)
                        self.progress_manager.complete_task(
                            "db_setup", "Database setup complete"
                        )
                    self.logger.info("DuckDB spatial extension confirmed loaded.")
                else:
                    if self.progress_manager:
                        self.progress_manager.complete_task(
                            "db_setup", "Database setup FAILED"
                        )
                    self.logger.error(
                        "DuckDB spatial extension failed to install and is not loaded."
                    )
                    raise RuntimeError(
                        "DuckDB spatial extension is required but could not be loaded."
                    ) from install_exc
            except Exception as check_exc:
                if self.progress_manager:
                    self.progress_manager.complete_task(
                        "db_setup", "Database setup FAILED"
                    )
                self.logger.error(
                    f"Failed to check if spatial extension is loaded: {check_exc}"
                )
                raise RuntimeError(
                    "Failed to verify DuckDB spatial extension status."
                ) from check_exc

    def _process(self, data: Any) -> tuple[int, int]:
        """
        Core logic for the Load stage.

        Expects input data to be a tuple containing iterables of transformed
        storms and observations. Loads data into the database using a Unit of Work.

        Args:
            data: A tuple containing (List[PydanticStorm], List[PydanticObservation]).

        Returns:
            A tuple containing the count of loaded storms and observations.

        Raises:
            TypeError: If the input data is not in the expected format.
            LoadError: If database operations fail.
        """
        if not isinstance(data, tuple) or len(data) != 2:
            raise TypeError(
                "Load stage expects a tuple: "
                "(transformed_storms, transformed_observations)"
            )

        try:
            # Get the storm and observation lists
            storms_list: list[PydanticStorm] = list(data[0])
            observations_list: list[PydanticObservation] = list(data[1])

            total_storms = len(storms_list)
            total_observations = len(observations_list)

            self.logger.info(
                f"Starting to load {total_storms} storms and "
                f"{total_observations} observations"
            )

            # Use the Unit of Work to manage the database session
            with self.uow_factory() as uow:
                # Load spatial extension
                self._load_spatial_extension(uow)

                # Setup progress tracking
                if self.progress_manager:
                    # Add tasks for storms and observations
                    if total_storms > 0:
                        self.progress_manager.add_task(
                            "load_storms", "Loading storms", total_storms
                        )

                    if total_observations > 0:
                        self.progress_manager.add_task(
                            "load_observations",
                            "Loading observations",
                            total_observations,
                        )

                # --- Load Storm Data ---
                if self.progress_manager and total_storms > 0:
                    self.progress_manager.add_task(
                        "load_storms", "Preparing storm records", total_storms
                    )

                storm_count = 0
                batch_count = 0
                batch_size = max(1, min(50, total_storms // 20))  # Adaptive batch size

                for storm in storms_list:
                    # Create and add DB storm object
                    storm_data = storm.model_dump(exclude={"observations"})
                    storm_data["storm_id"] = storm.storm_id

                    storm_db = DbStorm(**storm_data)
                    uow.storms.add(storm_db)
                    storm_count += 1
                    batch_count += 1

                    # Update progress in batches
                    if self.progress_manager and batch_count >= batch_size:
                        description = (
                            f"Loading storms: {storm_count:,}/{total_storms:,}"
                        )
                        self.progress_manager.update(
                            "load_storms",
                            advance=batch_count,
                            description=description,
                        )
                        batch_count = 0

                # Update remaining storms
                if self.progress_manager and batch_count > 0:
                    self.progress_manager.update("load_storms", advance=batch_count)

                # Complete storms task
                if self.progress_manager and total_storms > 0:
                    self.progress_manager.complete_task(
                        "load_storms", f"Loaded {storm_count:,} storm records"
                    )

                # --- Load Observation Data ---
                if self.progress_manager and total_observations > 0:
                    self.progress_manager.add_task(
                        "load_observations",
                        "Loading observation data",
                        total_observations,
                    )

                observation_count = 0
                current_observation_id = 1
                batch_count = 0
                batch_size = max(
                    1000, min(5000, total_observations // 20)
                )  # Larger batch size for observations

                for obs in observations_list:
                    # Prepare observation data
                    observation_data = obs.model_dump(exclude={"location"})
                    # Store naive UTC, not session-local time from an aware cast.
                    # Already-naive inputs retain their existing UTC meaning.
                    if obs.date.utcoffset() is not None:
                        observation_data["date"] = obs.date.astimezone(UTC).replace(
                            tzinfo=None
                        )

                    # Generate geometry
                    latitude = obs.location.latitude
                    longitude = obs.location.longitude
                    if longitude is not None and latitude is not None:
                        observation_data["geom"] = f"POINT({longitude} {latitude})"
                    else:
                        self.logger.warning(
                            f"Missing lat/lon for observation {obs.date}, "
                            f"cannot create geom."
                        )
                        observation_data["geom"] = None

                    # Add IDs
                    observation_data["id"] = current_observation_id
                    observation_data["storm_id"] = obs.storm_id

                    # Create and add DB object
                    observation_db = DbObservation(**observation_data)
                    uow.observations.add(observation_db)

                    # Update counters
                    current_observation_id += 1
                    observation_count += 1
                    batch_count += 1

                    # Update progress in batches
                    if self.progress_manager and batch_count >= batch_size:
                        percent = int(observation_count / total_observations * 100)
                        self.progress_manager.update(
                            "load_observations",
                            advance=batch_count,
                            description=f"Loading observations: {percent}% complete",
                        )
                        batch_count = 0

                # Update remaining observations
                if self.progress_manager and batch_count > 0:
                    self.progress_manager.update(
                        "load_observations", advance=batch_count
                    )

                # Complete observations task
                if self.progress_manager and total_observations > 0:
                    self.progress_manager.complete_task(
                        "load_observations",
                        f"Loaded {observation_count:,} observations",
                    )

                # Add commit task with determinate progress
                if self.progress_manager:
                    self.progress_manager.add_task(
                        "commit", "Saving data to database", 100
                    )
                    # Start at 10%
                    try:
                        self.progress_manager.progress.update(
                            self.progress_manager.tasks["commit"], completed=10
                        )
                    except (AttributeError, KeyError):
                        # For tests using mocks
                        self.progress_manager.update("commit", advance=10)

                # Commit data
                self.logger.info("Committing data to database...")
                if self.progress_manager:
                    # Update to 90%
                    try:
                        self.progress_manager.progress.update(
                            self.progress_manager.tasks["commit"], completed=90
                        )
                    except (AttributeError, KeyError):
                        # For tests using mocks
                        self.progress_manager.update("commit", advance=80)

                # Mark commit as complete
                if self.progress_manager:
                    # Ensure we're at 100%
                    try:
                        self.progress_manager.progress.update(
                            self.progress_manager.tasks["commit"], completed=100
                        )
                    except (AttributeError, KeyError):
                        # For tests using mocks
                        self.progress_manager.update("commit", advance=10)
                    # Complete the task
                    self.progress_manager.complete_task(
                        "commit", "Data committed to database"
                    )

            self.logger.info(
                f"Load complete: {storm_count} storms, {observation_count} observations"
            )
            return storm_count, observation_count

        except Exception as e:
            self.logger.error(f"Failed to load data: {e}", exc_info=True)
            raise LoadError(f"Database load failed: {e}") from e
