"""
Defines the Load stage of the ETL pipeline using the ETLStage base class.

This stage is responsible for loading transformed data into the DuckDB database
using the Repository and Unit of Work patterns.
"""

import logging
from collections.abc import Callable, Iterable  # Add Callable
from typing import Any

from rich.console import Console

# Alias DB models to avoid name clash with Pydantic models
from etl_pipeline.core import ETLStage  # Import the base class
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
        log_level: int = logging.INFO,
    ) -> None:
        """
        Initializes the Load stage.

        Args:
            uow_factory: A factory function or class that returns an
                         instance conforming to the AbstractUnitOfWork protocol.
                         Defaults to SqlAlchemyUnitOfWork.
            console: An optional Rich Console instance for progress display.
            log_level: The logging level (e.g., logging.INFO).
        """
        super().__init__(name="load", console=console, log_level=log_level)
        self.uow_factory = uow_factory

    def _load_spatial_extension(self, uow: AbstractUnitOfWork) -> None:
        """
        Ensures the DuckDB spatial extension is installed and loaded.

        Args:
            uow: The Unit of Work instance managing the database session.

        Raises:
            RuntimeError: If loading or checking the spatial extension fails.
        """
        try:
            uow.execute(statement="INSTALL spatial;")
            uow.execute(statement="LOAD spatial;")
            self.logger.info("DuckDB spatial extension installed and loaded via UoW.")
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
                    self.logger.info("DuckDB spatial extension confirmed loaded.")
                else:
                    self.logger.error(
                        "DuckDB spatial extension failed to install and is not loaded."
                    )
                    raise RuntimeError(
                        "DuckDB spatial extension is required but could not be loaded."
                    ) from install_exc
            except Exception as check_exc:
                self.logger.error(
                    f"Failed to check if spatial extension is loaded: {check_exc}"
                )
                raise RuntimeError(
                    "Failed to verify DuckDB spatial extension status."
                ) from check_exc

    def _process(self, data: Any) -> Any:
        """
        Core logic for the Load stage.

        Expects input data to be a tuple containing iterables of transformed
        storms and observations. Loads data into the database using a Unit of Work.

        Args:
            data: A tuple containing (Iterable[PydanticStorm],
                  Iterable[PydanticObservation]).

        Returns:
            A tuple containing the count of loaded storms and observations.

        Raises:
            TypeError: If the input data is not in the expected format.
        """
        if not isinstance(data, tuple) or len(data) != 2:
            raise TypeError(
                "Load stage expects a tuple: "
                "(transformed_storms, transformed_observations)"
            )

        transformed_storms: Iterable[PydanticStorm] = data[0]
        transformed_observations: Iterable[PydanticObservation] = data[1]

        # Convert iterables to lists to get counts for progress bars
        # (Note: In the current CLI flow, they are already lists)
        storms_list = list(transformed_storms)
        observations_list = list(transformed_observations)
        total_storms = len(storms_list)
        total_observations = len(observations_list)

        storm_count = 0
        observation_count = 0

        # Use the Unit of Work context manager
        with self.uow_factory() as uow:
            self.logger.info("Starting data loading process via Unit of Work...")

            # Ensure spatial extension is loaded within the transaction
            self._load_spatial_extension(uow)

            # Use the progress bar context manager
            with self.console_handler.progress:
                # Create progress tasks
                storm_task_id = self.console_handler.create_task(
                    description="Loading storms", total=total_storms
                )
                obs_task_id = self.console_handler.create_task(
                    description="Loading observations", total=total_observations
                )

                # --- Storm Data Loading ---
                self.logger.info("Loading storm data...")
                for ts in storms_list:  # Iterate over the list
                    # Exclude 'observations' relationship data from the dump
                    storm_data = ts.model_dump(exclude={"observations"})
                    # Explicitly add the storm_id from the Pydantic property
                    storm_data["storm_id"] = ts.storm_id
                    storm_db = DbStorm(**storm_data)
                    uow.storms.add(storm_db)
                    storm_count += 1
                    # Update storm progress
                    self.console_handler.update_progress(storm_task_id, advance=1)
                self.logger.info(f"Added {storm_count} storms to the session.")

                # --- Observation Data Loading ---
                current_observation_id = 1  # Initialize counter for observation IDs
                self.logger.info("Loading observation data...")
                for to in observations_list:  # Iterate over the list
                    observation_data = to.model_dump(exclude={"location"})
                    latitude = to.location.latitude
                    longitude = to.location.longitude

                    if longitude is not None and latitude is not None:
                        observation_data["geom"] = f"POINT({longitude} {latitude})"
                    else:
                        self.logger.warning(
                            f"Missing lat/lon for observation {to.date}, "
                            f"cannot create geom."
                        )
                        observation_data["geom"] = None

                    # Add the generated ID
                    observation_data["id"] = current_observation_id

                    # Get storm_id directly from the Pydantic model field
                    # (Error handling for missing attribute removed as it's now
                    # required by Pydantic model)
                    observation_data["storm_id"] = to.storm_id

                    current_observation_id += 1  # Increment ID counter
                    # Create DB object - storm_id is now in observation_data
                    observation_db = DbObservation(**observation_data)
                    uow.observations.add(observation_db)
                    observation_count += 1
                    # Update observation progress
                    self.console_handler.update_progress(obs_task_id, advance=1)
                self.logger.info(
                    f"Added {observation_count} observations to the session."
                )
                # Add an indeterminate task for the commit phase
                self.console_handler.create_task(
                    description="Committing data...", total=None
                )

            # Commit/rollback happens implicitly when 'with uow:' block exits
            # The 'with self.console_handler.progress:' block also exits,
            # stopping the display
        self.logger.info("Commit/rollback initiated by UoW context manager exit.")

        # Return counts after the UoW block has fully completed
        # (committed or rolled back)
        return storm_count, observation_count
