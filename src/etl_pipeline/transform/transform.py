"""
Transformation stage for the ETL pipeline.

This module defines the TransformStage class which processes raw HURDAT2 data,
groups it into storms, and uses parsing functions from the 'parser' module
to create validated Storm and Observation objects. It leverages the base
ETLStage for logging and progress reporting.
"""

from collections.abc import Iterator
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from etl_pipeline.core import ETLStage
from etl_pipeline.exceptions import TransformError, ValidationError
from etl_pipeline.transform.models import Observation, Storm

from .parser import is_header_line, parse_header_line, parse_track_line


class TransformStage(ETLStage):
    """
    ETL Stage for transforming raw HURDAT2 data into structured Storm objects.

    Delegates detailed parsing to functions in the 'parser' module and uses
    base class features for logging and progress display.
    """

    def __init__(self, name: str = "transform", **kwargs: Any) -> None:
        """
        Initialize the transform stage.

        Args:
            name: The name of the stage (default: "transform").
            **kwargs: Additional arguments passed to the base ETLStage.
        """
        super().__init__(name=name, **kwargs)

    def _process(self, data: list[list[str]]) -> list[Storm]:
        """
        Core processing logic for the transformation stage.

        Orchestrates the grouping of raw data into storms and the creation
        of validated Storm objects.

        Args:
            data: A list of lists, where each inner list represents a row
                  of raw string data from the HURDAT2 file.

        Returns:
            A list of validated Storm objects.

        Raises:
            TransformError: If a fatal error occurs during the transformation process.
        """
        self.logger.info("Starting transformation of HURDAT2 data.")
        try:
            # Process data row by row, yielding Storm objects
            storms = list(self._process_hurdat2_data(data))
            self.logger.info(
                f"Transformation complete: {len(storms)} storms processed."
            )
            return storms
        except Exception as e:
            # Catch unexpected errors during the overall process
            self.logger.error(f"Transformation failed: {e}", exc_info=True)
            raise TransformError(f"Failed to transform data: {e}") from e

    def _process_hurdat2_data(self, raw_data: list[list[str]]) -> Iterator[Storm]:
        """
        Groups raw data and yields validated Storm objects.

        Iterates through grouped storm data (header + observations), attempts
        to create a validated Storm object for each, logs errors, and yields
        successfully created storms. Handles progress reporting.

        Args:
            raw_data: The list of raw data rows.

        Yields:
            Successfully validated Storm objects.
        """
        storms_data = self._group_into_storms(raw_data)
        task_id = self.console_handler.create_task(
            description="Transforming storms", total=len(storms_data)
        )
        storm_count = 0
        processed_storm_ids = set()  # Track processed storms to avoid duplicates

        with self.console_handler.progress:
            for i, (header_row, observation_rows) in enumerate(storms_data):
                # Construct an identifier for logging purposes
                storm_header_id = header_row[0].strip() if header_row else "N/A"
                storm_identifier = f"Storm {i + 1} (Header: {storm_header_id})"
                try:
                    # Attempt to create the storm object
                    storm = self._create_storm(header_row, observation_rows)

                    # Check for duplicate storm IDs (can occur in source data)
                    if storm.storm_id in processed_storm_ids:
                        self.logger.warning(
                            f"Duplicate storm ID detected and skipped: {storm.storm_id}"
                        )
                        continue  # Skip processing this duplicate storm

                    # Yield the valid, unique storm
                    yield storm
                    processed_storm_ids.add(storm.storm_id)
                    storm_count += 1
                except (TransformError, ValidationError) as e:
                    # Log specific errors during storm creation and continue
                    self.logger.error(f"Skipping {storm_identifier} due to error: {e}")
                except Exception as e:
                    # Log unexpected errors during storm creation and continue
                    self.logger.error(
                        f"Unexpected error creating {storm_identifier}: {e}",
                        exc_info=True,
                    )

                # Update progress bar regardless of success/failure for this storm
                self.console_handler.update_progress(task_id, advance=1)

        self.logger.info(f"Successfully processed {storm_count} unique storms.")

    def _group_into_storms(
        self, raw_data: list[list[str]]
    ) -> list[tuple[list[str], list[list[str]]]]:
        """
        Groups raw data rows into logical storms based on header rows.

        Iterates through the raw data, identifying header rows using the
        `is_header_line` parser function, and collecting associated
        observation rows.

        Args:
            raw_data: The list of raw data rows.

        Returns:
            A list of tuples, where each tuple contains the raw header row
            and a list of its corresponding raw observation rows.
        """
        storms = []
        current_header: list[str] | None = None
        observations: list[list[str]] = []

        for i, row in enumerate(raw_data):
            # Skip empty or blank rows
            if not row or not any(field.strip() for field in row):
                self.logger.debug(f"Skipping empty or blank row {i + 1}")
                continue

            # Check if the current row is a header using the parser function
            if is_header_line(row):
                # If a storm was being tracked, store it before starting the new one
                if current_header is not None:
                    if observations:
                        storms.append((current_header, observations))
                    else:
                        # Log if a header was found but had no associated observations
                        self.logger.warning(
                            f"Header {current_header[0].strip()} found with no "
                            "observation rows."
                        )
                # Start tracking the new storm
                current_header = row
                observations = []  # Reset observations list for the new storm
            elif current_header is not None:
                # If currently tracking a storm, add this row as an observation
                observations.append(row)
            else:
                # Row is not a header and no storm is active
                # (e.g., data before the first header)
                self.logger.warning(
                    f"Skipping row {i + 1} occurring before the first valid header: "
                    f"{row}"
                )

        # Add the last tracked storm after the loop finishes
        if current_header is not None and observations:
            storms.append((current_header, observations))
        elif current_header is not None:
            # Log if the last header had no observations
            self.logger.warning(
                f"Last header {current_header[0].strip()} had no observation rows."
            )

        self.logger.info(f"Grouped data into {len(storms)} potential storms.")
        return storms

    def _create_storm(
        self, header_row: list[str], observation_rows: list[list[str]]
    ) -> Storm:
        """
        Creates a single validated Storm object from its raw header and
        observation rows.

         Uses parser functions to process the header and each observation row,
        then attempts to instantiate and validate the Storm object using Pydantic.

        Args:
            header_row: The raw header row for the storm.
            observation_rows: A list of raw observation rows for the storm.

        Returns:
            A validated Storm object.

        Raises:
            TransformError: If parsing fails for the header or no valid
                            observations are found.
            ValidationError: If Pydantic validation fails for the Storm or its
                             Observations.
        """
        storm_id_for_log = header_row[0].strip() if header_row else "UNKNOWN"
        try:
            # Parse the header using the dedicated parser function
            parsed_header = parse_header_line(header_row)
            if not parsed_header:
                # Header format is invalid according to the parser
                raise TransformError(f"Invalid header format: {header_row}")

            basin, cyclone_number, year, name = parsed_header

            # Process observation rows, collecting valid ones
            valid_observations = []
            for i, obs_row in enumerate(observation_rows):
                try:
                    # Attempt to create a single observation
                    observation = self._create_observation(obs_row)
                    valid_observations.append(observation)
                except (TransformError, ValidationError) as e:
                    # Log observation error and continue processing others
                    self.logger.error(
                        f"Skipping observation {i + 1} for storm "
                        f"{storm_id_for_log}: {e}"
                    )
                except Exception as e:
                    # Log unexpected errors during observation creation
                    self.logger.error(
                        f"Unexpected error processing observation {i + 1} for "
                        f"storm {storm_id_for_log}: {e}",
                        exc_info=True,
                    )

            # A storm must have at least one valid observation
            if not valid_observations:
                raise TransformError(
                    f"No valid observations found for storm {storm_id_for_log}"
                )

            # Attempt to create the final Storm object with validated data
            storm = Storm(
                basin=basin,
                cyclone_number=cyclone_number,
                year=year,
                name=name,
                observations=valid_observations,
            )
            return storm

        except PydanticValidationError as e:
            # Catch validation errors during the final Storm object creation
            self.logger.error(
                f"Storm object validation failed for {storm_id_for_log}: {e.errors()}"
            )
            raise ValidationError(f"Storm validation failed: {e}") from e
        except TransformError as e:
            # Propagate TransformErrors raised explicitly (e.g., bad header, no obs)
            raise e
        except Exception as e:
            # Catch any other unexpected error during storm creation
            self.logger.error(
                f"Unexpected error creating storm {storm_id_for_log}: {e}",
                exc_info=True,
            )
            raise TransformError(f"Unexpected error creating storm: {e}") from e

    def _create_observation(self, row: list[str]) -> Observation:
        """
        Creates a single validated Observation object from its raw data row.

        Uses the `parse_track_line` function to get parsed data, then
        instantiates and validates the Observation object using Pydantic.

        Args:
            row: The raw data row for the observation.

        Returns:
            A validated Observation object.

        Raises:
            TransformError: If the parser fails to extract essential data.
            ValidationError: If Pydantic validation fails for the Observation.
        """
        try:
            # Parse the raw observation row using the parser function
            parsed_data = parse_track_line(row)
            if not parsed_data:
                # Parser indicated essential data was missing or invalid
                raise TransformError(
                    f"Parser failed to extract essential data from row: {row}"
                )

            # Create and validate the Observation object using the parsed dictionary
            # Pydantic handles detailed validation (Point creation, types, constraints)
            observation = Observation(**parsed_data)
            return observation

        except PydanticValidationError as e:
            # Catch validation errors during Observation instantiation
            raise ValidationError(
                f"Observation validation failed for row {row}: {e}"
            ) from e
        except TransformError as e:
            # Propagate TransformError raised by the parser
            raise e
        except Exception as e:
            # Catch any other unexpected error during observation creation
            self.logger.error(
                f"Unexpected error creating observation from row {row}: {e}",
                exc_info=True,
            )
            raise TransformError(f"Unexpected error creating observation: {e}") from e
