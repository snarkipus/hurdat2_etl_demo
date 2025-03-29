"""
Defines the Transform stage of the ETL pipeline using the ETLStage base class.

This stage processes raw HURDAT2 data rows (lists of strings), groups them
logically into individual storms, parses the data using functions from the
'parser' module, and creates validated Pydantic Storm objects containing
validated Observation objects. It leverages the base ETLStage for logging
and progress reporting.
"""

from collections.abc import Iterator
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from etl_pipeline.core import ETLStage
from etl_pipeline.exceptions import TransformError, ValidationError

# Import the Pydantic models used for transformation output
from etl_pipeline.transform.models import Observation as PydanticObservation
from etl_pipeline.transform.models import Storm as PydanticStorm

# Import parser functions specific to this stage
from .parser import is_header_line, parse_header_line, parse_track_line


class TransformStage(ETLStage):
    """
    ETL Stage for transforming raw HURDAT2 data into structured Pydantic Storm objects.

    Groups raw data rows, delegates detailed parsing to functions in the 'parser'
    module, validates data using Pydantic models, and uses base class features
    for logging and progress display.

    Expected input data format:
        A list of lists, where each inner list is a raw row (list of strings)
        from the source HURDAT2 file, as produced by the Extract stage.
    """

    def __init__(self, name: str = "transform", **kwargs: Any) -> None:
        """
        Initializes the Transform stage.

        Args:
            name: The name of the stage (defaults to "transform").
            **kwargs: Additional keyword arguments passed to the ETLStage base class
                      (e.g., console, log_level).
        """
        super().__init__(name=name, **kwargs)

    def _process(self, data: Any) -> list[PydanticStorm]:
        """
        Core processing logic for the transformation stage.

        Orchestrates the grouping of raw data rows into storms and the creation
        of validated Pydantic Storm objects.

        Args:
            data: A list of lists, where each inner list represents a row
                  of raw string data from the HURDAT2 file.

        Returns:
            A list of validated Pydantic Storm objects.

        Raises:
            TypeError: If the input data is not a list of lists.
            TransformError: If a fatal error occurs during the transformation process.
        """
        if not isinstance(data, list) or not all(isinstance(row, list) for row in data):
            raise TypeError(
                "Input data for TransformStage must be a list of lists (rows)."
            )

        self.logger.info("Starting transformation of HURDAT2 data.")
        try:
            # Process data row by row, yielding Storm objects
            storms = list(self._process_hurdat2_data(data))
            self.logger.info(
                f"Transformation complete: {len(storms)} storms processed successfully."
            )
            return storms
        except Exception as e:
            # Catch unexpected errors during the overall process
            self.logger.error(f"Transformation failed: {e}", exc_info=True)
            raise TransformError(f"Failed to transform data: {e}") from e

    def _process_hurdat2_data(
        self, raw_data: list[list[str]]
    ) -> Iterator[PydanticStorm]:
        """
        Groups raw data, creates, validates, and yields Pydantic Storm objects.

        Iterates through grouped storm data (header + observations), attempts
        to create a validated Pydantic Storm object for each, logs errors for
        invalid storms/observations, and yields successfully created storms.
        Manages progress reporting for the transformation process.

        Args:
            raw_data: The list of raw data rows (list of lists of strings).

        Yields:
            Successfully validated Pydantic Storm objects.
        """
        storms_data = self._group_into_storms(raw_data)
        if not storms_data:
            self.logger.warning("No storm data groups found in the input.")
            return  # Return empty iterator if no groups

        task_id = self.console_handler.create_task(
            description="Transforming storms", total=len(storms_data)
        )
        storm_count = 0
        processed_storm_ids = set()  # Track processed storms to avoid duplicates

        with self.console_handler.progress:
            for i, (header_row, observation_rows) in enumerate(storms_data):
                # Construct an identifier for logging purposes
                storm_header_id = header_row[0].strip() if header_row else "N/A"
                storm_identifier = f"Storm {i + 1} (Header ID: {storm_header_id})"
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
        observation rows for each header.

        Args:
            raw_data: The list of raw data rows (list of lists of strings).

        Returns:
            A list of tuples, where each tuple contains the raw header row
            and a list of its corresponding raw observation rows. Returns an
            empty list if no valid headers are found.
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
                            "observation rows following it."
                        )
                # Start tracking the new storm
                current_header = row
                observations = []  # Reset observations list for the new storm
            elif current_header is not None:
                # If currently tracking a storm, add this row as an observation
                observations.append(row)
            else:
                # Row is not a header and no storm is active
                # (e.g., data before first header)
                self.logger.warning(
                    f"Skipping row {i + 1} occurring before the first valid header: "
                    f"{row}"
                )

        # Add the last tracked storm after the loop finishes
        if current_header is not None:
            if observations:
                storms.append((current_header, observations))
            else:
                # Log if the last header had no observations
                self.logger.warning(
                    f"Last header {current_header[0].strip()} had no "
                    "observation rows following it."
                )

        self.logger.info(f"Grouped data into {len(storms)} potential storms.")
        return storms

    def _create_storm(
        self, header_row: list[str], observation_rows: list[list[str]]
    ) -> PydanticStorm:
        """
        Creates a single validated Pydantic Storm object from its raw data.

        Uses parser functions to process the header and each observation row,
        then attempts to instantiate and validate the Storm object using Pydantic.
        Filters out invalid observations within a storm.

        Args:
            header_row: The raw header row for the storm.
            observation_rows: A list of raw observation rows for the storm.

        Returns:
            A validated Pydantic Storm object.

        Raises:
            TransformError: If parsing fails for the header or if no valid
                            observations are found for the storm.
            ValidationError: If Pydantic validation fails for the final Storm object.
        """
        storm_id_for_log = header_row[0].strip() if header_row else "UNKNOWN"
        try:
            # Parse the header using the dedicated parser function
            parsed_header = parse_header_line(header_row)
            if not parsed_header:
                raise TransformError(f"Invalid header format: {header_row}")

            basin, cyclone_number, year, name = parsed_header
            # Generate the storm_id here
            storm_id = f"{basin}{cyclone_number:02d}{year}"

            # Process observation rows, collecting valid ones
            valid_observations = []
            for i, obs_row in enumerate(observation_rows):
                try:
                    # Attempt to create a single observation, passing storm_id
                    observation = self._create_observation(obs_row, storm_id)
                    valid_observations.append(observation)
                except (TransformError, ValidationError) as e:
                    # Log observation error and continue processing others
                    # for this storm
                    self.logger.error(
                        f"Skipping observation {i + 1} for storm "
                        f"{storm_id_for_log} due to error: {e}"
                    )
                except Exception as e:
                    # Log unexpected errors during observation creation
                    self.logger.error(
                        f"Unexpected error processing observation {i + 1} for "
                        f"storm {storm_id_for_log}: {e}",
                        exc_info=True,
                    )

            # A storm must have at least one valid observation to be considered valid
            if not valid_observations:
                raise TransformError(
                    f"No valid observations found for storm {storm_id_for_log}"
                )

            # Attempt to create the final Storm object with validated data
            storm = PydanticStorm(
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
            # Wrap Pydantic error in our domain-specific ValidationError
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

    def _create_observation(self, row: list[str], storm_id: str) -> PydanticObservation:
        """
        Creates a single validated Pydantic Observation object from a raw data row.

        Uses the `parse_track_line` function to get parsed data, adds the
        provided storm_id, then instantiates and validates the Observation
        object using Pydantic.

        Args:
            row: The raw data row (list of strings) for the observation.
            storm_id: The unique identifier of the parent storm.

        Returns:
            A validated Pydantic Observation object.

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

            # Add the storm_id to the parsed data before validation
            parsed_data["storm_id"] = storm_id

            # Create and validate the Observation object using the parsed dictionary
            # Pydantic handles detailed validation (Point creation, types, constraints)
            observation = PydanticObservation(**parsed_data)
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
