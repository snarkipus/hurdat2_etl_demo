"""
Defines the Transform stage of the ETL pipeline using the ETLStage base class.
"""

from typing import Any

from pydantic import ValidationError as PydanticValidationError
from rich.console import Console

from etl_pipeline.core import ETLStage, ProgressManager
from etl_pipeline.exceptions import TransformError, ValidationError

# Import the Pydantic models used for transformation output
from etl_pipeline.transform.models import Observation as PydanticObservation
from etl_pipeline.transform.models import Storm as PydanticStorm

# Import parser functions specific to this stage
from .parser import is_header_line, parse_header_line, parse_track_line


class TransformStage(ETLStage):
    """
    ETL Stage for transforming raw HURDAT2 data into structured Pydantic Storm objects.
    """

    def __init__(
        self,
        name: str = "transform",
        console: Console | None = None,
        progress_manager: ProgressManager | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initializes the Transform stage.

        Args:
            name: The name of the stage (defaults to "transform").
            console: An optional Rich Console instance to use.
            progress_manager: An optional shared progress manager.
            **kwargs: Additional keyword arguments passed to the ETLStage base class.
        """
        super().__init__(
            name=name, console=console, progress_manager=progress_manager, **kwargs
        )

    def _process(self, data: Any) -> list[PydanticStorm]:
        """
        Core processing logic for the transformation stage.

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
            # First, group data into storms with a progress indicator
            total_rows = len(data)
            if self.progress_manager:
                self.progress_manager.add_task(
                    "transform_group", "Analyzing storm data structure", total_rows
                )

            storms_data = self._group_into_storms(data)

            # Show completion with storm count
            if self.progress_manager:
                self.progress_manager.complete_task(
                    "transform_group", f"Found {len(storms_data):,} storm records"
                )

            # Process storm groups into validated Storm objects
            storms_total = len(storms_data)
            if self.progress_manager and storms_total > 0:
                self.progress_manager.add_task(
                    "transform_storms", "Validating hurricane records", storms_total
                )

            storms = []
            batch_count = 0
            batch_size = max(
                1, min(10, storms_total // 100)
            )  # Adaptive batch for smoother updates

            for i, (header_row, observation_rows) in enumerate(storms_data):
                try:
                    storm = self._create_storm(header_row, observation_rows)
                    storms.append(storm)

                except (TransformError, ValidationError) as e:
                    self.logger.error(f"Error processing storm {i + 1}: {e}")
                except Exception as e:
                    self.logger.error(
                        f"Unexpected error creating storm {i + 1}: {e}", exc_info=True
                    )

                # Count attempted groups, including rejected storms, as actual work.
                batch_count += 1
                if self.progress_manager and (
                    batch_count >= batch_size or i + 1 == storms_total
                ):
                    self.progress_manager.update(
                        "transform_storms",
                        advance=0,
                        completed=i + 1,
                        description=f"Processing storms: {i + 1:,}/{storms_total:,}",
                    )
                    batch_count = 0

            if self.progress_manager and storms_total > 0:
                self.progress_manager.complete_task(
                    "transform_storms", f"Processed {len(storms):,} valid storms"
                )

            # Remove duplicates
            unique_storms_dict = {storm.storm_id: storm for storm in storms}
            unique_storms = list(unique_storms_dict.values())

            self.logger.info(
                f"Transformation complete: {len(unique_storms)} unique storms "
                "processed."
            )
            return unique_storms

        except Exception as e:
            self.logger.error(f"Transformation failed: {e}", exc_info=True)
            raise TransformError(f"Failed to transform data: {e}") from e

    def _group_into_storms(
        self, raw_data: list[list[str]]
    ) -> list[tuple[list[str], list[list[str]]]]:
        """
        Groups raw data rows into logical storms based on header rows.
        """
        storms = []
        current_header: list[str] | None = None
        observations: list[list[str]] = []
        total_rows = len(raw_data)
        batch_size = min(max(total_rows // 20, 100), 1000)

        for i, row in enumerate(raw_data):
            # Skip empty or blank rows
            if not row or not any(field.strip() for field in row):
                self.logger.debug(f"Skipping empty row {i + 1}")
            # Check if the current row is a header
            elif is_header_line(row):
                # If a storm was being tracked, store it before starting the new one
                if current_header is not None:
                    if observations:
                        storms.append((current_header, observations))
                    else:
                        self.logger.warning(
                            f"Header {current_header[0].strip()} "
                            "found with no observations."
                        )
                # Start tracking the new storm
                current_header = row
                observations = []  # Reset observations list
            elif current_header is not None:
                # If currently tracking a storm, add this row as an observation
                observations.append(row)
            else:
                # Row is not a header and no storm is active
                self.logger.warning(f"Skipping row {i + 1} before first valid header")

            if self.progress_manager and (
                (i + 1) % batch_size == 0 or i + 1 == total_rows
            ):
                self.progress_manager.update(
                    "transform_group",
                    advance=0,
                    completed=i + 1,
                    description=f"Analyzing data: {i + 1:,}/{total_rows:,} rows",
                )

        # Add the last tracked storm after the loop finishes
        if current_header is not None:
            if observations:
                storms.append((current_header, observations))
            else:
                self.logger.warning(
                    f"Last header {current_header[0].strip()} had no observations."
                )

        self.logger.info(f"Grouped data into {len(storms)} potential storms.")
        return storms

    def _create_storm(
        self, header_row: list[str], observation_rows: list[list[str]]
    ) -> PydanticStorm:
        """
        Creates a single validated Pydantic Storm object from its raw data.
        """
        storm_id_for_log = header_row[0].strip() if header_row else "UNKNOWN"

        try:
            # Parse the header using the dedicated parser function
            parsed_header = parse_header_line(header_row)
            if not parsed_header:
                raise TransformError(f"Invalid header format for {storm_id_for_log}")

            basin, cyclone_number, year, name = parsed_header
            # Generate the storm_id
            storm_id = f"{basin}{cyclone_number:02d}{year}"

            # Process observation rows
            valid_observations = []
            for i, obs_row in enumerate(observation_rows):
                try:
                    # Create a single observation
                    observation = self._create_observation(obs_row, storm_id)
                    valid_observations.append(observation)
                except (TransformError, ValidationError) as e:
                    self.logger.error(
                        f"Skipping observation {i + 1} "
                        f"for storm {storm_id_for_log}: {e}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Unexpected error processing observation {i + 1}: {e}",
                        exc_info=True,
                    )

            # A storm must have at least one valid observation
            if not valid_observations:
                raise TransformError(
                    f"No valid observations found for storm {storm_id_for_log}"
                )

            # Create the final Storm object with validated data
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
                f"Storm object validation failed for {storm_id_for_log}: "
                f"{e.errors(include_input=False, include_context=False)}"
            )
            # Wrap Pydantic error in our domain-specific ValidationError
            raise ValidationError(
                "Storm validation failed: "
                f"{e.errors(include_input=False, include_context=False)}"
            ) from e
        except TransformError as e:
            # Propagate TransformErrors raised explicitly
            raise e
        except Exception as e:
            # Catch any other unexpected error
            self.logger.error(
                f"Unexpected error creating storm {storm_id_for_log}: {e}",
                exc_info=True,
            )
            raise TransformError(f"Unexpected error creating storm: {e}") from e

    def _create_observation(self, row: list[str], storm_id: str) -> PydanticObservation:
        """
        Creates a single validated Pydantic Observation object from a raw data row.
        """
        try:
            # Parse the raw observation row
            parsed_data = parse_track_line(row)
            if not parsed_data:
                raise TransformError(
                    f"Parser failed to extract essential data for storm {storm_id}"
                )

            # Add the storm_id to the parsed data before validation
            parsed_data["storm_id"] = storm_id

            # Create and validate the Observation object using the parsed dictionary
            observation = PydanticObservation(**parsed_data)
            return observation

        except PydanticValidationError as e:
            # Catch validation errors during Observation instantiation
            raise ValidationError(
                f"Observation validation failed for {storm_id}: "
                f"{e.errors(include_input=False, include_context=False)}"
            ) from e
        except TransformError as e:
            # Propagate TransformError raised by the parser
            raise e
        except Exception as e:
            # Catch any other unexpected error during observation creation
            self.logger.error(
                f"Unexpected error creating observation for {storm_id}: {e}",
                exc_info=True,
            )
            raise TransformError(f"Unexpected error creating observation: {e}") from e
