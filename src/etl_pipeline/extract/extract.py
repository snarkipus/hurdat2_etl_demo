"""Defines the Extract stage of the ETL pipeline using the ETLStage base class."""

import csv
from collections.abc import Iterator
from typing import Any

from rich.progress import TaskID

from etl_pipeline.core import ETLStage
from etl_pipeline.exceptions import ExtractionError


class ExtractStage(ETLStage):
    """
    ETL Stage responsible for extracting data from a source CSV file.

    Reads data row by row and yields each row as a list of strings,
    after trimming whitespace from each field. It utilizes the base class
    for logging and console/progress bar management.

    Expected input data format:
        A dictionary: {'file_path': 'path/to/your/data.csv'}
    """

    def __init__(self, name: str = "extract", **kwargs: Any) -> None:
        """
        Initializes the Extract stage.

        Args:
            name: The name of the stage (defaults to "extract").
            **kwargs: Additional keyword arguments passed to the ETLStage base class
                      (e.g., console, log_level).
        """
        super().__init__(name=name, **kwargs)
        self._task_id: TaskID | None = None  # For managing the progress bar task

    def execute(self, data: Any) -> Iterator[list[str]]:
        """
        Executes the extraction process.

        Validates input, sets up progress tracking, calls the core processing logic,
        and handles teardown and error logging.

        Args:
            data: A dictionary containing the 'file_path' key with the path
                  to the source CSV file.

        Returns:
            An iterator yielding each CSV row as a list of strings.

        Raises:
            TypeError: If input data is not a dictionary or missing 'file_path'.
            ValueError: If 'file_path' value is not a string.
            ExtractionError: If the file cannot be accessed or if CSV parsing fails.
        """
        if not isinstance(data, dict) or "file_path" not in data:
            raise TypeError(
                "Input data for ExtractStage must be a dict with a 'file_path' key."
            )
        file_path = data["file_path"]
        if not isinstance(file_path, str):
            raise ValueError("'file_path' value must be a string.")

        self.logger.info(f"Starting execution of stage: {self.name}")
        self.logger.info(f"Attempting extraction from: {file_path}")

        # --- Pre-check file accessibility ---
        try:
            # Attempt to open briefly to check permissions/existence
            with open(file_path):
                pass
        except (FileNotFoundError, PermissionError, OSError) as e:
            self.logger.error(f"Error accessing file '{file_path}': {e}", exc_info=True)
            raise ExtractionError(f"Could not access source file: {file_path}") from e

        # --- Setup Progress Bar ---
        # Create an indeterminate task (total=None) as we don't know rows beforehand
        self._task_id = self.console_handler.create_task(
            description="Extracting data", total=None
        )

        # --- Process Data ---
        # Use the progress bar context manager
        with self.console_handler.progress:
            try:
                # Delegate to the core processing method
                yield from self._process(data)
                self.logger.info("Extraction process completed successfully.")
            except ExtractionError as e:
                # Log specific extraction errors
                self.logger.error(f"Extraction failed: {e}", exc_info=True)
                raise  # Re-raise the specific error
            except Exception as e:
                # Log unexpected errors during processing
                self.logger.error(
                    f"Unexpected error during extraction: {e}", exc_info=True
                )
                raise ExtractionError(f"Extraction failed unexpectedly: {e}") from e
            finally:
                # --- Teardown ---
                # Ensure progress task is stopped even if errors occur
                if self._task_id is not None:
                    # Update description to show completion and mark as completed
                    self.console_handler.progress.update(
                        self._task_id,
                        description="Extracting data... Done!",
                        completed=True,  # Mark as completed
                    )
                    # Optional: Stop the task explicitly if needed, though context
                    # manager handles exit
                    # self.console_handler.progress.stop_task(self._task_id)
                self.logger.info(f"Completed execution of stage: {self.name}")

    def _process(self, data: Any) -> Iterator[list[str]]:
        """
        Core logic to read and yield rows from the CSV file.

        Args:
            data: Dictionary containing the validated 'file_path'.

        Yields:
            Each row from the CSV file as a list of strings, with whitespace trimmed
            from each field.

        Raises:
            ExtractionError: If the file cannot be read or if a CSV parsing error
                occurs.
        """
        file_path = data["file_path"]
        reader = None  # Initialize reader for potential use in error reporting

        try:
            # Specify encoding for CSV file
            with open(file_path, newline="", encoding="utf-8") as csvfile:
                reader = csv.reader(csvfile)
                self.logger.debug(f"Opened CSV file: {file_path}")

                for row in reader:
                    # Trim whitespace from each field in the row
                    cleaned_row = [field.strip() for field in row]
                    yield cleaned_row

                    # Update progress (even for indeterminate tasks, shows activity)
                    if self._task_id is not None:
                        self.console_handler.update_progress(self._task_id, advance=1)

        except csv.Error as e:
            line_num = reader.line_num if reader else "N/A"
            self.logger.error(
                f"CSV parsing error in '{file_path}' near line {line_num}: {e}",
                exc_info=True,
            )
            raise ExtractionError(
                f"Error parsing CSV file '{file_path}' near line {line_num}: {e}"
            ) from e
        except OSError as e:
            self.logger.error(
                f"OS error reading file '{file_path}': {e}", exc_info=True
            )
            raise ExtractionError(f"OS error reading file: {file_path}") from e
        except Exception as e:
            # Catch other potential exceptions like UnicodeDecodeError
            self.logger.error(
                f"Unexpected error processing file '{file_path}': {e}", exc_info=True
            )
            raise ExtractionError(
                f"Unexpected error processing file: {file_path}"
            ) from e
