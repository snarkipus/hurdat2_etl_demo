"""Defines the Extract stage of the ETL pipeline using the ETLStage base class."""

import csv
from collections.abc import Iterator
from typing import Any

from rich.console import Console

from etl_pipeline.core import ETLStage, ProgressManager
from etl_pipeline.exceptions import ExtractionError


class ExtractStage(ETLStage):
    """
    ETL Stage responsible for extracting data from a source CSV file.

    Reads data row by row and yields each row as a list of strings,
    after trimming whitespace from each field.
    """

    def __init__(
        self,
        name: str = "extract",
        console: Console | None = None,
        progress_manager: ProgressManager | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initializes the Extract stage.

        Args:
            name: The name of the stage (defaults to "extract").
            console: An optional Rich Console instance to use.
            progress_manager: An optional shared progress manager.
            **kwargs: Additional keyword arguments passed to the ETLStage base class.
        """
        super().__init__(
            name=name, console=console, progress_manager=progress_manager, **kwargs
        )
        self.row_count = 0

    def _process(self, data: Any) -> Iterator[list[str]]:
        """
        Core logic to read and yield rows from the CSV file.

        Args:
            data: Dictionary containing the validated 'file_path'.

        Yields:
            Each row from the CSV file as a list of strings, with whitespace trimmed.

        Raises:
            TypeError: If input data is not a dictionary or missing 'file_path'.
            ValueError: If 'file_path' value is not a string.
            ExtractionError: If the file cannot be read or if CSV parsing error occurs.
        """
        if not isinstance(data, dict) or "file_path" not in data:
            raise TypeError(
                "Input data for ExtractStage must be a dict with a 'file_path' key."
            )
        file_path = data["file_path"]
        if not isinstance(file_path, str):
            raise ValueError("'file_path' value must be a string.")

        self.logger.info(f"Starting extraction from: {file_path}")

        # Pre-check file accessibility
        try:
            with open(file_path):
                pass
        except (FileNotFoundError, PermissionError, OSError) as e:
            self.logger.error(f"Error accessing file '{file_path}': {e}", exc_info=True)
            raise ExtractionError(f"Could not access source file: {file_path}") from e

        reader = None
        try:
            # Register a sub-task if we have a progress manager
            if self.progress_manager:
                self.progress_manager.add_task(
                    "extract_rows",
                    "Reading input file",
                    100,  # Use determinate progress (100%)
                )

            # Pre-count lines to estimate file size for progress bar
            line_count = 0
            with open(file_path, newline="", encoding="utf-8") as f:
                for _ in f:
                    line_count += 1

            # Calculate a proportion for adjusting progress updates
            if line_count > 0:
                self.proportion = 100.0 / line_count
            else:
                self.proportion = 1.0  # Default if no lines found

            # Reset progress bar to 0 and set description with count info
            if self.progress_manager:
                try:
                    self.progress_manager.progress.update(
                        self.progress_manager.tasks["extract_rows"],
                        completed=0,
                        description=f"Reading {line_count:,} lines from input file",
                    )
                except (AttributeError, KeyError):
                    # For tests using mocks without full implementation
                    self.progress_manager.update("extract_rows", completed=0)

            # Open and process the CSV file
            with open(file_path, newline="", encoding="utf-8") as csvfile:
                reader = csv.reader(csvfile)
                self.logger.debug(f"Opened CSV file: {file_path}")

                # Process in batches to avoid UI flicker
                batch_size = 100
                batch_count = 0

                for row in reader:
                    # Trim whitespace from each field in the row
                    cleaned_row = [field.strip() for field in row]

                    # Update row count for progress tracking
                    self.row_count += 1
                    batch_count += 1

                    # Update progress if we have a progress manager
                    if self.progress_manager and batch_count >= batch_size:
                        # Calculate true completion percentage based on row count
                        completed = min(int(self.row_count * self.proportion), 100)
                        try:
                            self.progress_manager.progress.update(
                                self.progress_manager.tasks["extract_rows"],
                                completed=completed,
                                description=f"Reading data: {self.row_count:,} rows",
                            )
                        except (AttributeError, KeyError):
                            # For tests using mocks without full implementation
                            self.progress_manager.update(
                                "extract_rows", advance=batch_count
                            )
                        batch_count = 0

                    yield cleaned_row

            # Ensure we show 100% at the end
            if self.progress_manager:
                try:
                    self.progress_manager.progress.update(
                        self.progress_manager.tasks["extract_rows"],
                        completed=100,
                        description=f"Read {self.row_count:,} rows",
                    )
                except (AttributeError, KeyError):
                    # For tests using mocks
                    self.progress_manager.update("extract_rows", completed=100)

                # Mark as complete
                self.progress_manager.complete_task(
                    "extract_rows", f"Read {self.row_count:,} rows"
                )

            self.logger.info(f"Extraction completed. {self.row_count} rows processed.")

        except csv.Error as e:
            line_num = getattr(reader, "line_num", "unknown")
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
            self.logger.error(
                f"Unexpected error processing file '{file_path}': {e}", exc_info=True
            )
            raise ExtractionError(
                f"Unexpected error processing file: {file_path}"
            ) from e
