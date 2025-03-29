import csv
from collections.abc import Iterator
from typing import Any

from rich.progress import TaskID

from etl_pipeline.core import ETLStage
from etl_pipeline.exceptions import ExtractionError


class ExtractStage(ETLStage):
    """Extract stage for the ETL pipeline.

    Reads data from a CSV file and yields raw rows as lists of strings.

    Input data must be a dictionary containing {'file_path': 'path/to/file.csv'}.
    """

    def __init__(self, name: str = "extract", **kwargs: Any) -> None:
        """Initialize the extract stage."""
        super().__init__(name=name, **kwargs)
        self._task_id: TaskID | None = None

    def execute(self, data: Any) -> Iterator[list[str]]:
        """Execute the extraction process from a CSV file.

        Handles setup (logging, file validation, progress tracking) and teardown,
        delegating core extraction logic to _process.

        Args:
            data: Dictionary containing 'file_path' key with path to CSV file.

        Returns:
            Iterator yielding CSV rows as lists of strings.

        Raises:
            TypeError: If data is not a dict or missing 'file_path' key.
            ValueError: If 'file_path' is not a string.
            ExtractionError: If file access fails or during CSV processing.
        """
        if not isinstance(data, dict) or "file_path" not in data:
            raise TypeError("Input data must be a dict with a 'file_path' key.")
        file_path = data["file_path"]
        if not isinstance(file_path, str):
            raise ValueError("'file_path' must be a string.")

        self.logger.info(f"Starting execution of stage: {self.name}")
        self.logger.info(f"Attempting extraction from: {file_path}")

        # Validate file accessibility before starting extraction
        try:
            with open(file_path):
                pass  # File is accessible
        except (FileNotFoundError, PermissionError, OSError) as e:
            self.logger.error(f"Error accessing file '{file_path}': {e}")
            raise ExtractionError(f"Could not access file: {file_path}") from e

        # Create indeterminate progress bar
        self._task_id = self.console_handler.create_task(
            description="Extracting data", total=None
        )

        # Process file with progress tracking
        with self.console_handler.progress:
            try:
                yield from self._process(data)
                self.logger.info("Extraction complete")
                self.logger.info(f"Completed execution of stage: {self.name}")
            except ExtractionError:
                self.logger.error(f"Extraction failed for stage: {self.name}")
                raise
            except Exception as e:
                self.logger.error(
                    f"Unexpected error during extraction in stage {self.name}: {e}"
                )
                raise ExtractionError(
                    f"Extraction failed unexpectedly in stage {self.name}: {e}"
                ) from e
            finally:
                if self._task_id is not None:
                    self.console_handler.progress.stop_task(self._task_id)

    def _process(self, data: Any) -> Iterator[list[str]]:
        """Read and parse data from a CSV file.

        Args:
            data: Dictionary containing validated 'file_path'.

        Yields:
            Raw CSV rows as lists of strings with whitespace trimmed.

        Raises:
            ExtractionError: If file cannot be read or CSV parsing fails.
        """
        file_path = data["file_path"]
        reader = None  # Initialize for exception handling
        try:
            with open(file_path, newline="") as file:
                reader = csv.reader(file)

                for row in reader:
                    cleaned_row = [field.strip() for field in row]
                    yield cleaned_row

                    if self._task_id is not None:
                        self.console_handler.update_progress(self._task_id, advance=1)

        except csv.Error as e:
            line_num = reader.line_num if reader is not None else "N/A"
            self.logger.error(f"CSV parsing error near line {line_num}: {e}")
            raise ExtractionError(
                f"Error parsing CSV file near line {line_num}: {e}"
            ) from e
        except OSError as e:
            self.logger.error(f"IO error reading file '{file_path}': {e}")
            raise ExtractionError(f"IO error reading file: {file_path}") from e
