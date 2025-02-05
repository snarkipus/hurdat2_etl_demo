"""Extract module for HURDAT2 ETL pipeline"""

import logging
from collections.abc import Iterator
from pathlib import Path

from ..config.settings import Settings
from ..core import ETLStage
from ..exceptions import ExtractionError
from ..models import Storm
from .parser import parse_hurdat2

logger = logging.getLogger(__name__)


class Extract(ETLStage[None, Iterator[Storm]]):
    """Extract stage for HURDAT2 data.

    Args:
        input_path: Path to HURDAT2 data file. If None, uses default from Settings.
        progress_enabled: Whether to enable progress tracking.
    """

    def __init__(
        self, input_path: Path | None = None, progress_enabled: bool = True
    ) -> None:
        super().__init__(progress_enabled=progress_enabled)
        self.input_path = input_path or Settings.HURDAT2_DATA_FILE

    def validate_file(self) -> bool:
        """Validate input file exists and is readable.

        Returns:
            True if file is valid.

        Raises:
            ExtractionError: If file validation fails.
        """
        if not self.input_path.exists():
            raise ExtractionError(f"Input file not found: {self.input_path}")
        if not self.input_path.is_file():
            raise ExtractionError(f"Input path is not a file: {self.input_path}")
        return True

    def process(self, _: None) -> Iterator[Storm]:
        """Process HURDAT2 file and yield Storm objects with progress tracking.

        Args:
            _: Unused input parameter.

        Returns:
            Iterator of Storm objects.

        Raises:
            ExtractionError: If extraction fails.
        """
        logger.info(f"Starting extraction from {self.input_path}")

        try:
            self.validate_file()

            # Count total lines for progress bar
            total_lines = sum(1 for _ in open(self.input_path, encoding="utf-8"))
            self.init_progress(total_lines, "Extracting storms")

            try:
                for storm in parse_hurdat2(self.input_path):
                    # Update progress for header
                    self.update_progress()
                    # Update progress for each observation
                    for _obs in storm.observations:
                        self.update_progress()
                    yield storm

            finally:
                self.close_progress()

        except Exception as e:
            logger.error(f"Extraction failed: {e!s}")
            raise ExtractionError(f"Failed to extract data: {e!s}") from e
