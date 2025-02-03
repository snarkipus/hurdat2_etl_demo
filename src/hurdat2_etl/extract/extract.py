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


class Extract(ETLStage):
    """Extract stage for HURDAT2 data"""

    def __init__(self, input_path: Path | None = None):
        self.input_path = input_path or Settings.HURDAT2_DATA_FILE

    def validate_file(self) -> bool:
        """Validate input file exists and is readable"""
        if not self.input_path.exists():
            raise ExtractionError(f"Input file not found: {self.input_path}")
        if not self.input_path.is_file():
            raise ExtractionError(f"Input path is not a file: {self.input_path}")
        return True

    def process(self, _: None) -> Iterator[Storm]:
        """Process HURDAT2 file and yield Storm objects"""
        logger.info(f"Starting extraction from {self.input_path}")

        try:
            self.validate_file()
            yield from parse_hurdat2(self.input_path)

        except Exception as e:
            logger.error(f"Extraction failed: {e!s}")
            raise ExtractionError(f"Failed to extract data: {e!s}") from e
