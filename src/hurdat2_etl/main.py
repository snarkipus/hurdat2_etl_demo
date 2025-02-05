"""ETL Main Script

This script orchestrates the ETL pipeline for HURDAT2 data processing.
It configures and executes the Extract, Transform, and Load stages
with progress tracking and comprehensive error handling.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import NoReturn

from .config.settings import Settings
from .core import ETLPipeline
from .exceptions import ETLError
from .extract.extract import Extract
from .load.load import Load
from .transform.transform import Transform

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments.
    """
    parser = argparse.ArgumentParser(
        description="HURDAT2 ETL Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Path to HURDAT2 data file",
        default=Settings.HURDAT2_DATA_FILE,
    )
    parser.add_argument(
        "--db",
        type=Path,
        help="Path to output database",
        default=Settings.DB_PATH,
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Database insertion batch size",
        default=Settings.DB_BATCH_SIZE,
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def setup_logging(debug: bool = False) -> None:
    """Configure logging with appropriate level and format.

    Args:
        debug: Whether to enable debug logging.
    """
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def run_etl(args: argparse.Namespace) -> None:
    """Execute the ETL pipeline with the specified configuration.

    Args:
        args: Command line arguments.

    Raises:
        ETLError: If any stage of the pipeline fails.
    """
    logger.info("Starting HURDAT2 ETL pipeline")

    try:
        # Configure pipeline stages
        extract = Extract(
            input_path=args.input,
            progress_enabled=not args.no_progress,
        )
        transform = Transform(
            progress_enabled=not args.no_progress,
        )
        load = Load(
            db_path=args.db,
            batch_size=args.batch_size,
            progress_enabled=not args.no_progress,
        )

        # Create and run pipeline
        pipeline = ETLPipeline([extract, transform, load])
        pipeline.run(None)  # Extract stage takes no input

        logger.info("ETL pipeline completed successfully")

    except ETLError as e:
        logger.error(f"ETL pipeline failed: {e!s}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e!s}")
        raise ETLError(f"Pipeline failed with unexpected error: {e!s}") from e


def main() -> NoReturn:
    """Main entry point for the ETL pipeline.

    This function:
    1. Parses command line arguments
    2. Sets up logging
    3. Executes the ETL pipeline
    4. Handles errors and sets exit code
    """
    try:
        args = parse_args()
        setup_logging(args.debug)
        run_etl(args)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e!s}")
        sys.exit(1)


if __name__ == "__main__":
    main()
