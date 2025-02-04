"""Database Load Package

This package provides functionality for loading storm data into a Spatialite database.
It handles:
- Database initialization and connection management
- Schema creation and management
- Data insertion with validation
- Database validation and reporting
"""

from ..config.settings import Settings
from ..exceptions import DatabaseError
from ..models import Storm
from .connection import DatabaseManager
from .operations import DatabaseOperations
from .reporting import DatabaseReporter
from .schema import SchemaManager


def load_data(storms: list[Storm]) -> None:
    """Load storm data into the database.

    This function orchestrates the complete database loading process:
    1. Database initialization with schema creation
    2. Data insertion with batch processing
    3. Comprehensive validation
    4. Detailed reporting

    Args:
        storms: List of Storm objects to load into the database

    Raises:
        DatabaseError: If any part of the loading process fails
        ValueError: If input validation fails
    """
    if not storms:
        raise ValueError("No storm data provided")

    db_path = Settings.DB_PATH

    try:
        # Initialize database schema
        schema_manager = SchemaManager(db_path)
        schema_manager.initialize_database()

        # Insert data
        operations = DatabaseOperations(db_path)
        operations.insert_storms(storms, Settings.DB_BATCH_SIZE)

        # Validate and report
        reporter = DatabaseReporter(db_path)
        validation_results = reporter.validate_database()
        reporter.generate_report(validation_results)

    except Exception as e:
        raise DatabaseError(f"Database load failed: {e!s}") from e


__all__ = [
    "DatabaseManager",
    "DatabaseOperations",
    "DatabaseReporter",
    "SchemaManager",
    "load_data",
]
