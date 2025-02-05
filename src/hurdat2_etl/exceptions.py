"""
Custom exceptions for the HURDAT2 ETL pipeline.
All exceptions inherit from base ETLError class.
"""


class ETLError(Exception):
    """Base exception class for ETL pipeline errors."""

    pass


class ExtractionError(ETLError):
    """Raised when data extraction fails."""

    pass


class TransformError(ETLError):
    """Raised when data transformation fails."""

    pass


class LoadError(ETLError):
    """Raised when data loading fails."""

    pass


class DatabaseError(ETLError):
    """Base class for database-related errors."""

    pass


class DatabaseConnectionError(DatabaseError):
    """Raised when database connection fails."""

    pass


class DatabaseInitializationError(DatabaseError):
    """Raised when database initialization fails."""

    pass


class DatabaseInsertionError(DatabaseError):
    """Raised when data insertion fails."""

    pass


class DatabaseValidationError(DatabaseError):
    """Raised when database validation fails."""

    pass


class ValidationError(ETLError):
    """Raised when data validation fails."""

    pass


class ProgressError(ETLError):
    """Raised when progress tracking operations fail.

    This includes errors such as:
    - Invalid progress bar initialization
    - Progress tracking state errors
    - Progress bar update failures
    """

    pass
