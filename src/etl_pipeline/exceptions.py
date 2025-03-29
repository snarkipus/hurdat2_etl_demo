"""
Custom exceptions for the HURDAT2 ETL pipeline.
"""


class ETLError(Exception):
    """Base exception class for ETL pipeline errors."""

    pass


class ExtractionError(ETLError):
    """Raised when data extraction fails (e.g., CSV parsing, integer conversion)."""

    pass


class TransformError(ETLError):
    """Raised when data transformation fails (e.g., standardization, normalization)."""

    pass


class LoadError(ETLError):
    """Raised when data loading fails (e.g., database insertion)."""

    pass


class DatabaseError(ETLError):
    """Base class for database-related errors."""

    pass


class DatabaseConnectionError(DatabaseError):
    """Raised when database connection fails (e.g., file access issues)."""

    pass


class DatabaseInsertionError(DatabaseError):
    """Raised when data insertion into the database fails."""

    pass


class ValidationError(ETLError):
    """Raised when Pydantic data validation fails during transformation."""

    pass
