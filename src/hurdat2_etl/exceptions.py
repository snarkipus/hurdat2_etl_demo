"""
Custom exceptions for the HURDAT2 ETL pipeline.
All exceptions inherit from base ETLError class.
"""


class ETLError(Exception):
    """Base exception class for ETL pipeline errors"""

    pass


class ExtractionError(ETLError):
    """Raised when data extraction fails"""

    pass


class TransformError(ETLError):
    """Raised when data transformation fails"""

    pass


class LoadError(ETLError):
    """Raised when data loading fails"""

    pass


class ValidationError(ETLError):
    """Raised when data validation fails"""

    pass
