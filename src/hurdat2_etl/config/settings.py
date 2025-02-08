"""ETL Project Configuration Settings"""

from pathlib import Path
from typing import Final


class Settings:
    """ETL configuration settings"""

    # Base directory
    BASE_DIR: Final[Path] = Path(__file__).parent.parent.parent.parent

    # Data extraction settings
    SOURCE_PATH: str = "data/source"
    TARGET_PATH: str = "data/target"

    # Data files
    HURDAT2_DATA_FILE: Final[Path] = BASE_DIR / "ref" / "hurdat2-1851-2023-051124.txt"

    # Database configuration
    DB_NAME: str = "hurdat2.db"
    DB_PATH: Final[Path] = BASE_DIR / DB_NAME

    # SQLite configuration
    SPATIALITE_LIBRARY_PATH: str = "/usr/lib/x86_64-linux-gnu/mod_spatialite.so"
    DB_BATCH_SIZE: int = 100  # Number of records per batch insert
    DB_CONNECTION_TIMEOUT: Final[float] = 5.0  # Seconds to wait for connection
    # Connection pool and database engine settings
    DB_POOL_SIZE: int = 5
    DB_PRAGMA_SETTINGS: Final[dict[str, int | str]] = {
        "foreign_keys": 1,  # Enforce foreign key constraints
        "journal_mode": "WAL",  # Write-Ahead Logging
        "synchronous": "NORMAL",  # Balanced durability/performance
        "cache_size": -2000,  # 2MB cache
    }

    # Logging configuration
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "etl.log"

    # Validation
    MISSING_VALUES: Final[set[int]] = {-999, -99}
    MAX_LATITUDE: Final[float] = 90.0
    MAX_LONGITUDE: Final[float] = 360.0
