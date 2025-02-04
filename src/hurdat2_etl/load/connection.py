"""Database Connection Management Module

This module provides enhanced database connection management with:
- Connection pooling
- Lifecycle management
- Extension loading
- PRAGMA configuration
"""

import logging
from os import PathLike
from queue import Queue
from typing import Union

import pysqlite3 as sqlite3  # type: ignore

from ..config.settings import Settings
from ..exceptions import DatabaseConnectionError

# Type alias for path arguments
PathType = Union[str, "PathLike[str]"]


class DatabaseManager:
    """Manages a pool of database connections."""

    def __init__(self, db_path: PathType, pool_size: int = 5):
        """Initialize the connection pool.

        Args:
            db_path: Path to the SQLite database file
            pool_size: Number of connections to maintain in the pool

        Raises:
            DatabaseConnectionError: If pool initialization fails
        """
        self.db_path = db_path
        self.pool_size = pool_size
        self.connection_pool: Queue[sqlite3.Connection] = Queue()
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Initialize the connection pool.

        Raises:
            DatabaseConnectionError: If connection creation fails
        """
        try:
            for _ in range(self.pool_size):
                conn = self._create_connection()
                self.connection_pool.put(conn)
            logging.debug(
                f"Initialized connection pool with {self.pool_size} connections"
            )
        except Exception as e:
            raise DatabaseConnectionError(
                f"Failed to initialize connection pool: {e}"
            ) from e

    def _create_connection(self) -> sqlite3.Connection:
        """Create a new database connection with optimized settings.

        Returns:
            sqlite3.Connection: Configured database connection

        Raises:
            DatabaseConnectionError: If connection creation fails
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.enable_load_extension(True)
            conn.load_extension(Settings.SPATIALITE_LIBRARY_PATH)

            # Configure connection with optimal settings
            for pragma, value in Settings.DB_PRAGMA_SETTINGS.items():
                conn.execute(f"PRAGMA {pragma}={value}")

            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")

            return conn
        except Exception as e:
            raise DatabaseConnectionError(
                f"Failed to create database connection: {e}"
            ) from e

    def get_connection(self) -> sqlite3.Connection:
        """Get a connection from the pool.

        Returns:
            sqlite3.Connection: Database connection

        Raises:
            DatabaseConnectionError: If no connection is available
        """
        try:
            conn = self.connection_pool.get(timeout=Settings.DB_CONNECTION_TIMEOUT)
            logging.debug("Retrieved connection from pool")
            return conn
        except Exception as e:
            raise DatabaseConnectionError(
                f"Failed to get connection from pool: {e}"
            ) from e

    def return_connection(self, conn: sqlite3.Connection) -> None:
        """Return a connection to the pool.

        Args:
            conn: The connection to return

        Raises:
            DatabaseConnectionError: If returning the connection fails
        """
        try:
            self.connection_pool.put(conn)
            logging.debug("Returned connection to pool")
        except Exception as e:
            raise DatabaseConnectionError(
                f"Failed to return connection to pool: {e}"
            ) from e

    def close_all(self) -> None:
        """Close all connections in the pool.

        Raises:
            DatabaseConnectionError: If closing connections fails
        """
        try:
            while not self.connection_pool.empty():
                conn = self.connection_pool.get_nowait()
                conn.close()
            logging.debug("Closed all connections in pool")
        except Exception as e:
            raise DatabaseConnectionError(
                f"Failed to close all connections: {e}"
            ) from e
