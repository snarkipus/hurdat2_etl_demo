"""Database Connection Management Module

This module provides enhanced database connection management with:
- Connection pooling
- Lifecycle management
- Extension loading
- PRAGMA configuration
"""

import logging
from os import PathLike
from queue import Empty, Queue
from typing import Union

import apsw

from ..config.settings import Settings
from ..exceptions import DatabaseConnectionError

# Type alias for path arguments
PathType = Union[str, "PathLike[str]"]


class DatabaseManager:
    """Manages a pool of database connections."""

    connection_pool: Queue[apsw.Connection]  # Explicit type declaration

    def __init__(self, db_path: PathType, pool_size: int = Settings.DB_POOL_SIZE):
        """Initialize the connection pool.

        Args:
            db_path: Path to the SQLite database file
            pool_size: Number of connections to maintain in the pool

        Raises:
            DatabaseConnectionError: If pool initialization fails
        """
        self.db_path = db_path
        self.pool_size = pool_size
        self.connection_pool: Queue[apsw.Connection] = Queue()
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

    def _create_connection(self) -> apsw.Connection:
        """Create a new database connection with optimized settings.

        Returns:
            apsw.Connection: Configured database connection

        Raises:
            DatabaseConnectionError: If connection creation fails
        """
        try:
            conn = apsw.Connection(str(self.db_path))
            conn.enableloadextension(True)
            conn.loadextension(Settings.SPATIALITE_LIBRARY_PATH)

            # Configure connection with optimal settings
            cursor = conn.cursor()
            for pragma, value in Settings.DB_PRAGMA_SETTINGS.items():
                cursor.execute(f"PRAGMA {pragma}={value}")

            # Enable foreign keys
            cursor.execute("PRAGMA foreign_keys = ON")

            return conn
        except Exception as e:
            raise DatabaseConnectionError(f"Connection failed: {e}") from e

    def get_connection(self) -> apsw.Connection:
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
            if isinstance(e, Empty):
                raise DatabaseConnectionError(
                    "Connection retrieval failed: pool exhausted"
                ) from e
            raise DatabaseConnectionError(f"Connection retrieval failed: {e}") from e

    def return_connection(self, conn: apsw.Connection) -> None:
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
            raise DatabaseConnectionError(f"Connection return failed: {e}") from e

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
