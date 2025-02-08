"""Database Operations Module

This module handles data operations including:
- Data insertion with batch processing
- Transaction management
- Basic validation checks
"""

import logging

from apsw import Cursor
from tqdm.auto import tqdm

from ..exceptions import DatabaseInsertionError
from ..models import Storm
from .connection import DatabaseManager, PathType


class DatabaseOperations:
    """Handles database operations for storm data."""

    def __init__(self, db_path: PathType):
        """Initialize database operations.

        Args:
            db_path: Path to the database file
        """
        self.db_path = db_path
        self.manager = DatabaseManager(db_path)

    def insert_storms(self, storms: list[Storm], batch_size: int) -> None:
        """Insert storm data with batch processing.

        Args:
            storms: List of Storm objects to insert
            batch_size: Size of batches for processing

        Raises:
            DatabaseInsertionError: If insertion fails
            ValueError: If input validation fails
        """
        if not storms:
            raise ValueError("No storm data provided for insertion")

        if batch_size <= 0:
            raise ValueError("Batch size must be positive")

        try:
            conn = self.manager.get_connection()
            cur = conn.cursor()

            try:
                cur.execute("BEGIN TRANSACTION")
                self._process_storms(cur, storms, batch_size)
                conn.cursor().execute("COMMIT")
                logging.info(
                    f"Successfully inserted {len(storms)} storms into database"
                )

            except Exception as e:
                conn.cursor().execute("ROLLBACK")
                raise DatabaseInsertionError(f"Database insertion failed: {e!s}") from e
            finally:
                self.manager.return_connection(conn)
                self.manager.close_all()

        except Exception as e:
            raise DatabaseInsertionError(f"Database operation failed: {e!s}") from e

    def _process_storms(
        self, cur: Cursor, storms: list[Storm], batch_size: int
    ) -> None:
        """Process storms in batches.

        Args:
            cur: Database cursor
            storms: List of storms to process
            batch_size: Size of batches

        Raises:
            DatabaseInsertionError: If processing fails
            ValueError: If storm data is invalid
        """
        for storm in tqdm(storms, desc="Processing storms"):
            try:
                storm_id = self._insert_storm(cur, storm)
                self._process_observations(cur, storm_id, storm, batch_size)
            except Exception as e:
                raise DatabaseInsertionError(
                    f"Failed to process storm {storm.name}: {e!s}"
                ) from e

    def _insert_storm(self, cur: Cursor, storm: Storm) -> int:
        """Insert a single storm record.

        Args:
            cur: Database cursor
            storm: Storm object to insert

        Returns:
            int: ID of inserted storm

        Raises:
            ValueError: If storm data is invalid
            DatabaseInsertionError: If insertion fails
        """
        if not all([storm.basin, storm.cyclone_number, storm.year, storm.name]):
            raise ValueError(f"Invalid storm data: {storm}")

        try:
            cur.execute(
                """
                INSERT INTO storms (basin, cyclone_number, year, name)
                VALUES (?, ?, ?, ?)
                """,
                (storm.basin, storm.cyclone_number, storm.year, storm.name),
            )

            # Handle the Optional[int] type of lastrowid
            lastrowid = cur.connection.last_insert_rowid()
            if lastrowid is None:
                raise DatabaseInsertionError("Failed to get inserted storm ID")
            return lastrowid

        except Exception as e:
            raise DatabaseInsertionError(f"Failed to insert storm record: {e!s}") from e

    def _process_observations(
        self, cur: Cursor, storm_id: int, storm: Storm, batch_size: int
    ) -> None:
        """Process storm observations in batches.

        Args:
            cur: Database cursor
            storm_id: ID of the parent storm
            storm: Storm object containing observations
            batch_size: Size of batches

        Raises:
            DatabaseInsertionError: If processing fails
        """
        for i in range(0, len(storm.observations), batch_size):
            batch = storm.observations[i : i + batch_size]
            try:
                values = [
                    (
                        storm_id,
                        obs.date.isoformat(),
                        obs.record_identifier,
                        obs.status.value,
                        obs.max_wind,
                        obs.min_pressure,
                        obs.ne34,
                        obs.se34,
                        obs.sw34,
                        obs.nw34,
                        obs.ne50,
                        obs.se50,
                        obs.sw50,
                        obs.nw50,
                        obs.ne64,
                        obs.se64,
                        obs.sw64,
                        obs.nw64,
                        obs.max_wind_radius,
                        obs.location.to_wkt(),
                    )
                    for obs in batch
                ]

                cur.executemany(
                    """
                    INSERT INTO observations (
                        storm_id, date, record_identifier, status,
                        max_wind, min_pressure,
                        ne34, se34, sw34, nw34,
                        ne50, se50, sw50, nw50,
                        ne64, se64, sw64, nw64,
                        max_wind_radius, geom
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ST_PointFromText(?, 4326)
                    )
                    """,
                    values,
                )
            except Exception as e:
                raise DatabaseInsertionError(
                    f"Failed to insert batch for storm {storm.name} "
                    f"(ID: {storm_id}): {e!s}"
                ) from e
