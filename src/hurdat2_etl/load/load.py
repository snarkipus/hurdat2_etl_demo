"""Database Load Module

This module handles all database-related operations including:
- Database initialization and connection management
- Schema creation and management
- Data insertion with validation
- Database validation and reporting

The module uses a connection pool for better performance and implements
comprehensive error handling with specific exceptions for different failure modes.
"""

import logging
import os
from collections.abc import Iterator
from os import PathLike
from typing import Any, Union

import apsw

from ..config.settings import Settings
from ..core import ETLStage
from ..exceptions import (
    DatabaseInitializationError,
    DatabaseInsertionError,
    DatabaseValidationError,
)
from ..models import Storm
from .connection import DatabaseManager

# Type alias for path arguments
PathType = Union[str, "PathLike[str]"]


class Load(ETLStage[Iterator[Storm], None]):
    """Load stage for HURDAT2 data.

    This stage handles database operations including initialization,
    data insertion, and validation.

    Args:
        db_path: Path to the database file. If None, uses default from Settings.
        batch_size: Number of records to insert in each batch.
        progress_enabled: Whether to enable progress tracking.

    Raises:
        ValueError: If batch_size is not positive.
    """

    def __init__(
        self,
        db_path: PathType | None = None,
        batch_size: int | None = None,
        progress_enabled: bool = True,
    ) -> None:
        super().__init__(progress_enabled=progress_enabled)
        self.db_path = db_path or Settings.DB_PATH

        # Validate batch size
        if batch_size is not None and batch_size <= 0:
            raise ValueError("Batch size must be positive")
        self.batch_size = batch_size or Settings.DB_BATCH_SIZE

    def init_database(self) -> None:
        """Initialize a fresh Spatialite database with schema."""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
            logging.info(f"Removed existing database: {self.db_path}")

        try:
            manager = DatabaseManager(self.db_path)
            conn = manager.get_connection()

            try:
                cursor = conn.cursor()
                cursor.execute("SELECT InitSpatialMetadata(1);")

                # Create base tables with detailed schema
                cursor = conn.cursor()
                cursor.execute(
                    """
                    DROP TABLE IF EXISTS observations;
                    DROP TABLE IF EXISTS storms;

                    CREATE TABLE storms (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        basin TEXT NOT NULL,
                        cyclone_number INTEGER NOT NULL,
                        year INTEGER NOT NULL,
                        name TEXT NOT NULL,
                        UNIQUE(basin, cyclone_number, year),
                        CONSTRAINT valid_basin CHECK (basin IN ('AL', 'EP', 'CP')),
                        CONSTRAINT valid_cyclone_number CHECK (cyclone_number > 0),
                        CONSTRAINT valid_year CHECK (year >= 1851)
                    );

                    CREATE TABLE observations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        storm_id INTEGER NOT NULL,
                        date TEXT NOT NULL,
                        record_identifier TEXT,
                        status TEXT NOT NULL,
                        max_wind INTEGER,
                        min_pressure INTEGER,
                        ne34 INTEGER,
                        se34 INTEGER,
                        sw34 INTEGER,
                        nw34 INTEGER,
                        ne50 INTEGER,
                        se50 INTEGER,
                        sw50 INTEGER,
                        nw50 INTEGER,
                        ne64 INTEGER,
                        se64 INTEGER,
                        sw64 INTEGER,
                        nw64 INTEGER,
                        max_wind_radius INTEGER,
                        FOREIGN KEY(storm_id) REFERENCES storms(id) ON DELETE CASCADE
                    );

                    -- Create indices for common queries
                    CREATE INDEX idx_storms_year ON storms(year);
                    CREATE INDEX idx_storms_basin ON storms(basin);
                    CREATE INDEX idx_observations_date ON observations(date);
                    CREATE INDEX idx_observations_status ON observations(status);
                """
                )

                # Add spatial support with validation
                conn.execute(
                    "SELECT AddGeometryColumn('observations', 'geom', 4326, 'POINT', "
                    "'XY');"
                )
                conn.execute(
                    """
                    CREATE TRIGGER observations_geom_validate
                    BEFORE INSERT ON observations
                    FOR EACH ROW
                    BEGIN
                        SELECT CASE
                            WHEN NEW.geom IS NULL THEN
                                RAISE(ROLLBACK, 'Geometry cannot be null')
                            WHEN GeometryType(NEW.geom) != 'POINT' THEN
                                RAISE(ROLLBACK, 'Invalid geometry type')
                            WHEN ST_SRID(NEW.geom) != 4326 THEN
                                RAISE(ROLLBACK, 'Invalid SRID')
                            WHEN ST_X(NEW.geom) < -180 OR ST_X(NEW.geom) > 180 THEN
                                RAISE(ROLLBACK, 'Longitude out of range (-180 to 180)')
                            WHEN ST_Y(NEW.geom) < -90 OR ST_Y(NEW.geom) > 90 THEN
                                RAISE(ROLLBACK, 'Latitude out of range (-90 to 90)')
                        END;
                    END;
                """
                )
                # Add observations data validation trigger
                conn.execute(
                    """
                    CREATE TRIGGER observations_validate
                    BEFORE INSERT ON observations
                    FOR EACH ROW
                    BEGIN
                        SELECT CASE
                            WHEN NEW.status NOT IN (
                                'TD', 'TS', 'HU', 'EX', 'SD', 'SS', 'LO', 'WV', 'DB'
                            ) THEN
                                RAISE(ROLLBACK, 'Invalid storm status')
                            WHEN NEW.max_wind < 0
                                AND NEW.max_wind NOT IN (-999, -99) THEN
                                RAISE(ROLLBACK, 'Invalid max wind value')
                            WHEN NEW.min_pressure < 0
                                AND NEW.min_pressure NOT IN (-999, -99) THEN
                                RAISE(ROLLBACK, 'Invalid min pressure value')
                        END;
                    END;
                    """
                )

                conn.execute("SELECT CreateSpatialIndex('observations', 'geom');")
                cur = conn.cursor()
                # No explicit commit needed - handled by transaction manager
                logging.info(
                    "Database initialized successfully with enhanced schema "
                    "and validation"
                )

            except Exception as e:
                cur = conn.cursor()
                cur.execute("ROLLBACK")
                raise DatabaseInitializationError(
                    f"Failed to initialize database schema: {e}"
                ) from e
            finally:
                manager.return_connection(conn)
                manager.close_all()

        except Exception as e:
            raise DatabaseInitializationError(
                f"Database initialization failed: {e}"
            ) from e

    def insert_storms(self, storms: list[Storm]) -> None:
        """Insert storm data into the database with progress tracking."""
        if not storms:
            raise ValueError("No storm data provided for insertion")

        try:
            manager = DatabaseManager(self.db_path)
            conn = manager.get_connection()
            cur = conn.cursor()

            try:
                # APSW automatically starts transactions on first execute()

                self.init_progress(len(storms), "Loading storms")

                for storm in storms:
                    try:
                        # Validate storm data
                        if not all(
                            [storm.basin, storm.cyclone_number, storm.year, storm.name]
                        ):
                            raise ValueError(f"Invalid storm data: {storm}")

                        # Insert storm record with parameter binding
                        cur.execute(
                            """
                            INSERT INTO storms (basin, cyclone_number, year, name)
                            VALUES (?, ?, ?, ?)
                        """,
                            (storm.basin, storm.cyclone_number, storm.year, storm.name),
                        )

                        storm_id = cur.getconnection().last_insert_rowid()

                        # Process observations in batches
                        for i in range(0, len(storm.observations), self.batch_size):
                            batch = storm.observations[i : i + self.batch_size]
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

                        self.update_progress()

                    except Exception as e:
                        raise DatabaseInsertionError(
                            f"Failed to process storm {storm.name}: {e!s}"
                        ) from e

                # APSW auto-commits when leaving transaction scope
                logging.info(
                    f"Successfully inserted {len(storms)} storms into database"
                )

            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except apsw.SQLError as rollback_error:
                    if "no transaction is active" not in str(rollback_error):
                        # Only ignore the specific "no transaction" error
                        raise rollback_error
                raise DatabaseInsertionError(f"Database insertion failed: {e!s}") from e
            finally:
                self.close_progress()
                manager.return_connection(conn)
                manager.close_all()

        except Exception as e:
            raise DatabaseInsertionError(f"Database operation failed: {e!s}") from e

    def validate_database(self) -> dict[str, Any]:
        """Validate database contents and structure with enhanced checks."""
        try:
            manager = DatabaseManager(self.db_path)
            conn = manager.get_connection()
            cur = conn.cursor()

            validation_results = {}

            try:
                # Structure validation with detailed schema analysis
                cur.execute(
                    """
                    SELECT type, name, sql
                    FROM sqlite_master
                    WHERE type IN ('table','index','trigger')
                    AND name NOT LIKE 'sqlite_%'
                    ORDER BY type, name
                """
                )
                validation_results["schema"] = cur.fetchall()

                # Comprehensive basin coverage analysis
                cur.execute(
                    """
                    SELECT basin,
                           COUNT(*) as storm_count,
                           MIN(year) as first_year,
                           MAX(year) as last_year,
                           COUNT(DISTINCT year) as active_years,
                           -- Additional statistics
                           AVG(
                               (SELECT COUNT(*)
                                FROM observations
                                WHERE storm_id = storms.id)
                           ) as avg_observations_per_storm
                    FROM storms
                    GROUP BY basin
                    ORDER BY storm_count DESC
                """
                )
                validation_results["basin_stats"] = cur.fetchall()

                # Enhanced intensity distribution analysis
                cur.execute(
                    f"""
                    WITH intensity_categories AS (
                        SELECT
                            CASE
                                WHEN max_wind <= 33 THEN 'TD'
                                WHEN max_wind <= 63 THEN 'TS'
                                WHEN max_wind <= 95 THEN 'Cat1-2'
                                ELSE 'Cat3+'
                            END as category,
                            min_pressure,
                            max_wind,
                            date
                        FROM observations
                        WHERE max_wind NOT IN (
                            {", ".join(str(v) for v in Settings.MISSING_VALUES)}
                        )
                    )
                    SELECT
                        category,
                        COUNT(*) as count,
                        MIN(min_pressure) as min_pressure,
                        AVG(min_pressure) as avg_pressure,
                        MAX(max_wind) as max_wind,
                        strftime('%Y', MIN(date)) as earliest_occurrence,
                        strftime('%Y', MAX(date)) as latest_occurrence
                    FROM intensity_categories
                    GROUP BY category
                    ORDER BY count DESC
                """
                )
                validation_results["intensity_stats"] = cur.fetchall()

                # Comprehensive spatial analysis
                cur.execute(
                    """
                    WITH raw_bounds AS (
                        SELECT
                            X(geom) as lon,
                            Y(geom) as lat,
                            strftime('%m', date) as month
                        FROM observations
                    ),
                    normalized_bounds AS (
                        SELECT
                            CASE
                                WHEN lon > 180 THEN lon - 360
                                WHEN lon < -180 THEN lon + 360
                                ELSE lon
                            END as norm_lon,
                            lat,
                            month
                        FROM raw_bounds
                    )
                    SELECT
                        MIN(norm_lon) as min_lon,
                        MAX(norm_lon) as max_lon,
                        MIN(lat) as min_lat,
                        MAX(lat) as max_lat,
                        COUNT(*) as total_observations,
                        COUNT(DISTINCT month) as active_months,
                        (
                            SELECT COUNT(DISTINCT storm_id)
                            FROM observations
                        ) as total_storms
                    FROM normalized_bounds
                """
                )
                spatial_stats = cur.fetchone()
                if spatial_stats:
                    validation_results["spatial_stats"] = spatial_stats
                else:
                    validation_results["spatial_stats"] = []

                return validation_results

            except Exception as e:
                raise DatabaseValidationError(f"Validation query failed: {e!s}") from e
            finally:
                manager.return_connection(conn)
                manager.close_all()

        except Exception as e:
            raise DatabaseValidationError(f"Database validation failed: {e!s}") from e

    def process(self, data: Iterator[Storm]) -> None:
        """Process and load Storm objects into the database.

        This method orchestrates the complete database loading process:
        1. Database initialization with schema creation
        2. Data insertion with batch processing
        3. Comprehensive validation
        4. Detailed reporting

        Args:
            data: Iterator of Storm objects to load.

        Raises:
            DatabaseError: If any part of the loading process fails
            ValueError: If input validation fails
        """
        # Convert iterator to list for multiple passes
        storms = list(data)
        if not storms:
            raise ValueError("No storm data provided")

        try:
            # Initialize database
            self.init_database()

            # Insert data with progress tracking
            self.insert_storms(storms)

            # Validate database
            validation_results = self.validate_database()

            # Generate comprehensive validation report
            logging.info("\nDatabase Validation Report")
            logging.info("=======================")

            logging.info("\nSchema Overview:")
            for type_, name, sql in validation_results["schema"]:
                logging.info(f"  {type_}: {name}")
                if sql:  # Log detailed schema for debugging
                    logging.debug(f"  SQL: {sql}")

            logging.info("\nBasin Coverage:")
            for basin, count, start, end, years, avg_obs in validation_results[
                "basin_stats"
            ]:
                logging.info(
                    f"  {basin}: {count} storms over {years} years ({start}-{end})"
                    f"\n    Average observations per storm: {avg_obs:.1f}"
                )

            logging.info("\nIntensity Distribution:")
            for stats in validation_results["intensity_stats"]:
                cat, count, min_p, avg_p, max_w, earliest, latest = stats
                logging.info(
                    f"  {cat}: {count} observations"
                    f"\n    Pressure range: {min_p}-{avg_p:.1f} mb"
                    f"\n    Max wind: {max_w} kt"
                    f"\n    Period: {earliest}-{latest}"
                )

            stats = validation_results["spatial_stats"]
            min_lon, max_lon, min_lat, max_lat, obs_count, months, storm_count = stats

            logging.info("\nSpatial Coverage:")
            logging.info(
                f"  Bounds: {abs(min_lon):.1f}°{'W' if min_lon < 0 else 'E'} to "
                f"{abs(max_lon):.1f}°{'W' if max_lon < 0 else 'E'}, "
                f"{min_lat:.1f}°{'S' if min_lat < 0 else 'N'} to "
                f"{max_lat:.1f}°N"
            )
            logging.info(
                f"  Coverage: {obs_count} observations across {storm_count} storms"
                f"\n    Active in {months} months of the year"
            )

            logging.info("\nDatabase load completed successfully")

        except Exception as e:
            logging.error(f"Database load failed: {e!s}")
            raise
