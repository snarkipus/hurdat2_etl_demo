"""Database Schema Management Module

This module handles database schema operations including:
- Table creation and management
- Trigger definitions
- Index creation
- Spatial extension setup
"""

import logging
import os
from typing import Any

from ..exceptions import DatabaseInitializationError
from .connection import DatabaseManager, PathType


class SchemaManager:
    """Manages database schema operations."""

    def __init__(self, db_path: PathType):
        """Initialize the schema manager.

        Args:
            db_path: Path to the database file
        """
        self.db_path = db_path
        self.manager = DatabaseManager(db_path)

    def initialize_database(self) -> None:
        """Initialize a fresh database with complete schema.

        This includes:
        - Spatial extensions and metadata
        - Tables for storms and observations
        - Spatial indices and triggers
        - Foreign key constraints

        Raises:
            DatabaseInitializationError: If initialization fails
        """
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
            logging.info(f"Removed existing database: {self.db_path}")

        try:
            conn = self.manager.get_connection()
            try:
                self._init_spatial_metadata(conn)
                self._create_base_tables(conn)
                self._add_spatial_support(conn)
                self._create_validation_triggers(conn)
                self._create_indices(conn)
                conn.cursor().execute("COMMIT")
                logging.info("Database initialized successfully with enhanced schema")
            except Exception as e:
                conn.cursor().execute("ROLLBACK")
                raise DatabaseInitializationError(
                    f"Failed to initialize database schema: {e}"
                ) from e
            finally:
                self.manager.return_connection(conn)
                self.manager.close_all()
        except Exception as e:
            raise DatabaseInitializationError(
                f"Database initialization failed: {e}"
            ) from e

    def _init_spatial_metadata(self, conn: Any) -> None:
        """Initialize spatial metadata.

        Args:
            conn: Database connection

        Raises:
            DatabaseInitializationError: If spatial initialization fails
        """
        try:
            conn.execute("SELECT InitSpatialMetadata(1);")
        except Exception as e:
            raise DatabaseInitializationError(
                f"Failed to initialize spatial metadata: {e}"
            ) from e

    def _create_base_tables(self, conn: Any) -> None:
        """Create base tables with constraints.

        Args:
            conn: Database connection

        Raises:
            DatabaseInitializationError: If table creation fails
        """
        try:
            conn.executescript(
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
                """
            )
        except Exception as e:
            raise DatabaseInitializationError(
                f"Failed to create base tables: {e}"
            ) from e

    def _add_spatial_support(self, conn: Any) -> None:
        """Add spatial support to observations table.

        Args:
            conn: Database connection

        Raises:
            DatabaseInitializationError: If spatial support setup fails
        """
        try:
            conn.execute(
                "SELECT AddGeometryColumn('observations', 'geom', 4326, 'POINT', 'XY');"
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
        except Exception as e:
            raise DatabaseInitializationError(
                f"Failed to add spatial support: {e}"
            ) from e

    def _create_validation_triggers(self, conn: Any) -> None:
        """Create data validation triggers.

        Args:
            conn: Database connection

        Raises:
            DatabaseInitializationError: If trigger creation fails
        """
        try:
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
                        WHEN NEW.max_wind < 0 AND NEW.max_wind NOT IN (-999, -99) THEN
                            RAISE(ROLLBACK, 'Invalid max wind value')
                        WHEN NEW.min_pressure < 0
                            AND NEW.min_pressure NOT IN (-999, -99) THEN
                            RAISE(ROLLBACK, 'Invalid min pressure value')
                    END;
                END;
                """
            )
        except Exception as e:
            raise DatabaseInitializationError(
                f"Failed to create validation triggers: {e}"
            ) from e

    def _create_indices(self, conn: Any) -> None:
        """Create database indices for optimization.

        Args:
            conn: Database connection

        Raises:
            DatabaseInitializationError: If index creation fails
        """
        try:
            conn.executescript(
                """
                CREATE INDEX idx_storms_year ON storms(year);
                CREATE INDEX idx_storms_basin ON storms(basin);
                CREATE INDEX idx_observations_date ON observations(date);
                CREATE INDEX idx_observations_status ON observations(status);
                """
            )
            conn.execute("SELECT CreateSpatialIndex('observations', 'geom');")
        except Exception as e:
            raise DatabaseInitializationError(f"Failed to create indices: {e}") from e
