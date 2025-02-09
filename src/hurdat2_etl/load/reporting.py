"""Database Reporting Module

This module handles database validation and reporting including:
- Schema validation
- Data integrity checks
- Coverage analysis
- Statistical reporting
"""

import logging
from typing import Any, cast

from apsw import Cursor

from ..config.settings import Settings
from ..exceptions import DatabaseValidationError
from .connection import DatabaseManager, PathType

# Type aliases for query results
SchemaRow = tuple[str, str, str]
BasinRow = tuple[str, int, int, int, int, float]
IntensityRow = tuple[str, int, int, float, int, str, str]
SpatialRow = tuple[float, float, float, float, int, int, int]


class DatabaseReporter:
    """Handles database validation and reporting."""

    def __init__(self, db_path: PathType):
        """Initialize the database reporter.

        Args:
            db_path: Path to the database file
        """
        self.db_path = db_path
        self.manager = DatabaseManager(db_path)

    def validate_database(self) -> dict[str, Any]:
        """Perform comprehensive database validation.

        Returns:
            Dict containing validation results including:
            - Schema information
            - Basin statistics
            - Intensity distribution
            - Spatial coverage

        Raises:
            DatabaseValidationError: If validation fails
        """
        try:
            conn = self.manager.get_connection()
            cur = conn.cursor()

            try:
                return {
                    "schema": self._validate_schema(cur),
                    "basin_stats": self._analyze_basin_coverage(cur),
                    "intensity_stats": self._analyze_intensity_distribution(cur),
                    "spatial_stats": self._analyze_spatial_coverage(cur),
                }
            except Exception as e:
                raise DatabaseValidationError(f"Validation query failed: {e!s}") from e
            finally:
                self.manager.return_connection(conn)
                self.manager.close_all()

        except Exception as e:
            raise DatabaseValidationError(f"Database validation failed: {e!s}") from e

    def _validate_schema(self, cur: Cursor) -> list[tuple[str, str, str]]:
        """Validate database schema.

        Args:
            cur: Database cursor

        Returns:
            List of tuples containing (type, name, sql) for database objects

        Raises:
            DatabaseValidationError: If schema validation fails
        """
        try:
            cur.execute(
                """
                SELECT type, name, sql
                FROM sqlite_master
                WHERE type IN ('table','index','trigger')
                AND name NOT LIKE 'sqlite_%'
                ORDER BY type, name
                """
            )
            results = cur.fetchall()
            return [cast(SchemaRow, (str(t), str(n), str(s))) for t, n, s in results]
        except Exception as e:
            raise DatabaseValidationError(f"Schema validation failed: {e!s}") from e

    def _analyze_basin_coverage(
        self, cur: Cursor
    ) -> list[tuple[str, int, int, int, int, float]]:
        """Analyze basin coverage statistics.

        Args:
            cur: Database cursor

        Returns:
            List of tuples containing basin statistics:
            (basin, storm_count, first_year, last_year, active_years, avg_observations)

        Raises:
            DatabaseValidationError: If analysis fails
        """
        try:
            cur.execute(
                """
                SELECT basin,
                       COUNT(*) as storm_count,
                       MIN(year) as first_year,
                       MAX(year) as last_year,
                       COUNT(DISTINCT year) as active_years,
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
            results = cur.fetchall()
            return [
                cast(
                    BasinRow,
                    (
                        str(b or ""),
                        int(c or 0),
                        int(s or 0),
                        int(e or 0),
                        int(y or 0),
                        float(a or 0.0),
                    ),
                )
                for b, c, s, e, y, a in results
            ]
        except Exception as e:
            raise DatabaseValidationError(
                f"Basin coverage analysis failed: {e!s}"
            ) from e

    def _analyze_intensity_distribution(
        self, cur: Cursor
    ) -> list[tuple[str, int, int, float, int, str, str]]:
        """Analyze storm intensity distribution."""
        try:
            cur.execute(
                """
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
                    WHERE max_wind NOT IN ("""
                + ", ".join(str(v) for v in Settings.MISSING_VALUES)
                + """)
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
            results = cur.fetchall()
            return [
                cast(
                    IntensityRow,
                    (
                        str(cat),
                        int(cnt),
                        int(minp),
                        float(avgp),
                        int(maxw),
                        str(early),
                        str(late),
                    ),
                )
                for cat, cnt, minp, avgp, maxw, early, late in [
                    (
                        cat or "",
                        cnt or 0,
                        minp or 0,
                        avgp or 0.0,
                        maxw or 0,
                        early or "",
                        late or "",
                    )
                    for cat, cnt, minp, avgp, maxw, early, late in results
                ]
            ]
        except Exception as e:
            raise DatabaseValidationError(
                f"Intensity distribution analysis failed: {e!s}"
            ) from e

    def _analyze_spatial_coverage(
        self, cur: Cursor
    ) -> tuple[float, float, float, float, int, int, int]:
        """Analyze spatial coverage of observations.

        Args:
            cur: Database cursor

        Returns:
            Tuple containing spatial statistics:
            (min_lon, max_lon, min_lat, max_lat, total_observations,
             active_months, total_storms)

        Raises:
            DatabaseValidationError: If analysis fails
        """
        try:
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
            result = cur.fetchone()
            if result is None:
                raise DatabaseValidationError("No spatial data found")
            return cast(
                SpatialRow,
                (
                    float(result[0]),
                    float(result[1]),
                    float(result[2]),
                    float(result[3]),
                    int(result[4]),
                    int(result[5]),
                    int(result[6]),
                ),
            )
        except Exception as e:
            raise DatabaseValidationError(
                f"Spatial coverage analysis failed: {e!s}"
            ) from e

    def generate_report(self, validation_results: dict[str, Any]) -> None:
        """Generate a comprehensive validation report.

        Args:
            validation_results: Results from validate_database()
        """
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

        logging.info("\nDatabase validation completed successfully")
