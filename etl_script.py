# ruff: noqa
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import ClassVar, Final, TextIO, TypeVar

import pysqlite3 as sqlite3  # type: ignore
from pydantic import BaseModel, ConfigDict, field_validator
from tqdm import tqdm

# Constants
HURDAT_COLUMN_COUNT: Final = 21
HEADER_COLUMN_COUNT: Final = 3
CYCLONE_ID_LENGTH: Final = 8
MISSING_VALUE: Final = -999


class HurdatParseError(Exception):
    """Custom exception for HURDAT2 parsing errors"""

    pass


T = TypeVar("T", bound="Point")


@dataclass
class Point:
    """Geographic point in WGS84."""

    latitude: float
    longitude: float

    # Class constants for validation
    LAT_RANGE: ClassVar[tuple[float, float]] = (-90.0, 90.0)
    LON_RANGE: ClassVar[tuple[float, float]] = (-180.0, 180.0)

    def __post_init__(self) -> None:
        """Validate and normalize coordinates.

        Raises:
            ValueError: If latitude is out of the accepted range.
        """
        if not self.LAT_RANGE[0] <= self.latitude <= self.LAT_RANGE[1]:
            raise ValueError(f"Latitude {self.latitude} out of range {self.LAT_RANGE}")
        # Normalize longitude to [-180, 180]
        self.longitude = ((self.longitude + 180) % 360) - 180

    @classmethod
    def from_str(cls: type[T], lat_str: str, lon_str: str) -> T:
        """Create Point from HURDAT2 coordinate strings.

        The coordinate strings should include a trailing cardinal direction
        (i.e. 'N', 'S', 'E', 'W'). For example:
          - '34.5N' indicates 34.5 degrees North.
          - '120.3W' indicates 120.3 degrees West.

        Args:
            lat_str (str): Latitude string from HURDAT2.
            lon_str (str): Longitude string from HURDAT2.

        Returns:
            Point: An instance of Point initialized with the parsed coordinates.
        """

        def parse_coord(coord: str) -> float:
            coord = coord.strip()
            value = float(coord[:-1])
            if coord[-1] in ["S", "W"]:
                return -value
            return value

        return cls(
            latitude=parse_coord(lat_str),
            longitude=parse_coord(lon_str),
        )

    def to_wkt(self) -> str:
        """Convert to Well-Known Text format."""
        return f"POINT({self.longitude} {self.latitude})"


class Observation(BaseModel):
    """Represents a single weather observation."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    date: datetime
    time: int
    record_id: str | None
    status: str
    location: Point
    wind_speed: int
    pressure: int
    wind_radii_34kt: dict[str, int]
    wind_radii_50kt: dict[str, int]
    wind_radii_64kt: dict[str, int]
    radius_max_wind: int | None

    @field_validator("date", mode="before")
    @classmethod
    def validate_date(cls, value: int | str | datetime) -> datetime:
        """Validate and convert date values."""
        if isinstance(value, datetime):
            return value
        date_str = str(value).zfill(8) if isinstance(value, int) else str(value)
        try:
            return datetime.strptime(date_str, "%Y%m%d")
        except ValueError as e:
            raise ValueError(f"Invalid date format: {value}") from e

    @field_validator("location", mode="before")
    @classmethod
    def validate_location(cls, value: tuple[str, str] | Point) -> Point:
        if isinstance(value, tuple):
            return Point.from_str(value[0], value[1])
        return value


class Storm(BaseModel):
    """Represents a complete storm record with observations."""

    basin: str
    number: int
    year: int
    name: str
    observation_count: int
    observations: list[Observation] = []


def init_spatialite_db(db_path: str) -> None:
    """Initialize a fresh Spatialite database."""
    if os.path.exists(db_path):
        os.remove(db_path)
        logging.debug(f"Removed existing database: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.enable_load_extension(True)
        conn.load_extension("mod_spatialite")
        conn.execute("SELECT InitSpatialMetadata(1);")

        # Create base tables first
        conn.executescript(
            """
            DROP TABLE IF EXISTS observations;
            DROP TABLE IF EXISTS storms;

            CREATE TABLE storms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                basin TEXT NOT NULL,
                number INTEGER NOT NULL,
                year INTEGER NOT NULL,
                name TEXT,
                observation_count INTEGER,
                UNIQUE(basin, number, year)
            );

            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                storm_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                time INTEGER NOT NULL,
                record_id TEXT,
                status TEXT NOT NULL,
                wind_speed INTEGER NOT NULL,
                pressure INTEGER NOT NULL,
                FOREIGN KEY(storm_id) REFERENCES storms(id)
            );
        """
        )

        # Add spatial support
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
                END;
            END;
        """
        )
        conn.execute("SELECT CreateSpatialIndex('observations', 'geom');")

        # Add data validation
        conn.execute(
            """
            CREATE TRIGGER observations_validate
            BEFORE INSERT ON observations
            FOR EACH ROW
            BEGIN
                SELECT RAISE(ROLLBACK, 'Invalid wind speed')
                WHERE NEW.wind_speed < 0 OR NEW.wind_speed > 200;

                SELECT RAISE(ROLLBACK, 'Invalid pressure')
                WHERE NEW.pressure != -999
                AND (NEW.pressure < 800 OR NEW.pressure > 1100);
            END;
        """
        )

        conn.commit()
        logging.info("Database initialized successfully")
    except Exception as e:
        conn.rollback()
        logging.exception(f"Failed to initialize database: {e}")
        raise
    finally:
        conn.close()


def parse_observation(line: str, debug: bool = False) -> Observation | None:
    """Parse a single line of HURDAT2 observation data into an Observation object.

    Expects the line to have exactly HURDAT_COLUMN_COUNT comma-separated fields.
    This function validates the structure, parses wind radii values,
    and logs detailed errors for debugging purposes.

    Args:
        line (str): A comma-separated record from the HURDAT2 dataset.
        debug (bool, optional): If True, logs detailed debug information.
        Defaults to False.

    Returns:
        Observation | None: An Observation instance if parsing succeeds; None otherwise.
    """
    columns = [col.strip() for col in line.split(",")]
    if len(columns) != HURDAT_COLUMN_COUNT:
        msg = f"Expected {HURDAT_COLUMN_COUNT} columns, but got {len(columns)}: {line}"
        if debug:
            logging.debug(msg)
        else:
            logging.error(msg)
        return None

    try:
        # Parse basic fields
        date = datetime.strptime(columns[0], "%Y%m%d")
        time_val = int(columns[1])
        record_id = columns[2] if columns[2] else None
        status = columns[3]

        # Parse location using the updated Point.from_str method
        location = Point.from_str(columns[4], columns[5])

        wind_speed = int(columns[6])
        pressure = int(columns[7])

        # Parse wind radii values (remaining columns)
        # Expected to be wind radii for various wind thresholds.
        # '-999' values are interpreted as missing (MISSING_VALUE).
        wind_radii = []
        for val in columns[8:]:
            try:
                wind_radii.append(int(val))
            except ValueError:
                wind_radii.append(MISSING_VALUE)

        # Log parsed wind radii if debug is enabled.
        if debug:
            logging.debug(f"Parsed wind radii: {wind_radii}")

        # Create wind radii dictionaries
        wind_radii_34kt = {
            "NE": wind_radii[0],
            "SE": wind_radii[1],
            "SW": wind_radii[2],
            "NW": wind_radii[3],
        }
        wind_radii_50kt = {
            "NE": wind_radii[4],
            "SE": wind_radii[5],
            "SW": wind_radii[6],
            "NW": wind_radii[7],
        }
        wind_radii_64kt = {
            "NE": wind_radii[8],
            "SE": wind_radii[9],
            "SW": wind_radii[10],
            "NW": wind_radii[11],
        }

        # Construct the Observation object, adding wind radii dictionaries as needed.
        return Observation(
            date=date,
            time=time_val,
            record_id=record_id,
            status=status,
            location=location,
            wind_speed=wind_speed,
            pressure=pressure,
            wind_radii_34kt=wind_radii_34kt,
            wind_radii_50kt=wind_radii_50kt,
            wind_radii_64kt=wind_radii_64kt,
            radius_max_wind=wind_radii[12] if wind_radii[12] != MISSING_VALUE else None,
        )
    except Exception:
        logging.exception(f"Failed to parse observation from line: {line}")
        return None


def parse_hurdat2(filepath: Path | str, debug: bool = False) -> list[Storm]:  # noqa: PLR0915
    """Parse HURDAT2 format file into Storm objects.

    Format:
    AL122007,KAREN,19,
    20070925,0000,,TD,10.0N,35.9W,30,1006,  0,  0, 0,  0,  0,  0, 0, 0, 0, 0,0, 0, -999
    20070925,0600,,TS,10.3N,37.0W,35,1005, 40, 30, 0, 40,  0,  0, 0, 0, 0, 0,0, 0, -999
    20070925,1200,,TS,10.6N,38.0W,35,1005, 40, 30, 0, 40,  0,  0, 0, 0, 0, 0,0, 0, -999
    20070925,1800,,TS,10.8N,39.2W,35,1005, 40, 30, 0, 40,  0,  0, 0, 0, 0, 0,0, 0, -999
    20070926,0000,,TS,10.9N,40.4W,40,1003, 60, 30, 0, 40,  0,  0, 0, 0, 0, 0,0, 0, -999
    20070926,0600,,TS,11.2N,41.5W,50, 997, 60, 30, 0, 40, 40,  0, 0, 0, 0, 0,0, 0, -999
    20070926,1200,,HU,11.7N,42.4W,65, 988, 90, 60,40, 45, 60, 40,25,30,40,30,0,15, -999
    20070926,1800,,HU,12.3N,43.3W,65, 990,120, 90,40, 90, 90, 60,25,45,50,40,0, 0, -999
    20070927,0000,,TS,12.8N,44.6W,60, 995,180,150,45,150,150,105,25,90, 0, 0,0, 0, -999
    20070927,0600,,TS,13.2N,45.7W,55, 998,180,150,45,150,150,105,25,75, 0, 0,0, 0, -999
    20070927,1200,,TS,13.5N,46.8W,55,1002,170,140,40,100,120,100,20,40, 0, 0,0, 0, -999
    20070927,1800,,TS,14.1N,47.9W,50,1005,150,140,40, 80, 60, 60,20,30, 0, 0,0, 0, -999
    20070928,0000,,TS,14.1N,48.8W,50,1005,150,140,40, 80, 60, 60,20,30, 0, 0,0, 0, -999
    20070928,0600,,TS,14.3N,49.0W,40,1007,130,120,40, 70,  0,  0, 0, 0, 0, 0,0, 0, -999
    20070928,1200,,TS,14.6N,49.0W,35,1008,130,120,30, 30,  0,  0, 0, 0, 0, 0,0, 0, -999
    20070928,1800,,TS,15.8N,49.4W,35,1008,140,130,30, 30,  0,  0, 0, 0, 0, 0,0, 0, -999
    20070929,0000,,TS,16.1N,51.0W,35,1008,180,180, 0,  0,  0,  0, 0, 0, 0, 0,0, 0, -999
    20070929,0600,,TD,16.3N,52.6W,30,1008,  0,  0, 0,  0,  0,  0, 0, 0, 0, 0,0, 0, -999
    20070929,1200,,LO,16.8N,54.2W,30,1009,  0,  0, 0,  0,  0,  0, 0, 0, 0, 0,0, 0, -999
    """

    def process_header(line: str) -> Storm:
        """Process a HURDAT2 header line."""
        # Remove trailing commas and whitespace before splitting
        line = line.rstrip(",\n\r \t")
        parts = [part.strip() for part in line.split(",")]

        if len(parts) != HEADER_COLUMN_COUNT:
            raise HurdatParseError(
                f"Invalid header format: expected {HEADER_COLUMN_COUNT} "
                f"columns after removing trailing comma, got {len(parts)}\n"
                f"Line: '{line}'"
            )

        cyclone_id, name, obs_count = parts

        if not cyclone_id or len(cyclone_id) != CYCLONE_ID_LENGTH:
            raise HurdatParseError(f"Invalid cyclone ID format: '{cyclone_id}'")

        try:
            basin = cyclone_id[:2]
            number = int(cyclone_id[2:4])
            year = int(cyclone_id[4:])
            obs_count_int = int(obs_count)
        except ValueError as e:
            raise HurdatParseError(f"Failed to parse header values: {e!s}") from e

        return Storm(
            basin=basin,
            number=number,
            year=year,
            name=name.strip() if name else "UNNAMED",
            observation_count=obs_count_int,
        )

    storms: list[Storm] = []
    storm_ids: dict[tuple[str, int, int], Storm] = {}  # Track storms by ID tuple

    with open(filepath) as f:
        current_storm: Storm | None = None

        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            if line[0:2] in ["AL", "EP", "CP"]:  # Header line
                if current_storm:
                    storms.append(current_storm)

                current_storm = process_header(line)
                storm_id = (
                    current_storm.basin,
                    current_storm.number,
                    current_storm.year,
                )

                if debug:
                    logging.debug(
                        f"Processing storm: {storm_id} - {current_storm.name}"
                    )

                if storm_id in storm_ids:
                    logging.error(
                        f"Duplicate storm found during parse: "
                        f"{storm_id} - Current: {current_storm.name}, "
                        f"Previous: {storm_ids[storm_id].name}"
                    )
                storm_ids[storm_id] = current_storm

            elif current_storm is not None:
                # Process observation
                if obs := parse_observation(line, debug):
                    current_storm.observations.append(obs)

        # Don't forget the last storm
        if current_storm:
            storms.append(current_storm)

    if debug:
        logging.debug(f"Parsed {len(storms)} storms")
        logging.debug(f"Unique storm IDs: {len(storm_ids)}")

    return storms

    def process_observations(f: TextIO, storm: Storm, pbar: tqdm) -> None:  # type: ignore
        """Process all observations for a storm."""
        while True:
            pos = f.tell()
            line = f.readline()
            if not line or line.startswith("AL"):
                f.seek(pos)
                break

            if obs := parse_observation(line.strip(), debug):
                storm.observations.append(obs)
            pbar.update(1)

    try:
        with open(filepath) as f:
            total_lines = sum(1 for _ in f)

        with (
            tqdm(total=total_lines, desc="Parsing HURDAT2") as pbar,
            open(filepath) as f,
        ):
            while line := f.readline():
                if line.startswith("AL"):
                    storm = process_header(line)
                    process_observations(f, storm, pbar)
                    storms.append(storm)
                    pbar.update(1)

        if debug:
            logging.debug(f"Total storms parsed: {len(storms)}")
            logging.debug(
                f"Total observations: {sum(s.observation_count for s in storms)}"
            )

            # Validate observation counts
            for storm in storms:
                if len(storm.observations) != storm.observation_count:
                    logging.warning(
                        f"Storm {storm.name} ({storm.year}): "
                        f"expected {storm.observation_count} observations, "
                        f"got {len(storm.observations)}"
                    )

        return storms

    except FileNotFoundError as err:
        raise HurdatParseError(f"HURDAT2 file not found: {filepath}") from err
    except Exception as e:
        raise HurdatParseError(f"Failed to parse HURDAT2 file: {e}") from e


def create_spatialite_connection(db_path: str) -> sqlite3.Connection:
    """Create a connection with Spatialite extension enabled."""
    conn = sqlite3.connect(db_path)
    conn.enable_load_extension(True)
    conn.load_extension("mod_spatialite")

    # Verify spatialite functions are available
    cur = conn.cursor()
    cur.execute("SELECT spatialite_version()")
    version = cur.fetchone()[0]
    logging.debug(f"Spatialite version: {version}")

    return conn


def insert_observations(
    db_path: str, storms: list[Storm], batch_size: int = 1000
) -> None:
    conn = create_spatialite_connection(db_path)
    cur = conn.cursor()

    try:
        cur.execute("DROP TRIGGER IF EXISTS observations_validate;")
        # Allow both -99 and -999 as missing values
        cur.execute(
            """
            CREATE TRIGGER observations_validate
            BEFORE INSERT ON observations
            FOR EACH ROW
            BEGIN
                SELECT CASE
                    WHEN NEW.wind_speed NOT IN (-99, -999)
                    AND (NEW.wind_speed < 0 OR NEW.wind_speed > 200)
                        THEN RAISE(ROLLBACK, 'Invalid wind speed')
                    WHEN NEW.pressure NOT IN (-99, -999)
                    AND (NEW.pressure < 800 OR NEW.pressure > 1100)
                        THEN RAISE(ROLLBACK, 'Invalid pressure')
                END;
            END;
        """
        )

        cur.execute("BEGIN TRANSACTION")

        for storm in tqdm(storms, desc="Processing storms"):
            try:
                # Insert storm record
                cur.execute(
                    """
                    INSERT INTO storms (basin, number, year, name, observation_count)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        storm.basin,
                        storm.number,
                        storm.year,
                        storm.name,
                        storm.observation_count,
                    ),
                )

                storm_id = cur.lastrowid

                # Process observations in batches with debug logging
                for i in range(0, len(storm.observations), batch_size):
                    batch = storm.observations[i : i + batch_size]
                    try:
                        values = [
                            (
                                storm_id,
                                obs.date.isoformat(),
                                obs.time,
                                obs.record_id,
                                obs.status,
                                obs.wind_speed,
                                obs.pressure,
                                obs.location.to_wkt(),
                            )
                            for obs in batch
                        ]
                        if any(
                            obs.wind_speed != MISSING_VALUE
                            and (obs.wind_speed < 0 or obs.wind_speed > 200)  # noqa: PLR2004
                            for obs in batch
                        ):
                            logging.warning(
                                f"Invalid wind speed in batch for storm {storm.name}: "
                                f"{[(obs.date, obs.wind_speed) for obs in batch]}"
                            )

                        cur.executemany(
                            """
                            INSERT INTO observations (
                                storm_id, date, time, record_id, status,
                                wind_speed, pressure, geom
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ST_PointFromText(?, 4326))
                        """,
                            values,
                        )
                    except Exception as e:
                        logging.error(
                            f"Failed to insert batch for storm {storm.name}: {e}"
                        )
                        raise

            except Exception as e:
                logging.error(f"Failed to process storm {storm.name}: {e}")
                raise

        conn.commit()

    except Exception:
        conn.rollback()
        logging.exception("Failed to insert data:")
        raise
    finally:
        conn.close()


def validate_database(db_path: str) -> None:
    """Validate database contents and structure with enhanced checks."""
    conn = create_spatialite_connection(db_path)
    cur = conn.cursor()

    try:
        # Debug raw coordinates
        cur.execute(
            """
            SELECT
                o.id,
                s.basin,
                AsText(o.geom) as wkt,
                X(o.geom) as lon,
                Y(o.geom) as lat
            FROM observations o
            JOIN storms s ON o.storm_id = s.id
            WHERE X(o.geom) < -100 OR X(o.geom) > 20
            LIMIT 5
        """
        )
        debug_coords = cur.fetchall()
        for row in debug_coords:
            logging.debug(f"Suspect coordinates: {row}")

        # Spatial bounds with debug info
        cur.execute(
            """
            WITH raw_coords AS (
                SELECT
                    o.id,
                    s.basin,
                    X(o.geom) as raw_lon,
                    CASE
                        WHEN X(o.geom) > 180 THEN X(o.geom) - 360
                        WHEN X(o.geom) < -180 THEN X(o.geom) + 360
                        ELSE X(o.geom)
                    END as norm_lon,
                    Y(o.geom) as lat
                FROM observations o
                JOIN storms s ON o.storm_id = s.id
                WHERE s.basin = 'AL'
            )
            SELECT
                MIN(raw_lon) as min_raw_lon,
                MAX(raw_lon) as max_raw_lon,
                MIN(norm_lon) as min_norm_lon,
                MAX(norm_lon) as max_norm_lon,
                MIN(lat) as min_lat,
                MAX(lat) as max_lat,
                COUNT(*) as obs_count
            FROM raw_coords
        """
        )
        spatial_stats = cur.fetchone()

        # Structure validation
        cur.execute(
            """
            SELECT type, name, sql
            FROM sqlite_master
            WHERE type IN ('table','index','trigger')
            AND name NOT LIKE 'sqlite_%'
            ORDER BY type, name
        """
        )
        schema = cur.fetchall()

        # Basin and temporal coverage
        cur.execute(
            """
            SELECT basin,
                   COUNT(*) as storm_count,
                   MIN(year) as first_year,
                   MAX(year) as last_year,
                   COUNT(DISTINCT year) as active_years
            FROM storms
            GROUP BY basin
            ORDER BY storm_count DESC
        """
        )
        basin_stats = cur.fetchall()

        # Storm intensity patterns with proper pressure handling
        cur.execute(
            """
            SELECT
                CASE
                    WHEN wind_speed <= 33 THEN 'TD'
                    WHEN wind_speed <= 63 THEN 'TS'
                    ELSE 'HU'
                END as category,
                COUNT(*) as count,
                MIN(CASE WHEN pressure NOT IN (-99, -999)
                    THEN pressure END) as min_pressure,
                AVG(CASE WHEN pressure NOT IN (-99, -999)
                    THEN pressure END) as avg_pressure
            FROM observations
            WHERE wind_speed NOT IN (-99, -999)
            GROUP BY category
            ORDER BY count DESC
        """
        )
        intensity_stats = cur.fetchall()

        # Spatial coverage with corrected longitude normalization
        cur.execute(
            """
            WITH raw_bounds AS (
                SELECT
                    X(geom) as lon,
                    Y(geom) as lat
                FROM observations
                WHERE storm_id IN (
                    SELECT id
                    FROM storms
                    WHERE basin = 'AL'
                )
            ),
            normalized_bounds AS (
                SELECT
                    CASE
                        WHEN lon > 180 THEN lon - 360
                        WHEN lon < -180 THEN lon + 360
                        ELSE lon
                    END as norm_lon,
                    lat
                FROM raw_bounds
            )
            SELECT
                MIN(norm_lon) as min_lon,
                MAX(norm_lon) as max_lon,
                MIN(lat) as min_lat,
                MAX(lat) as max_lat,
                (SELECT COUNT(*) FROM observations) as obs_count,
                (SELECT COUNT(DISTINCT storm_id) FROM observations) as storm_count
            FROM normalized_bounds
        """
        )
        spatial_stats = cur.fetchone()

        # Output validation results
        logging.info("\nDatabase Validation Report")
        logging.info("=======================")

        logging.info("\nSchema Overview:")
        for type_, name, _ in schema:
            logging.info(f"  {type_}: {name}")

        logging.info("\nBasin Coverage:")
        for basin, count, start, end, years in basin_stats:
            logging.info(
                f"  {basin}: {count} storms over {years} years ({start}-{end})"
            )

        logging.info("\nIntensity Distribution:")
        for cat, count, min_p, avg_p in intensity_stats:
            logging.info(
                f"  {cat}: {count} obs (min pressure: {min_p}, avg: {avg_p:.1f})"
            )

        logging.info("\nSpatial Statistics:")
        min_lon = spatial_stats[0]
        max_lon = spatial_stats[1]
        min_lat = spatial_stats[2]
        max_lat = spatial_stats[3]

        logging.info(
            f"  Bounds: {abs(min_lon):.1f}°{'W' if min_lon < 0 else 'E'} to "
            f"{abs(max_lon):.1f}°{'W' if max_lon < 0 else 'E'}, "
            f"{min_lat:.1f}°{'S' if min_lat < 0 else 'N'} to "
            f"{max_lat:.1f}°N"
        )
        logging.info(
            f"  Coverage: {spatial_stats[4]} observations "
            f"across {spatial_stats[5]} storms"
        )

    except Exception as e:
        logging.error(f"Validation failed: {e}")
        raise
    finally:
        conn.close()


def main() -> None:
    """ETL entry point."""
    import argparse
    import logging

    parser = argparse.ArgumentParser(description="HURDAT2 ETL Script")
    parser.add_argument("input_file", help="HURDAT2 format input file")
    parser.add_argument("db_file", help="Output SQLite database file")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    try:
        logging.debug(f"Processing input file: {args.input_file}")
        logging.debug(f"Output database: {args.db_file}")

        init_spatialite_db(args.db_file)
        storms = parse_hurdat2(args.input_file, args.debug)
        insert_observations(args.db_file, storms)
        validate_database(args.db_file)

        logging.info("ETL process completed successfully")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
