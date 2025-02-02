import logging
import sys
import warnings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import ClassVar, Final, TypeVar

from pydantic import BaseModel, ConfigDict, field_validator
from tqdm import tqdm

# Constants
HURDAT_COLUMN_COUNT: Final = 21


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
        """Validate and normalize coordinates."""
        if not self.LAT_RANGE[0] <= self.latitude <= self.LAT_RANGE[1]:
            raise ValueError(f"Latitude {self.latitude} out of range {self.LAT_RANGE}")

        # Normalize longitude to [-180, 180]
        self.longitude = ((self.longitude + 180) % 360) - 180

    @classmethod
    def from_str(cls: type[T], lat_str: str, lon_str: str) -> T:
        """Create Point from HURDAT2 coordinate strings."""

        def parse_coord(coord: str) -> float:
            value = float(coord[:-1])
            return -value if coord[-1] in ["S", "W"] else value

        return cls(
            latitude=parse_coord(lat_str.strip()),
            longitude=parse_coord(lon_str.strip()),
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


try:
    import pysqlite3 as sqlite3
except ImportError:
    import sqlite3


def init_spatialite_db(db_path: str) -> None:
    """Initialize spatialite database with extension support."""
    spatialite_path = "/usr/lib/x86_64-linux-gnu/mod_spatialite.so"

    if Path(db_path).exists():
        logging.warning(f"Database {db_path} already exists, will be overwritten")
        Path(db_path).unlink()

    try:
        conn = sqlite3.connect(db_path)
        conn.enable_load_extension(True)
        conn.load_extension(spatialite_path)

        with conn:
            conn.execute("SELECT InitSpatialMetadata(1)")

            # Create storms table
            conn.execute(
                """
            CREATE TABLE storms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                basin TEXT NOT NULL,
                number INTEGER NOT NULL,
                year INTEGER NOT NULL,
                name TEXT NOT NULL,
                observation_count INTEGER NOT NULL,
                UNIQUE(basin, number, year)
            )"""
            )

            # Create observations table with all fields
            conn.execute(
                """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                storm_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                time INTEGER NOT NULL,
                status TEXT NOT NULL,
                wind_speed INTEGER NOT NULL,
                pressure INTEGER NOT NULL,
                wind_radii_34kt_ne INTEGER,
                wind_radii_34kt_se INTEGER,
                wind_radii_34kt_sw INTEGER,
                wind_radii_34kt_nw INTEGER,
                wind_radii_50kt_ne INTEGER,
                wind_radii_50kt_se INTEGER,
                wind_radii_50kt_sw INTEGER,
                wind_radii_50kt_nw INTEGER,
                wind_radii_64kt_ne INTEGER,
                wind_radii_64kt_se INTEGER,
                wind_radii_64kt_sw INTEGER,
                wind_radii_64kt_nw INTEGER,
                radius_max_wind INTEGER,
                timestamp DATETIME NOT NULL,
                FOREIGN KEY(storm_id) REFERENCES storms(id)
            )"""
            )

            # Add geometry column
            conn.execute(
                """
            SELECT AddGeometryColumn('observations',
                'location',
                4326,
                'POINT',
                'XY',
                0
            )"""
            )

            # Create indexes
            conn.execute("CREATE INDEX idx_observations_date ON observations(date)")
            conn.execute(
                "CREATE INDEX idx_observations_timestamp ON observations(timestamp)"
            )
            conn.execute("SELECT CreateSpatialIndex('observations', 'location')")

            logging.info(f"Initialized spatial database: {db_path}")

    except sqlite3.Error as e:
        raise HurdatParseError(f"Failed to initialize database: {e}") from e
    finally:
        conn.close()


def parse_observation(line: str, debug: bool = False) -> Observation | None:
    """Parse HURDAT2 observation line."""
    try:
        parts = line.split(",")
        if len(parts) < HURDAT_COLUMN_COUNT:
            if debug:
                logging.debug(f"Invalid line format: {line}")
            return None

        def parse_wind_radii(parts: list[str], start_idx: int) -> dict[str, int]:
            return {
                "NE": (
                    int(parts[start_idx]) if parts[start_idx].strip() != "-999" else 0
                ),
                "SE": (
                    int(parts[start_idx + 1])
                    if parts[start_idx + 1].strip() != "-999"
                    else 0
                ),
                "SW": (
                    int(parts[start_idx + 2])
                    if parts[start_idx + 2].strip() != "-999"
                    else 0
                ),
                "NW": (
                    int(parts[start_idx + 3])
                    if parts[start_idx + 3].strip() != "-999"
                    else 0
                ),
            }

        obs = Observation(
            date=Observation.validate_date(parts[0].strip()),
            time=int(parts[1]),
            record_id=parts[2].strip() or None,
            status=parts[3].strip(),
            location=Point.from_str(parts[4].strip(), parts[5].strip()),
            wind_speed=int(parts[6]),
            pressure=int(parts[7]) if parts[7].strip() != "-999" else 0,
            wind_radii_34kt=parse_wind_radii(parts, 8),
            wind_radii_50kt=parse_wind_radii(parts, 12),
            wind_radii_64kt=parse_wind_radii(parts, 16),
            radius_max_wind=int(parts[20]) if parts[20].strip() != "-999" else None,
        )
        if debug:
            logging.debug(f"Parsed observation: {obs}")
        return obs

    except Exception as e:
        if debug:
            msg = f"Failed to parse observation\nData: {line}\nError: {e!s}"
            warnings.warn(msg, UserWarning, stacklevel=2)
        return None


def parse_hurdat2(filepath: Path | str, debug: bool = False) -> list[Storm]:
    """Parse HURDAT2 format file into Storm objects."""
    storms: list[Storm] = []
    current_storm = None
    line_number = 0

    def parse_header(line: str) -> Storm:
        """Parse HURDAT2 header line into Storm object."""
        parts = line.strip().split(",")
        cyclone_id = parts[0].strip()
        basin = cyclone_id[:2]
        number = int(cyclone_id[2:4])
        year = int(cyclone_id[4:])
        name = parts[1].strip()
        observation_count = int(parts[2])

        return Storm(
            basin=basin,
            number=number,
            year=year,
            name=name,
            observation_count=observation_count,
        )

    try:
        with open(filepath) as f:
            total_lines = sum(1 for _ in f)

        with (
            open(filepath) as f,
            tqdm(total=total_lines, desc="Parsing HURDAT2") as pbar,
        ):
            for line in f:
                line_number += 1
                parsed_line = line.strip()
                pbar.update(1)

                if parsed_line.startswith("AL"):
                    if current_storm:
                        storms.append(current_storm)
                    current_storm = parse_header(parsed_line)

                elif parsed_line and current_storm:
                    observation = parse_observation(parsed_line, debug)
                    if observation:
                        current_storm.observations.append(observation)

            if current_storm:
                storms.append(current_storm)

        if debug:
            logging.debug(f"Total storms parsed: {len(storms)}")
            logging.debug(
                f"Total observations: {sum(s.observation_count for s in storms)}"
            )

        return storms

    except FileNotFoundError as e:
        raise HurdatParseError(f"HURDAT2 file not found: {filepath}") from e


def insert_observations(
    db_path: str, storms: list[Storm], batch_size: int = 1000
) -> None:
    """Insert storm observations into spatialite database."""
    total_storms = len(storms)
    total_observations = sum(s.observation_count for s in storms)
    logging.info(
        f"Processing {total_storms} storms with {total_observations} observations"
    )

    storm_sql = """
    INSERT INTO storms (basin, number, year, name, observation_count)
    VALUES (?, ?, ?, ?, ?)
    """

    obs_sql = """
    INSERT INTO observations (
        storm_id, date, time, status, wind_speed, pressure,
        wind_radii_34kt_ne, wind_radii_34kt_se, wind_radii_34kt_sw, wind_radii_34kt_nw,
        wind_radii_50kt_ne, wind_radii_50kt_se, wind_radii_50kt_sw, wind_radii_50kt_nw,
        wind_radii_64kt_ne, wind_radii_64kt_se, wind_radii_64kt_sw, wind_radii_64kt_nw,
        radius_max_wind, timestamp, location
    )
    VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        GeomFromText(?, 4326)
    )
    """

    try:
        conn = sqlite3.connect(db_path)
        conn.enable_load_extension(True)
        conn.load_extension("/usr/lib/x86_64-linux-gnu/mod_spatialite.so")

        with (
            conn,
            tqdm(total=total_storms, desc="Processing storms") as storm_bar,
            tqdm(total=total_observations, desc="Processing observations") as obs_bar,
        ):
            batch = []

            for storm in storms:
                cursor = conn.execute(
                    storm_sql,
                    (
                        storm.basin,
                        storm.number,
                        storm.year,
                        storm.name,
                        storm.observation_count,
                    ),
                )
                storm_id = cursor.lastrowid
                storm_bar.update(1)

                for obs in storm.observations:
                    batch.append(
                        (
                            storm_id,
                            obs.date.strftime("%Y-%m-%d"),
                            obs.time,
                            obs.status,
                            obs.wind_speed,
                            obs.pressure,
                            obs.wind_radii_34kt["NE"],
                            obs.wind_radii_34kt["SE"],
                            obs.wind_radii_34kt["SW"],
                            obs.wind_radii_34kt["NW"],
                            obs.wind_radii_50kt["NE"],
                            obs.wind_radii_50kt["SE"],
                            obs.wind_radii_50kt["SW"],
                            obs.wind_radii_50kt["NW"],
                            obs.wind_radii_64kt["NE"],
                            obs.wind_radii_64kt["SE"],
                            obs.wind_radii_64kt["SW"],
                            obs.wind_radii_64kt["NW"],
                            obs.radius_max_wind,
                            f"{obs.date.strftime('%Y-%m-%d')} {obs.time:04d}",
                            obs.location.to_wkt(),
                        )
                    )

                    if len(batch) >= batch_size:
                        conn.executemany(obs_sql, batch)
                        obs_bar.update(len(batch))
                        batch.clear()
                        conn.commit()

            if batch:
                conn.executemany(obs_sql, batch)
                obs_bar.update(len(batch))
                conn.commit()

    except sqlite3.Error as e:
        raise HurdatParseError(f"Failed to insert data: {e}") from e
    finally:
        conn.close()


def main() -> None:
    """ETL entry point."""
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser(description="HURDAT2 ETL Script")
    parser.add_argument("input_file", help="HURDAT2 format input file")
    parser.add_argument("db_file", help="Output SQLite database file")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")

    args = parser.parse_args()

    try:
        init_spatialite_db(args.db_file)
        storms = parse_hurdat2(args.input_file, args.debug)
        insert_observations(args.db_file, storms)
        logging.info("ETL process completed successfully")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
