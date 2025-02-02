import sqlite3
import warnings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import ClassVar, Final, TypeVar

from pydantic import BaseModel, ConfigDict, field_validator

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


def init_spatialite_db(db_path: str) -> None:
    """Initialize spatialite database."""
    conn = sqlite3.connect(db_path)
    conn.enable_load_extension(True)
    conn.execute("SELECT load_extension('mod_spatialite')")
    conn.execute("SELECT InitSpatialMetadata(1)")

    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS observations (
        id INTEGER PRIMARY KEY,
        date TEXT,
        time INTEGER,
        status TEXT,
        wind_speed INTEGER,
        pressure INTEGER,
        location GEOMETRY
    )"""
    )

    conn.execute("SELECT CreateSpatialIndex('observations', 'location')")
    conn.commit()
    conn.close()


def parse_hurdat2(filepath: Path | str, debug: bool = False) -> list[Storm]:
    """...existing docstring..."""
    storms: list[Storm] = []
    current_storm = None
    line_number = 0

    try:
        with open(filepath) as f:
            for current_line in f:
                line_number += 1
                parsed_line = current_line.strip()

                if parsed_line.startswith("AL"):
                    try:
                        if current_storm:
                            storms.append(current_storm)
                    except (IndexError, ValueError) as e:
                        msg = (
                            f"Failed to parse storm header: {parsed_line}. "
                            f"Error: {e!s}"
                        )
                        warnings.warn(msg, UserWarning, stacklevel=2)
                elif parsed_line and current_storm:
                    parts = parsed_line.split(",")
                    if len(parts) < HURDAT_COLUMN_COUNT:
                        msg = (
                            f"Invalid columns in data line: {len(parts)}. "
                            f"Line: {parsed_line}"
                        )
                        warnings.warn(msg, UserWarning, stacklevel=2)
                        continue

                    try:
                        obs = Observation(
                            date=datetime.strptime(parts[0].strip(), "%Y%m%d"),
                            time=int(parts[1]),
                            record_id=parts[2],
                            status=parts[3],
                            location=Point.from_str(parts[4], parts[5]),
                            wind_speed=int(parts[6]),
                            pressure=int(parts[7]),
                            wind_radii_34kt={},
                            wind_radii_50kt={},
                            wind_radii_64kt={},
                            radius_max_wind=None,
                        )
                        current_storm.observations.append(obs)
                    except Exception as e:
                        msg = f"Line {line_number}: Failed to parse observation"
                        if debug:
                            msg += f"\nData: {parsed_line}\nError: {e!s}"
                            warnings.warn(msg, UserWarning, stacklevel=2)
                        continue

    except FileNotFoundError as e:
        raise HurdatParseError(f"HURDAT2 file not found: {filepath}") from e

    return storms


storms = parse_hurdat2(
    "/home/snarkipus/Projects/Tauri/reference/ref/hurdat2-1851-2023-051124.txt",
    debug=True,
)

if storms and storms[0].observations:
    print(storms[0].observations[0].model_dump())
else:
    print("No storm data available or no observations for the first storm")
