"""Data models for HURDAT2 ETL pipeline."""

import re
from datetime import datetime
from enum import Enum
from typing import Annotated, ClassVar

from pydantic import AfterValidator, BaseModel, Field, computed_field, field_validator

# Locally defined settings constants
MAX_LATITUDE = 90  # Maximum latitude in degrees
MAX_LONGITUDE = 360  # Maximum longitude in degrees (HURDAT2 uses 0-360°)
MISSING_VALUES = {-99, -999}  # Values indicating missing data in HURDAT2


# Define StormStatus enum locally
class StormStatus(str, Enum):
    """
    Hurricane status indicators from HURDAT2 format specification.
    String enum ensures values match exactly with source data.
    """

    TROPICAL_STORM = "TS"  # Tropical Storm
    HURRICANE = "HU"  # Hurricane
    EXTRATROPICAL = "EX"  # Extratropical cyclone
    LOW = "LO"  # Low pressure system
    WAVE = "WV"  # Tropical Wave
    DISTURBANCE = "DB"  # Disturbance
    SUBTROPICAL_STORM = "SS"  # Subtropical Storm
    SUBTROPICAL_DEPRESSION = "SD"  # Subtropical Depression
    TROPICAL_DEPRESSION = "TD"  # Tropical Depression
    TROPICAL_WAVE = "WV"  # Duplicate of WAVE - per HURDAT2 spec
    UNKNOWN = "XX"  # Unknown/Missing status


class Point(BaseModel):
    """Geographic point with latitude and longitude in WGS84 decimal degrees."""

    COORDINATE_PATTERN: ClassVar[re.Pattern[str]] = re.compile(
        r"(-?\d+\.?\d*)\s*([NSEW])$"
    )

    latitude: float = Field(description="Latitude in decimal degrees")
    longitude: float = Field(description="Longitude in decimal degrees")

    @field_validator("latitude", mode="before")
    @classmethod
    def validate_latitude(cls, value: str) -> float:
        """Convert HURDAT2 latitude to decimal degrees."""
        if isinstance(value, str):
            return cls.convert_hurdat2_coordinates(value, is_latitude=True)
        return value

    @field_validator("longitude", mode="before")
    @classmethod
    def validate_longitude(cls, value: str) -> float:
        """Convert HURDAT2 longitude to decimal degrees."""
        if isinstance(value, str):
            return cls.convert_hurdat2_coordinates(value, is_latitude=False)
        return value

    @classmethod
    def convert_hurdat2_coordinates(cls, coord: str, is_latitude: bool) -> float:
        """Convert HURDAT2 coordinate string to decimal degrees (WGS84 format)."""
        match = cls.COORDINATE_PATTERN.match(coord.strip().upper())
        if not match:
            raise ValueError(f"Invalid HURDAT2 format: {coord}")

        degrees = float(match.group(1))
        direction = match.group(2)

        if is_latitude:
            if direction not in "NS":
                raise ValueError(f"Latitude must use N/S direction, got: {direction}")
            if not 0 <= degrees <= MAX_LATITUDE:
                raise ValueError(f"Latitude {degrees} out of range [0, {MAX_LATITUDE}]")
            return -degrees if direction == "S" else degrees

        if direction not in "EW":
            raise ValueError(f"Longitude must use E/W direction, got: {direction}")
        if not 0 <= degrees <= MAX_LONGITUDE:
            raise ValueError(f"Longitude {degrees} out of range [0, {MAX_LONGITUDE}]")
        return (
            degrees - 360
            if direction == "W" and degrees > 180
            else -degrees
            if direction == "W"
            else degrees
        )

    def to_wkt(self) -> str:
        """Convert to Well-Known Text format."""
        return f"POINT({self.longitude} {self.latitude})"


# Custom type for non-negative integers
def check_non_negative(v: int) -> int:
    if v < 0:
        raise ValueError("Value must be non-negative")
    return v


NonNegativeInt = Annotated[int, AfterValidator(check_non_negative)]


def validate_iso_datetime(v: datetime) -> datetime:
    """Validate that a datetime can be represented in ISO format."""
    # Just check if it can be formatted as ISO
    v.isoformat()

    # Don't modify the datetime object, just return it as is
    return v


IsoDateTime = Annotated[datetime, AfterValidator(validate_iso_datetime)]


class Observation(BaseModel):
    """Single hurricane observation record."""

    storm_id: str = Field(..., description="Unique identifier of the parent storm")
    date: IsoDateTime
    record_identifier: str | None = None
    status: StormStatus
    location: Point
    max_wind: NonNegativeInt | None = None
    min_pressure: NonNegativeInt | None = None
    ne34: NonNegativeInt | None = None
    se34: NonNegativeInt | None = None
    sw34: NonNegativeInt | None = None
    nw34: NonNegativeInt | None = None
    ne50: NonNegativeInt | None = None
    se50: NonNegativeInt | None = None
    sw50: NonNegativeInt | None = None
    nw50: NonNegativeInt | None = None
    ne64: NonNegativeInt | None = None
    se64: NonNegativeInt | None = None
    sw64: NonNegativeInt | None = None
    nw64: NonNegativeInt | None = None
    max_wind_radius: NonNegativeInt | None = None

    @computed_field
    def max_wind_mph(self) -> float | None:
        """Wind speed in MPH (converted from knots) rounded to 1 decimal place."""
        if self.max_wind is None:
            return None
        return round(self.max_wind * 1.15078, 1)

    @field_validator("status", mode="before")
    @classmethod
    def parse_storm_status(cls, value: str) -> StormStatus:
        """Parse storm status from string."""
        try:
            return StormStatus(value)
        except ValueError as e:
            raise ValueError(
                f"Invalid storm status: {value}, expected one of {list(StormStatus)}"
            ) from e

    @field_validator(
        "max_wind",
        "min_pressure",
        "ne34",
        "se34",
        "sw34",
        "nw34",
        "ne50",
        "se50",
        "sw50",
        "nw50",
        "ne64",
        "se64",
        "sw64",
        "nw64",
        "max_wind_radius",
        mode="before",
    )
    @classmethod
    def parse_possible_missing(cls, value: str | int) -> int | None:
        """Parse integer fields that may be denoted with -99 or -999."""
        if isinstance(value, str):
            value = int(value.strip())
        if value in MISSING_VALUES:
            return None
        return value


class Storm(BaseModel):
    """Hurricane/storm record with observations."""

    basin: str = Field(..., description="Basin code (e.g., 'AL' for Atlantic)")
    cyclone_number: int = Field(..., ge=0, le=99, description="ATCF cyclone number")
    year: int = Field(..., ge=1800, le=2100, description="Year of storm")
    name: str = Field(..., min_length=1, description="Storm name or 'UNNAMED'")
    observations: list[Observation] = Field(
        ..., description="List of observation records"
    )

    @field_validator("basin")
    @classmethod
    def validate_basin(cls, value: str) -> str:
        """Validate basin code."""
        if value != "AL":
            raise ValueError(f"Invalid basin: {value}, expected 'AL'")
        return value

    @property
    def storm_id(self) -> str:
        """Generate storm ID from components."""
        return f"{self.basin}{self.cyclone_number:02d}{self.year}"

    @property
    def observation_count(self) -> int:
        """Get the number of observations."""
        return len(self.observations)
