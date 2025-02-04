"""Data models for HURDAT2 ETL pipeline."""

import re
from datetime import datetime
from typing import ClassVar, Final

from pydantic import BaseModel, Field, field_validator

from hurdat2_etl.config import settings
from hurdat2_etl.extract.types import StormStatus


class Point(BaseModel):
    """Geographic point with latitude and longitude in WGS84 decimal degrees"""

    COORDINATE_PATTERN: ClassVar[re.Pattern[str]] = re.compile(
        r"(\d+\.?\d*)\s*([NSEW])$"
    )

    latitude: float = Field(description="Latitude in decimal degrees")
    longitude: float = Field(description="Longitude in decimal degrees")

    @field_validator("latitude", mode="before")
    @classmethod
    def validate_latitude(cls, value: str) -> float:
        """Convert HURDAT2 latitude to decimal degrees."""
        if isinstance(value, str):
            return cls.parse_hurdat2(value, is_latitude=True)
        return value

    @field_validator("longitude", mode="before")
    @classmethod
    def validate_longitude(cls, value: str) -> float:
        """Convert HURDAT2 longitude to decimal degrees."""
        if isinstance(value, str):
            return cls.parse_hurdat2(value, is_latitude=False)
        return value

    @classmethod
    def parse_hurdat2(cls, coord: str, is_latitude: bool) -> float:
        """Convert HURDAT2 coordinate string to decimal degrees."""
        match = cls.COORDINATE_PATTERN.match(coord.strip().upper())
        if not match:
            raise ValueError(f"Invalid HURDAT2 format: {coord}")

        degrees = float(match.group(1))
        direction = match.group(2)

        if is_latitude:
            if direction not in "NS":
                raise ValueError(f"Latitude must use N/S direction, got: {direction}")
            if not (
                -settings.Settings.MAX_LATITUDE
                <= degrees
                <= settings.Settings.MAX_LATITUDE
            ):
                raise ValueError(f"Latitude {degrees} out of range [-90, 90]")
            return -degrees if direction == "S" else degrees
        else:
            if direction not in "EW":
                raise ValueError(f"Longitude must use E/W direction, got: {direction}")
            if not (
                (degrees >= -settings.Settings.MAX_LONGITUDE)
                and (degrees <= settings.Settings.MAX_LONGITUDE)
            ):
                raise ValueError(f"Longitude {degrees} out of range [-180, 180]")
            return -degrees if direction == "W" else degrees

    def to_wkt(self) -> str:
        """Convert to Well-Known Text format."""
        return f"POINT({self.longitude} {self.latitude})"


class Observation(BaseModel):
    """Single hurricane observation record"""

    date: datetime
    record_identifier: str | None
    status: StormStatus
    location: Point
    max_wind: int | None = Field(..., ge=0)
    min_pressure: int | None = Field(..., ge=0)
    ne34: int | None = Field(None, ge=0)
    se34: int | None = Field(None, ge=0)
    sw34: int | None = Field(None, ge=0)
    nw34: int | None = Field(None, ge=0)
    ne50: int | None = Field(None, ge=0)
    se50: int | None = Field(None, ge=0)
    sw50: int | None = Field(None, ge=0)
    nw50: int | None = Field(None, ge=0)
    ne64: int | None = Field(None, ge=0)
    se64: int | None = Field(None, ge=0)
    sw64: int | None = Field(None, ge=0)
    nw64: int | None = Field(None, ge=0)
    max_wind_radius: int | None = Field(None, ge=0)

    @classmethod
    def parse_storm_status(cls, value: str) -> StormStatus:
        """Parse storm status from string."""
        try:
            return StormStatus(value)
        except ValueError as e:
            raise ValueError(
                f"Invalid storm status: {value}, expected one of {list(StormStatus)}"
            ) from e

    @classmethod
    def parse_possible_missing(cls, value: str | int) -> int | None:
        """Parse integer fields that may be denoted with -99 or -999."""
        if isinstance(value, str):
            value = int(value.strip())

        if value in settings.Settings.MISSING_VALUES:
            return None

        return value


MAX_CYCLONES: Final = 99
MIN_YEAR: Final = 1800
MAX_YEAR: Final = 2100


class Storm(BaseModel):
    """Hurricane/storm record with observations"""

    basin: str
    cyclone_number: int = Field(ge=0, le=99)  # Add range validation
    year: int = Field(ge=1800, le=2100)  # Add reasonable year range
    name: str = Field(min_length=1)
    observations: list[Observation]

    @field_validator("cyclone_number")
    @classmethod
    def validate_cyclone_number(cls, value: int) -> int:
        if not 0 <= value <= MAX_CYCLONES:
            raise ValueError(f"Cyclone number must be between 0-99, got {value}")
        return value

    @field_validator("year")
    @classmethod
    def validate_year(cls, value: int) -> int:
        if not MIN_YEAR <= value <= MAX_YEAR:
            raise ValueError(f"Year must be between 1800-2100, got {value}")
        return value

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
        return f"{self.basin}{self.cyclone_number}{self.year}"

    @property
    def observation_count(self) -> int:
        """Get the number of observations."""
        return len(self.observations)
