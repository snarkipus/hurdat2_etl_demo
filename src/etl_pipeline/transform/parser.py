"""
Parser functions for the Transform stage of the ETL pipeline.

This module contains functions responsible for parsing raw HURDAT2 data lines,
performing data type conversions (datetime, coordinates via Pydantic models,
handling missing values), and preparing data structures for Pydantic validation.
"""

import re
from datetime import UTC, datetime
from typing import Any

# Import Pydantic models and constants
from .models import MISSING_VALUES, StormStatus

# Regex for header validation (Basin + Cyclone Number + Year)
# Example: AL011851
BASIN_PATTERN: re.Pattern[str] = re.compile(r"^[A-Z]{2}\d{6}$")


def is_header_line(row: list[str]) -> bool:
    """
    Validates if a raw data row represents a HURDAT2 storm header.

    Checks for the expected number of fields (3 after cleaning) and validates
    the format of the storm identifier (e.g., 'AL011851') and the entry count.

    Args:
        row: A list of strings representing a row from the raw data.

    Returns:
        True if the row is a valid header, False otherwise.
    """
    # Remove empty strings often present at the end of CSV rows
    cleaned_row = [field.strip() for field in row if field.strip()]
    if len(cleaned_row) != 3:
        return False  # Expecting ID, Name, Count

    basin_id, _, entries_str = cleaned_row

    # Validate basin/ID format (e.g., AL011851)
    if not BASIN_PATTERN.match(basin_id):
        return False

    # Validate entry count is an integer
    try:
        int(entries_str)
    except ValueError:
        return False

    return True


def parse_cyclone_id(cyclone_id: str) -> tuple[str, int, int] | None:
    """
    Parses a HURDAT2 cyclone identifier string into its components.

    Args:
        cyclone_id: The cyclone ID string (e.g., "AL011851").

    Returns:
        A tuple containing (basin, cyclone_number, year) if valid,
        otherwise None.
    """
    cyclone_id = cyclone_id.strip()
    if not BASIN_PATTERN.match(cyclone_id):
        return None

    try:
        basin = cyclone_id[:2]
        cyclone_number = int(cyclone_id[2:4])
        year = int(cyclone_id[4:])

        # Add basic range checks for validity
        if not (1800 <= year <= 2100 and 0 <= cyclone_number <= 99):
            return None
        return basin, cyclone_number, year
    except (ValueError, IndexError):
        # Error during slicing or int conversion
        return None


def parse_header_line(line_data: list[str]) -> tuple[str, int, int, str] | None:
    """
    Parses a raw storm header line into key storm identification components.

    Args:
        line_data: A list of strings representing the raw header row.
                   Example: ['AL011851', '           UNNAMED', '     14', '']

    Returns:
        A tuple containing (basin, cyclone_number, year, name) if the header
        is valid, otherwise None.
    """
    cleaned_row = [field.strip() for field in line_data if field.strip()]
    if len(cleaned_row) != 3:
        return None  # Expecting ID, Name, Count

    storm_id_str = cleaned_row[0]
    name = cleaned_row[1]
    # Entry count (cleaned_row[2]) is validated by is_header_line but not returned

    parsed_id = parse_cyclone_id(storm_id_str)
    if not parsed_id:
        return None  # Invalid storm ID format

    basin, cyclone_number, year = parsed_id

    # Ensure name is not empty (should be 'UNNAMED' if not assigned)
    if not name:
        return None

    return basin, cyclone_number, year, name


def parse_datetime(date_str: str, time_str: str) -> datetime | None:
    """
    Parses date (YYYYMMDD) and time (HHMM) strings into a timezone-aware UTC
    datetime object.

     Handles missing or invalid time strings by defaulting to '0000'.

    Args:
        date_str: The date string (e.g., "18510625").
        time_str: The time string (e.g., "0000").

    Returns:
        A timezone-aware datetime object (UTC) if parsing is successful,
        otherwise None.
    """
    date_str = date_str.strip()
    time_str = time_str.strip()

    if not date_str or len(date_str) != 8:
        return None  # Date is essential

    # Default time to midnight if missing or invalid format
    if not time_str or len(time_str) != 4 or not time_str.isdigit():
        time_str = "0000"

    try:
        datetime_str = f"{date_str}{time_str}"
        # Parse as naive datetime first
        dt_naive = datetime.strptime(datetime_str, "%Y%m%d%H%M")
        # Make timezone-aware (UTC) using standard library
        dt_utc = dt_naive.replace(tzinfo=UTC)
        return dt_utc
    except ValueError:
        # Invalid date/time components after defaulting time
        return None


def parse_int_or_none(value_str: str) -> int | None:
    """
    Parses a string into an integer, handling specific missing value markers.

    Converts known missing value indicators (e.g., -99, -999) defined in
    `models.MISSING_VALUES` to None. Also returns None for empty or non-integer strings.

    Args:
        value_str: The string representation of the integer.

    Returns:
        The parsed integer, or None if it's a missing value marker, empty,
        or cannot be parsed as an integer.
    """
    value_str = value_str.strip()
    if not value_str:
        return None
    try:
        value = int(value_str)
        # Check against the set of defined missing value markers
        if value in MISSING_VALUES:
            return None
        # Pydantic model will handle non-negative validation if needed
        return value
    except ValueError:
        # String could not be converted to an integer
        return None


def parse_track_line(line_data: list[str]) -> dict[str, Any] | None:
    """
    Parses a raw storm track point line into a dictionary suitable for
    Observation model instantiation.

     Extracts fields according to HURDAT2 format, performs necessary type conversions
    (datetime, integers with missing values), and prepares a dictionary where keys
    match the Observation model's field names. Coordinate parsing is deferred to
    the Point model within Observation validation.

    Args:
        line_data: A list of strings representing the raw track point row.
                   Example: ['18510625', ' 0000', ' ', ' HU', ' 28.0N',
                             ' 94.8W', '  80', ' -999', ...]

     Returns:
        A dictionary containing parsed data ready for Observation(**parsed_data),
        or None if essential fields (datetime, status, coordinates) cannot be parsed.
    """
    # Ensure minimum expected number of fields
    if len(line_data) < 21:
        return None

    # Strip whitespace from all fields for consistency
    row = [field.strip() for field in line_data]

    # Extract fields based on HURDAT2 format documentation
    date_str = row[0]
    time_str = row[1]
    record_identifier = row[2] if row[2] else None  # Can be empty
    status_str = row[3]
    lat_str = row[4]  # Pass raw string to Point model
    lon_str = row[5]  # Pass raw string to Point model
    max_wind_kts_str = row[6]
    min_pressure_mb_str = row[7]
    # Wind radii fields (34kt, 50kt, 64kt for NE, SE, SW, NW quadrants)
    ne34_str = row[8]
    se34_str = row[9]
    sw34_str = row[10]
    nw34_str = row[11]
    ne50_str = row[12]
    se50_str = row[13]
    sw50_str = row[14]
    nw50_str = row[15]
    ne64_str = row[16]
    se64_str = row[17]
    sw64_str = row[18]
    nw64_str = row[19]
    # Max wind radius (added in later format versions, might be missing/invalid)
    max_wind_radius_str = row[20] if len(row) > 20 else ""  # Handle potential missing

    # --- Parse Core Fields ---
    dt = parse_datetime(date_str, time_str)
    if dt is None:
        return None  # Datetime is essential

    # Validate status string against StormStatus enum
    try:
        status = StormStatus(status_str)
    except ValueError:
        return None  # Status is essential

    # --- Parse Optional Numeric Fields ---
    max_wind_kts = parse_int_or_none(max_wind_kts_str)
    min_pressure_mb = parse_int_or_none(min_pressure_mb_str)
    ne34 = parse_int_or_none(ne34_str)
    se34 = parse_int_or_none(se34_str)
    sw34 = parse_int_or_none(sw34_str)
    nw34 = parse_int_or_none(nw34_str)
    ne50 = parse_int_or_none(ne50_str)
    se50 = parse_int_or_none(se50_str)
    sw50 = parse_int_or_none(sw50_str)
    nw50 = parse_int_or_none(nw50_str)
    ne64 = parse_int_or_none(ne64_str)
    se64 = parse_int_or_none(se64_str)
    sw64 = parse_int_or_none(sw64_str)
    nw64 = parse_int_or_none(nw64_str)
    max_wind_radius = parse_int_or_none(max_wind_radius_str)

    # Return dictionary mapping to Observation model fields
    # Pydantic will handle Point creation/validation from lat_str/lon_str
    # and final validation of all fields (e.g., NonNegativeInt constraints)
    return {
        "date": dt,
        "record_identifier": record_identifier,
        "status": status,
        # Defer validation to Point model via Pydantic
        "location": {"latitude": lat_str, "longitude": lon_str},
        "max_wind": max_wind_kts,
        "min_pressure": min_pressure_mb,
        "ne34": ne34,
        "se34": se34,
        "sw34": sw34,
        "nw34": nw34,
        "ne50": ne50,
        "se50": se50,
        "sw50": sw50,
        "nw50": nw50,
        "ne64": ne64,
        "se64": se64,
        "sw64": sw64,
        "nw64": nw64,
        "max_wind_radius": max_wind_radius,
    }
