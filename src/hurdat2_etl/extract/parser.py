"""HURDAT2 format parser implementation."""

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Final

from hurdat2_etl.exceptions import ExtractionError
from hurdat2_etl.models import Observation, Point, Storm

HEADER_FIELDS_CNT: Final = 3
CYCLONE_ID_LENGTH: Final = 8


def parse_header(line: str) -> tuple[str, int, int, str, int]:
    """Parse HURDAT2 header line into storm components."""

    """Format:
    AL092021,                IDA,     40,
    1234567890123456789012345768901234567

    AL (Spaces 1 and 2) - Basin - Atlantic
    09 (Spaces 3 and 4) - ATCF cyclone number for that year
    2021 (Spaces 5-8, before first comma) - Year
    IDA (Spaces 19-28, before second comma) - Name, if available, or else “UNNAMED”
    40 (Spaces 34-36) - Number of best track entries - rows - to follow
    """
    if not line.strip():
        raise ExtractionError("Empty header line")

    # Remove trailing commas and whitespace before splitting
    line = line.rstrip(",\n\r\t")
    parts = [part.strip() for part in line.split(",")]

    if len(parts) != HEADER_FIELDS_CNT:
        raise ExtractionError(f"Expected 3 header parts, got {len(parts)}: {line}")

    cyclone_id, name, obs_count = parts

    # Basic string format checks
    if len(cyclone_id) != CYCLONE_ID_LENGTH:
        raise ExtractionError(f"Invalid cyclone ID format: {cyclone_id}")

    try:
        basin = cyclone_id[:2]
        number = int(cyclone_id[2:4])  # Parse only, validation in model
        year = int(cyclone_id[4:])  # Parse only, validation in model
        obs_count_int = int(obs_count)  # Parse only, validation in model
    except ValueError as e:
        raise ExtractionError(f"Failed to parse numeric values in header: {e}") from e

    return basin, number, year, name, obs_count_int


def parse_observation(line: str) -> Observation:
    """Parse observation line into Observation model."""

    """Format:
    20210829, 1655, L, HU, 29.1N,  90.2W, 130,  931,  130,  110,   80,  110,   70,   60,   40,   60,   45,   35,   20,   30,   10
    12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345

    2021 (Spaces 1-4) - Year
    08 (Spaces 5-6) - Month
    29 (Spaces 7-8, before 1st comma) - Day
    16 (Spaces 11-12) - Hours in UTC (Universal Time Coordinate)
    55 (Spaces 13-14, before 2nd comma) - Minutes
    L (Space 17, before 3rd comma) - Record identifier (see notes below)
        C - Closest approach to a coast, not followed by a landfall
        G - Genesis
        I - An intensity peak in terms of both pressure and wind
        L - Landfall (center of system crossing a coastline)
        P - Minimum in central pressure
        R - Provides additional detail on the intensity of the cyclone when rapid changes are underway
        S - Change of status of the system
        T - Provides additional detail on the track (position) of the cyclone
        W - Maximum sustained wind speed
    HU (Spaces 20-21, before 4th comma) - Status of system. Options are:
        TD - Tropical cyclone of tropical depression intensity (< 34 knots)
        TS - Tropical cyclone of tropical storm intensity (34-63 knots)
        HU - Tropical cyclone of hurricane intensity (> 64 knots)
        EX - Extratropical cyclone (of any intensity)
        SD - Subtropical cyclone of subtropical depression intensity (< 34 knots)
        SS - Subtropical cyclone of subtropical storm intensity (> 34 knots)
        LO - A low that is neither a tropical cyclone, a subtropical cyclone, nor an extratropical cyclone (of any intensity)
        WV - Tropical Wave (of any intensity)
        DB - Disturbance (of any intensity)
    29.1 (Spaces 24-27) - Latitude
    N (Space 28, before 5th comma) - Hemisphere - North or South
    90.2 (Spaces 31-35) - Longitude
    W (Space 36, before 6th comma) - Hemisphere - West or East
    130 (Spaces 39-41, before 7th comma) - Maximum sustained wind (in knots)
    931 (Spaces 44-47, before 8th comma) - Minimum Pressure (in millibars)
    130 (Spaces 50-53, before 9th comma) - 34 kt wind radii maximum extent in northeastern quadrant (in nautical miles)
    110 (Spaces 56-59, before 10th comma) - 34 kt wind radii maximum extent in southeastern quadrant (in nautical miles)
    70 (Spaces 62-65, before 11th comma) - 34 kt wind radii maximum extent in southwestern quadrant (in nautical miles)
    60 (Spaces 68-71, before 12th comma) - 34 kt wind radii maximum extent in northwestern quadrant (in nautical miles)
    40 (Spaces 74-77, before 13th comma) - 50 kt wind radii maximum extent in northeastern quadrant (in nautical miles)
    60 (Spaces 80-83, before 14th comma) - 50 kt wind radii maximum extent in southeastern quadrant (in nautical miles)
    80 (Spaces 86-89, before 15th comma) - 50 kt wind radii maximum extent in southwestern quadrant (in nautical miles)
    30 (Spaces 92-95, before 16th comma) - 50 kt wind radii maximum extent in northwestern quadrant (in nautical miles)
    45 (Spaces 98-101, before 17th comma) - 64 kt wind radii maximum extent in northeastern quadrant (in nautical miles)
    25 (Spaces 104-107, before 18th comma) - 64 kt wind radii maximum extent in southeastern quadrant (in nautical miles)
    35 (Spaces 110-113, before 19th comma) - 64 kt wind radii maximum extent in southwestern quadrant (in nautical miles)
    20 (Spaces 116-119, before 20th comma) - 64 kt wind radii maximum extent in northwestern quadrant (in nautical miles)
    15 (Spaces 122-125) - Radius of Maximum Wind (in nautical miles)
    """  # noqa: E501
    if not line.strip():
        raise ExtractionError("Empty line found in data")

    try:
        fields = [f.strip() for f in line.strip().split(",")]

        # Parse Date and Time
        date = datetime.strptime(f"{fields[0]},{fields[1]}", "%Y%m%d,%H%M")

        # Parse Record Identifier
        record_id = fields[2] if fields[2] else None

        # Parse Storm Status
        status = Observation.parse_storm_status(fields[3])

        # Parse Point object
        location = Point(latitude=fields[4], longitude=fields[5])  # type: ignore

        # Parse wind speed and pressure
        max_wind = Observation.parse_possible_missing(fields[6])
        min_pressure = Observation.parse_possible_missing(fields[7])

        # Create wind radii dictionaries
        wind_radii_34kt = {
            "NE": Observation.parse_possible_missing(fields[8]),
            "SE": Observation.parse_possible_missing(fields[9]),
            "SW": Observation.parse_possible_missing(fields[10]),
            "NW": Observation.parse_possible_missing(fields[11]),
        }

        wind_radii_50kt = {
            "NE": Observation.parse_possible_missing(fields[12]),
            "SE": Observation.parse_possible_missing(fields[13]),
            "SW": Observation.parse_possible_missing(fields[14]),
            "NW": Observation.parse_possible_missing(fields[15]),
        }

        wind_radii_64kt = {
            "NE": Observation.parse_possible_missing(fields[16]),
            "SE": Observation.parse_possible_missing(fields[17]),
            "SW": Observation.parse_possible_missing(fields[18]),
            "NW": Observation.parse_possible_missing(fields[19]),
        }

        max_wind_radius = Observation.parse_possible_missing(fields[20])

        return Observation(
            date=date,
            record_identifier=record_id,
            status=status,
            location=location,
            max_wind=max_wind,
            min_pressure=min_pressure,
            ne34=wind_radii_34kt["NE"],
            se34=wind_radii_34kt["SE"],
            sw34=wind_radii_34kt["SW"],
            nw34=wind_radii_34kt["NW"],
            ne50=wind_radii_50kt["NE"],
            se50=wind_radii_50kt["SE"],
            sw50=wind_radii_50kt["SW"],
            nw50=wind_radii_50kt["NW"],
            ne64=wind_radii_64kt["NE"],
            se64=wind_radii_64kt["SE"],
            sw64=wind_radii_64kt["SW"],
            nw64=wind_radii_64kt["NW"],
            max_wind_radius=max_wind_radius,
        )

    except ValueError as e:
        raise ExtractionError(f"Invalid observation format: {e}") from e
    except IndexError:
        raise ExtractionError("Missing required fields in observation") from None


def parse_hurdat2(filepath: Path) -> Iterator[Storm]:
    """Parse HURDAT2 file into Storm objects."""

    """Format:
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
    try:
        with open(filepath, encoding="utf-8") as f:
            line_num = 0
            while True:
                header = f.readline()
                if not header:
                    break

                line_num += 1
                try:
                    basin, number, year, name, num_observations = parse_header(header)
                except Exception as e:
                    raise ExtractionError(
                        f"Failed to parse header at line {line_num}: {header.strip()}\n"
                        f"Error: {e!s}"
                    ) from e

                observations = []
                for _ in range(num_observations):
                    line = f.readline()
                    line_num += 1
                    try:
                        observations.append(parse_observation(line))
                    except Exception as e:
                        raise ExtractionError(
                            f"Failed to parse observation at line {line_num}: "
                            f"{line.strip()}\n"
                            f"Error: {e!s}"
                        ) from e

                yield Storm(
                    basin=basin,
                    cyclone_number=number,
                    year=year,
                    name=name,
                    observations=observations,
                )

    except FileNotFoundError:
        raise ExtractionError(f"HURDAT2 file not found: {filepath}") from None
    except Exception as e:
        raise ExtractionError(f"Failed to parse HURDAT2 file: {e!s}") from e
