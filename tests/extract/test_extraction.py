from pathlib import Path
from datetime import datetime
import pytest

from hurdat2_etl.extract.parser import parse_header, parse_hurdat2, parse_observation
from hurdat2_etl.extract.types import StormStatus
from hurdat2_etl.models import Point, Storm, Observation
from hurdat2_etl.exceptions import ExtractionError

TEST_DATA_DIR = Path(__file__).parent.parent / "data" / "hurdat2"

# Parser Tests
class TestParser:
    def test_parse_header(self):
        """Test parsing of storm header line."""
        header = "AL122007,              KAREN,     19,"  # Real format with spaces
        basin, number, year, name, count = parse_header(header)

        assert basin == "AL"
        assert number == 12
        assert year == 2007
        assert name == "KAREN"
        assert count == 19

    def test_invalid_header_format(self):
        """Test parsing invalid header string formats."""
        invalid_headers = [
            ("", "Empty header line"),
            ("AL122007,", "Expected 3 header parts"),
            ("AL122007,KAREN", "Expected 3 header parts"),
            ("AL122007,KAREN,19,EXTRA", "Expected 3 header parts"),
            ("ALXXXX,KAREN,19,", "Invalid cyclone ID format"),
            ("AL122007,KAREN,XX,", "Failed to parse numeric values"),
        ]

        for header, expected_msg in invalid_headers:
            with pytest.raises(ExtractionError, match=expected_msg):
                parse_header(header)

    def test_parse_observation(self):
        """Test parsing of observation line."""
        # Real format with spaces and padding
        line = "20070925, 0000,  , TD, 10.0N,  35.9W,  30, 1006,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0, -999"
        obs = parse_observation(line)

        assert obs.date.year == 2007
        assert obs.date.month == 9
        assert obs.date.day == 25
        assert obs.record_identifier is None  # Empty record identifier
        assert obs.status == StormStatus.TROPICAL_DEPRESSION
        assert obs.location.latitude == 10.0
        assert obs.location.longitude == -35.9
        assert obs.max_wind == 30
        assert obs.min_pressure == 1006

    def test_invalid_observation_format(self):
        """Test parsing invalid observation formats."""
        invalid_lines = [
            "",  # Empty line
            "20070925, 0000",  # Incomplete line
            "XXXXX, 0000,  , TD, 10.0N,  35.9W,  30, 1006",  # Invalid date
            "20070925, 0000,  , XX, 10.0N,  35.9W,  30, 1006",  # Invalid status
            "20070925, 0000,  , TD, 91.0N,  35.9W,  30, 1006",  # Invalid latitude
        ]
        for line in invalid_lines:
            with pytest.raises(ExtractionError):
                parse_observation(line)

    def test_parse_missing_values(self):
        """Test parsing of missing values (-999, -99)."""
        line = "20070925, 0000,  , TD, 10.0N,  35.9W,  30, 1006, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999"
        obs = parse_observation(line)

        assert obs.ne34 is None
        assert obs.se34 is None
        assert obs.max_wind_radius is None

# File Parsing Tests
class TestFileParsing:
    def test_parse_hurdat2(self):
        """Test parsing complete HURDAT2 format file."""
        storms = list(parse_hurdat2(TEST_DATA_DIR / "test_data.txt"))
        assert len(storms) == 1

        storm = storms[0]
        assert storm.basin == "AL"
        assert storm.cyclone_number == 12
        assert storm.year == 2007
        assert storm.name == "KAREN"
        assert len(storm.observations) == 19
        assert storm.observations[0].status == StormStatus.TROPICAL_DEPRESSION
        assert storm.storm_id == "AL122007"

    def test_file_not_found(self):
        """Test handling of non-existent file."""
        with pytest.raises(ExtractionError, match="HURDAT2 file not found"):
            list(parse_hurdat2(TEST_DATA_DIR / "nonexistent.txt"))

    def test_parse_empty_file(self, tmp_path):
        """Test parsing empty file."""
        empty_file = tmp_path / "empty.txt"
        empty_file.write_text("")

        storms = list(parse_hurdat2(empty_file))
        assert len(storms) == 0

# Point Model Tests
class TestPoint:
    def test_coordinate_parsing(self):
        """Test parsing valid HURDAT2 coordinate formats."""
        test_cases = [
            ("29.1N", 29.1),      # North latitude
            ("90.2W", -90.2),     # West longitude
            ("0.0N", 0.0),        # Zero latitude
            ("180.0E", 180.0),    # Max longitude
        ]

        for coord_str, expected in test_cases:
            assert Point.parse_coordinate(coord_str) == expected

    def test_invalid_coordinate_format(self):
        """Test invalid coordinate formats."""
        invalid_coords = [
            "29.1",      # Missing direction
            "N29.1",     # Wrong order
            "29.1X",     # Invalid direction
            "ABCN",      # Invalid number
            "",          # Empty string
        ]
        for coord in invalid_coords:
            with pytest.raises(ValueError, match="Invalid format"):
                Point.parse_coordinate(coord)

    def test_out_of_range_coordinates(self):
        """Test coordinates outside valid ranges."""
        invalid_coords = [
            ("91.0N", "Latitude 91.0 out of range"),
            ("181.0E", "Longitude 181.0 out of range"),
        ]
        for coord, error_msg in invalid_coords:
            with pytest.raises(ValueError, match=error_msg):
                Point.parse_coordinate(coord)

    def test_point_construction(self):
        """Test Point construction with valid coordinates."""
        point = Point(latitude="29.1N", longitude="90.2W")
        assert point.latitude == 29.1
        assert point.longitude == -90.2

# Storm Model Tests
class TestStorm:
    """Test Storm model validation."""

    def test_valid_storm(self):
        """Test Storm creation with valid data."""
        storm = Storm(
            basin="AL",
            cyclone_number=12,
            year=2007,
            name="KAREN",
            observations=[]
        )
        assert storm.storm_id == "AL122007"

    def test_invalid_storm_header_validation(self):
        """Test Storm model header validation."""
        invalid_cases = [
            # Basin validation
            {"basin": "XX", "cyclone_number": 12, "year": 2007},
            # Cyclone number validation
            {"basin": "AL", "cyclone_number": 100, "year": 2007},
            {"basin": "AL", "cyclone_number": -1, "year": 2007},
            # Year validation
            {"basin": "AL", "cyclone_number": 12, "year": 1799},
            {"basin": "AL", "cyclone_number": 12, "year": 2101},
            # Name validation
            {"basin": "AL", "cyclone_number": 12, "year": 2007, "name": ""}
        ]

        for case in invalid_cases:
            name = case.pop("name", "TEST")  # Remove name if present, default to TEST
            with pytest.raises(ValueError):
                Storm(
                    **case,
                    name=name,
                    observations=[]
                )

# Observation Model Tests
class TestObservation:
    def test_observation_creation(self):
        """Test creating a valid Observation instance."""
        obs = Observation(
            date=datetime(2021, 8, 29, 12, 0),
            record_identifier="L",
            status=StormStatus.HURRICANE,
            location=Point(latitude="29.1N", longitude="90.2W"),
            max_wind=130,
            min_pressure=931,
            ne34=100,
            se34=90,
            sw34=80,
            nw34=85
        )
        assert obs.date.year == 2021
        assert obs.status == StormStatus.HURRICANE
        assert obs.max_wind == 130
        assert obs.min_pressure == 931
        assert obs.ne34 == 100
        assert obs.max_wind_radius is None  # Optional field

    def test_parse_storm_status(self):
        """Test storm status parsing."""
        assert Observation.parse_storm_status("HU") == StormStatus.HURRICANE
        assert Observation.parse_storm_status("TS") == StormStatus.TROPICAL_STORM
        assert Observation.parse_storm_status("TD") == StormStatus.TROPICAL_DEPRESSION

    def test_invalid_storm_status(self):
        """Test invalid storm status."""
        with pytest.raises(ValueError, match="Invalid storm status"):
            Observation.parse_storm_status("INVALID")

    def test_parse_possible_missing(self):
        """Test parsing of possible missing values."""
        test_cases = [
            ("-999", None),    # Standard missing value
            ("-99", None),     # Alternative missing value
            ("100", 100),      # Valid integer
            (50, 50),         # Already an integer
            ("  50  ", 50),   # Whitespace
            ("-50", -50),     # Negative number (valid for parsing, validated by model)
        ]

        for value, expected in test_cases:
            assert Observation.parse_possible_missing(value) == expected

    def test_invalid_missing_value_format(self):
        """Test invalid formats for missing values."""
        invalid_values = [
            "abc",      # Non-numeric
            "",         # Empty string
            "12.34",    # Float
        ]
        for value in invalid_values:
            with pytest.raises(ValueError):
                Observation.parse_possible_missing(value)

    def test_observation_validation(self):
        """Test Observation model validation."""
        with pytest.raises(ValueError):
            Observation(
                date=datetime(2021, 8, 29, 12, 0),
                record_identifier="L",
                status=StormStatus.HURRICANE,
                location=Point(latitude="29.1N", longitude="90.2W"),
                max_wind=-1,  # Invalid negative value
                min_pressure=931
            )

        # Test optional fields can be None
        obs = Observation(
            date=datetime(2021, 8, 29, 12, 0),
            record_identifier="L",
            status=StormStatus.HURRICANE,
            location=Point(latitude="29.1N", longitude="90.2W"),
            max_wind=130,
            min_pressure=931,
            ne34=None,  # Optional fields set to None
            se34=None,
            max_wind_radius=None
        )
        assert obs.ne34 is None
        assert obs.se34 is None
        assert obs.max_wind_radius is None
