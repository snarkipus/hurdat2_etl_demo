from datetime import UTC, datetime  # Add UTC

import pytest
from pydantic import ValidationError

from etl_pipeline.transform.models import Observation, Point, Storm, StormStatus


# Point Model Tests
class TestPoint:
    def test_coordinate_parsing(self):
        """Test parsing valid HURDAT2 coordinate formats."""
        test_cases = [
            ("29.1N", 29.1),  # North latitude
            ("29.1S", -29.1),  # South latitude
            ("90.2W", -90.2),  # West longitude
            ("90.2E", 90.2),  # East longitude
            ("0.0N", 0.0),  # Zero latitude
            ("180.0E", 180.0),  # Max longitude
            ("-0.0W", 0.0),  # Negative zero longitude
        ]

        for coord_str, expected in test_cases:
            if "N" in coord_str or "S" in coord_str:
                assert (
                    Point.convert_hurdat2_coordinates(coord_str, is_latitude=True)
                    == expected
                )
            else:
                assert (
                    Point.convert_hurdat2_coordinates(coord_str, is_latitude=False)
                    == expected
                )

    def test_invalid_coordinate_format(self):
        """Test invalid coordinate formats."""
        invalid_coords = [
            "29.1",  # Missing direction
            "N29.1",  # Wrong order
            "29.1X",  # Invalid direction
            "ABCN",  # Invalid number
            "",  # Empty string
        ]
        for coord in invalid_coords:
            with pytest.raises(ValueError, match="Invalid HURDAT2 format"):
                Point.convert_hurdat2_coordinates(coord, is_latitude=True)
            with pytest.raises(ValueError, match="Invalid HURDAT2 format"):
                Point.convert_hurdat2_coordinates(coord, is_latitude=False)

    def test_out_of_range_coordinates(self):
        """Test coordinates outside valid ranges."""
        # Test invalid latitude
        with pytest.raises(ValidationError) as exc_info:
            Point(latitude="91.0N", longitude="90.0W")
        assert "Latitude 91.0 out of range [0, 90]" in str(exc_info.value)

        # Test invalid longitude
        with pytest.raises(ValidationError) as exc_info:
            Point(latitude="45.0N", longitude="381.0E")
        assert "Longitude 381.0 out of range [0, 360]" in str(exc_info.value)

        # Test individual coordinate validation
        with pytest.raises(ValidationError) as exc_info:
            Point(latitude="91.0N", longitude="45.0W")
        assert "Latitude 91.0 out of range [0, 90]" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            Point(latitude="45.0N", longitude="381.0E")
        assert "Longitude 381.0 out of range [0, 360]" in str(exc_info.value)

    def test_point_construction(self):
        """Test Point construction with valid coordinates."""
        point = Point(latitude="29.1N", longitude="90.2W")
        assert point.latitude == 29.1
        assert point.longitude == -90.2

    def test_eastern_longitude_is_not_wrapped(self):
        """Normalization is unchanged; geographic acceptance is a separate gate."""
        point = Point(latitude="25.0N", longitude="270E")
        assert point.longitude == 270.0
        assert point.latitude == 25.0

    def test_invalid_coordinate_format_instantiation(self):
        """Test Point instantiation with invalid coordinate formats."""
        invalid_formats = [
            ("29.1", "90.0W"),  # Missing direction lat
            ("29.1N", "90.0"),  # Missing direction lon
            ("N29.1", "90.0W"),  # Wrong order lat
            ("29.1N", "W90.0"),  # Wrong order lon
            ("29.1X", "90.0W"),  # Invalid direction lat
            ("29.1N", "90.0X"),  # Invalid direction lon
            ("ABCN", "90.0W"),  # Invalid number lat
            ("29.1N", "ABCW"),  # Invalid number lon
            ("", "90.0W"),  # Empty string lat
            ("29.1N", ""),  # Empty string lon
        ]
        for lat_str, lon_str in invalid_formats:
            with pytest.raises(ValidationError):
                Point(latitude=lat_str, longitude=lon_str)


# Storm Model Tests
class TestStorm:
    """Test Storm model validation."""

    def test_valid_storm(self):
        """Test Storm creation with valid data."""
        storm = Storm(
            basin="AL", cyclone_number=12, year=2007, name="KAREN", observations=[]
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
            {"basin": "AL", "cyclone_number": 12, "year": 2007, "name": ""},
        ]

        for case in invalid_cases:
            name = case.pop("name", "TEST")  # Remove name if present, default to TEST
            # Expect Pydantic's ValidationError for model field validation
            with pytest.raises(ValidationError):
                Storm(**case, name=name, observations=[])


# Observation Model Tests
class TestObservation:
    def test_observation_creation(self):
        """Test creating a valid Observation instance."""
        obs = Observation(
            storm_id="AL01TEST",  # Add required storm_id
            date=datetime(2021, 8, 29, 12, 0, tzinfo=UTC),  # Add tzinfo
            record_identifier="L",
            status=StormStatus.HURRICANE,
            location=Point(latitude="29.1N", longitude="90.2W"),
            max_wind=130,
            min_pressure=931,
            ne34=100,
            se34=90,
            sw34=80,
            nw34=85,
        )
        assert obs.date.year == 2021
        assert obs.status == StormStatus.HURRICANE
        assert obs.max_wind == 130
        assert obs.min_pressure == 931
        assert obs.ne34 == 100
        assert obs.max_wind_radius is None  # Optional field
        assert obs.max_wind_mph == pytest.approx(
            130 * 1.15078, 0.1
        )  # Test mph conversion

    def test_wind_speed_conversion(self):
        """Test max_wind_mph computed field with various values."""
        test_cases = [
            (130, 149.6),  # 130 knots = 149.6 mph
            (0, 0.0),  # Zero wind speed
            (75, 86.3),  # 75 knots = 86.3 mph
            (None, None),  # Missing wind speed
        ]

        for max_wind, expected_mph in test_cases:
            obs = Observation(
                storm_id="AL01TEST",  # Add required storm_id
                date=datetime(2021, 8, 29, 12, 0, tzinfo=UTC),  # Add tzinfo
                status=StormStatus.HURRICANE,
                location=Point(latitude="29.1N", longitude="90.2W"),
                max_wind=max_wind,
                min_pressure=931,
            )
            if expected_mph is not None:
                assert obs.max_wind_mph == pytest.approx(expected_mph, abs=0.1)
            else:
                assert obs.max_wind_mph is None

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
            ("-999", None),  # Standard missing value
            ("-99", None),  # Alternative missing value
            ("100", 100),  # Valid integer
            (50, 50),  # Already an integer
            ("  50  ", 50),  # Whitespace
            ("-50", -50),  # Negative number (valid for parsing, validated by model)
        ]

        for value, expected in test_cases:
            assert Observation.parse_possible_missing(value) == expected

    def test_invalid_missing_value_format(self):
        """Test invalid formats for missing values."""
        invalid_values = [
            "abc",  # Non-numeric
            "",  # Empty string
            "12.34",  # Float
        ]
        for value in invalid_values:
            with pytest.raises(ValueError):
                Observation.parse_possible_missing(value)

    def test_observation_validation(self):
        """Test Observation model validation."""
        # Test validation for NonNegativeInt fields
        with pytest.raises(ValidationError):  # Expect ValidationError
            Observation(
                storm_id="AL01TEST",  # Add required storm_id
                date=datetime(2021, 8, 29, 12, 0, tzinfo=UTC),  # Add tzinfo
                record_identifier="L",
                status=StormStatus.HURRICANE,
                location=Point(latitude="29.1N", longitude="90.2W"),
                max_wind=-1,  # Invalid negative value
                min_pressure=931,
            )

        # Test optional fields can be None
        obs = Observation(
            storm_id="AL01TEST",  # Add required storm_id
            date=datetime(2021, 8, 29, 12, 0, tzinfo=UTC),  # Add tzinfo
            record_identifier="L",
            status=StormStatus.HURRICANE,
            location=Point(latitude="29.1N", longitude="90.2W"),
            max_wind=130,
            min_pressure=931,
            ne34=None,  # Optional fields set to None
            se34=None,
            max_wind_radius=None,
        )
        assert obs.ne34 is None
        assert obs.se34 is None
        assert obs.max_wind_radius is None
