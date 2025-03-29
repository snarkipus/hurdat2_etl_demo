"""Unit tests for the transformation parser functions."""

from datetime import UTC, datetime

import pytest

from etl_pipeline.transform.models import StormStatus
from etl_pipeline.transform.parser import (
    is_header_line,
    parse_cyclone_id,
    parse_datetime,
    parse_header_line,
    parse_int_or_none,
    parse_track_line,
)

# --- Test is_header_line ---


@pytest.mark.parametrize(
    "row, expected",
    [
        (["AL011851", "UNNAMED", "14", ""], True),  # Valid header
        (
            ["AL011851", "UNNAMED", "14"],
            True,
        ),  # Valid header without trailing empty string
        (["AL011851", "UNNAMED"], False),  # Too few fields
        (
            ["AL011851", "UNNAMED", "14", "EXTRA"],
            False,
        ),  # Too many fields (after cleaning)
        (["XX011851", "UNNAMED", "14"], True),  # Basin pattern allows any two letters
        (["AL01185", "UNNAMED", "14"], False),  # Invalid ID format (too short)
        (["AL011851", "UNNAMED", "ABC"], False),  # Invalid entry count
        ([], False),  # Empty row
        (["", "", ""], False),  # Blank row
    ],
)
def test_is_header_line(row, expected):
    """Test header line identification."""
    assert is_header_line(row) == expected


# --- Test parse_cyclone_id ---


@pytest.mark.parametrize(
    "cyclone_id, expected",
    [
        ("AL011851", ("AL", 1, 1851)),
        ("CP052023", ("CP", 5, 2023)),
        (" AL011851 ", ("AL", 1, 1851)),  # With whitespace
        ("AL992000", ("AL", 99, 2000)),  # Max cyclone number
        ("AL001900", ("AL", 0, 1900)),  # Min cyclone number
        ("AL011799", None),  # Year out of range (low)
        ("AL012101", None),  # Year out of range (high)
        ("AL1001999", None),  # Cyclone number out of range
        ("AL11999", None),  # Too short
        ("AL011999X", None),  # Extra char
        ("XX011999", ("XX", 1, 1999)),  # Non-standard basin allowed by regex
        ("", None),
        ("AL01", None),
    ],
)
def test_parse_cyclone_id(cyclone_id, expected):
    """Test cyclone ID parsing."""
    assert parse_cyclone_id(cyclone_id) == expected


# --- Test parse_header_line ---


@pytest.mark.parametrize(
    "line_data, expected",
    [
        (["AL011851", "UNNAMED", "14", ""], ("AL", 1, 1851, "UNNAMED")),
        ([" CP052023 ", "  HILARY ", " 50 "], ("CP", 5, 2023, "HILARY")),
        (["AL011851", "UNNAMED"], None),  # Too few fields
        (["AL011851", "", "14"], None),  # Empty name
        (["INVALID", "NAME", "10"], None),  # Invalid ID format
    ],
)
def test_parse_header_line(line_data, expected):
    """Test parsing of valid and invalid header lines."""
    assert parse_header_line(line_data) == expected


# --- Test parse_datetime ---


@pytest.mark.parametrize(
    "date_str, time_str, expected",
    [
        ("18510625", "0000", datetime(1851, 6, 25, 0, 0, tzinfo=UTC)),
        ("20231030", "1830", datetime(2023, 10, 30, 18, 30, tzinfo=UTC)),
        (
            " 20231030 ",
            " 1830 ",
            datetime(2023, 10, 30, 18, 30, tzinfo=UTC),
        ),  # Whitespace
        (
            "20231030",
            "",
            datetime(2023, 10, 30, 0, 0, tzinfo=UTC),
        ),  # Missing time defaults to 0000
        (
            "20231030",
            " ",
            datetime(2023, 10, 30, 0, 0, tzinfo=UTC),
        ),  # Blank time defaults to 0000
        (
            "20231030",
            "ABCD",
            datetime(2023, 10, 30, 0, 0, tzinfo=UTC),
        ),  # Invalid time defaults to 0000
        ("20231301", "1200", None),  # Invalid month
        ("20231030", "2500", None),  # Invalid hour
        ("202310", "1200", None),  # Invalid date format
        ("", "1200", None),  # Missing date
    ],
)
def test_parse_datetime(date_str, time_str, expected):
    """Test datetime parsing."""
    assert parse_datetime(date_str, time_str) == expected


# --- Test parse_int_or_none ---


@pytest.mark.parametrize(
    "value_str, expected",
    [
        ("100", 100),
        ("  50 ", 50),
        ("0", 0),
        ("-50", -50),  # Negative allowed by parser, validated by model
        ("-999", None),
        (" -99 ", None),
        ("", None),
        (" ", None),
        ("abc", None),
        ("10.5", None),
    ],
)
def test_parse_int_or_none(value_str, expected):
    """Test parsing integers with missing value handling."""
    assert parse_int_or_none(value_str) == expected


# --- Test parse_track_line ---

# Basic valid track line example
VALID_TRACK_ROW = [
    "18510625",
    "0000",
    " ",
    "HU",
    "28.0N",
    "94.8W",
    "80",
    "-999",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",  # Fill remaining fields
]
# Expected dictionary structure (coordinates passed as strings)
EXPECTED_VALID_DICT = {
    "date": datetime(1851, 6, 25, 0, 0, tzinfo=UTC),
    "record_identifier": None,
    "status": StormStatus.HURRICANE,
    "location": {"latitude": "28.0N", "longitude": "94.8W"},
    "max_wind": 80,
    "min_pressure": None,
    "ne34": None,
    "se34": None,
    "sw34": None,
    "nw34": None,
    "ne50": None,
    "se50": None,
    "sw50": None,
    "nw50": None,
    "ne64": None,
    "se64": None,
    "sw64": None,
    "nw64": None,
    "max_wind_radius": None,
}

# Track line with more data including radii and max_wind_radius
VALID_TRACK_ROW_FULL = [
    "20050829",
    "1200",
    "I",
    "HU",
    "29.5N",
    "89.6W",
    "110",
    "920",
    "200",
    "175",
    "120",
    "150",  # 34kt
    "100",
    "90",
    "60",
    "75",  # 50kt
    "50",
    "45",
    "30",
    "40",  # 64kt
    "40",  # Max wind radius
]
EXPECTED_VALID_DICT_FULL = {
    "date": datetime(2005, 8, 29, 12, 0, tzinfo=UTC),
    "record_identifier": "I",
    "status": StormStatus.HURRICANE,
    "location": {"latitude": "29.5N", "longitude": "89.6W"},
    "max_wind": 110,
    "min_pressure": 920,
    "ne34": 200,
    "se34": 175,
    "sw34": 120,
    "nw34": 150,
    "ne50": 100,
    "se50": 90,
    "sw50": 60,
    "nw50": 75,
    "ne64": 50,
    "se64": 45,
    "sw64": 30,
    "nw64": 40,
    "max_wind_radius": 40,
}


@pytest.mark.parametrize(
    "line_data, expected",
    [
        (VALID_TRACK_ROW, EXPECTED_VALID_DICT),
        (VALID_TRACK_ROW_FULL, EXPECTED_VALID_DICT_FULL),
        (VALID_TRACK_ROW[:20], None),  # Too few fields
        (["invalid_date"] + VALID_TRACK_ROW[1:], None),  # Invalid date
        (
            [VALID_TRACK_ROW[0], VALID_TRACK_ROW[1], "L", "INVALID_STATUS"]
            + VALID_TRACK_ROW[4:],
            None,
        ),  # Invalid status
    ],
)
def test_parse_track_line(line_data, expected):
    """Test parsing of valid and invalid track lines."""
    assert parse_track_line(line_data) == expected
