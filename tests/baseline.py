"""Source-derived persisted oracle shared by checkout and installed-wheel tests.

test_data.txt lines 8/30 and ref/schema_def.md establish UTC, knots, millibars,
NE/SE/SW/NW radii (nm), and longitude/latitude WKT. No application imports.
"""

from datetime import UTC, datetime
from math import isclose
from pathlib import Path

import duckdb


def assert_baseline(output: Path) -> None:
    """Independently reopen a finalized database and check explicit source values."""
    with duckdb.connect(str(output), read_only=True) as connection:
        assert connection.execute(
            "SELECT version_num FROM alembic_version"
        ).fetchall() == [("6accd1b8062d",)]
        assert connection.execute("SELECT count(*) FROM observations").fetchone() == (
            34,
        )
        assert connection.execute(
            "SELECT storm_id, basin, cyclone_number, year, name "
            "FROM storms ORDER BY storm_id"
        ).fetchall() == [
            ("AL122007", "AL", 12, 2007, "KAREN"),
            ("AL162023", "AL", 16, 2023, "OPHELIA"),
        ]
        rows = connection.execute("""
            SELECT storm_id, date, record_identifier,
                   status, max_wind, max_wind_mph, min_pressure,
                   ne34, se34, sw34, nw34, ne50, se50, sw50, nw50,
                   ne64, se64, sw64, nw64, max_wind_radius, geom
            FROM observations
            WHERE (storm_id = 'AL122007' AND geom = 'POINT(-42.4 11.7)')
               OR (storm_id = 'AL162023' AND record_identifier = 'L')
            ORDER BY storm_id
        """).fetchall()
    expected = [
        (
            "AL122007",
            datetime(2007, 9, 26, 12, tzinfo=UTC),
            None,
            "HU",
            65,
            74.8,
            988,
            90,
            60,
            40,
            45,
            60,
            40,
            25,
            30,
            40,
            30,
            0,
            15,
            None,
            "POINT(-42.4 11.7)",
        ),
        (
            "AL162023",
            datetime(2023, 9, 23, 10, 15, tzinfo=UTC),
            "L",
            "TS",
            60,
            69.0,
            981,
            270,
            120,
            90,
            80,
            40,
            40,
            50,
            60,
            0,
            0,
            0,
            0,
            30,
            "POINT(-77.1 34.7)",
        ),
    ]
    assert len(rows) == len(expected)
    for row, wanted in zip(rows, expected, strict=True):
        # SQL TIMESTAMP means UTC, never the host's local timezone.
        assert row[1].tzinfo is None
        actual = (row[0], row[1].replace(tzinfo=UTC), *row[2:])
        assert actual[:5] == wanted[:5]
        # 65 * 1.15078 -> 74.8; 60 * 1.15078 -> 69.0. Allow FLOAT error only.
        assert isclose(actual[5], wanted[5], rel_tol=0, abs_tol=0.00001)
        assert actual[6:] == wanted[6:]
