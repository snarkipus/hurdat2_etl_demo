"""Test database operations in the load module."""

import os
from datetime import datetime
from tempfile import NamedTemporaryFile

import pytest
from pysqlite3 import dbapi2 as sqlite3  # type: ignore

from hurdat2_etl.config.settings import Settings
from hurdat2_etl.exceptions import (
    DatabaseInitializationError,
    DatabaseInsertionError,
    DatabaseValidationError,
)
from hurdat2_etl.extract.types import StormStatus
from hurdat2_etl.load.load import DatabaseManager, Load
from hurdat2_etl.models import Observation, Point, Storm


@pytest.fixture
def temp_db():
    """Create a temporary database file."""
    with NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    yield db_path

    # Cleanup
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture
def sample_storm():
    """Create a sample storm with observations."""
    return Storm(
        basin="AL",
        cyclone_number=1,
        year=2023,
        name="TEST_STORM",
        observations=[
            Observation(
                date=datetime(2023, 1, 1),
                record_identifier="TEST1",
                status=StormStatus.TROPICAL_STORM,
                location=Point(latitude=25.0, longitude=-80.0),
                max_wind=50,
                min_pressure=995,
                ne34=60,
                se34=50,
                sw34=40,
                nw34=45,
                ne50=30,
                se50=25,
                sw50=20,
                nw50=25,
                ne64=None,
                se64=None,
                sw64=None,
                nw64=None,
                max_wind_radius=25,
            ),
            Observation(
                date=datetime(2023, 1, 2),
                record_identifier="TEST2",
                status=StormStatus.HURRICANE,
                location=Point(latitude=26.0, longitude=-79.0),
                max_wind=75,
                min_pressure=980,
                ne34=100,
                se34=90,
                sw34=80,
                nw34=85,
                ne50=50,
                se50=45,
                sw50=40,
                nw50=45,
                ne64=25,
                se64=20,
                sw64=15,
                nw64=20,
                max_wind_radius=15,
            ),
        ],
    )


def test_database_manager_basic(temp_db):
    """Test basic DatabaseManager functionality."""
    load = Load(db_path=temp_db)
    load.init_database()
    manager = DatabaseManager(temp_db)
    conn = manager.get_connection()

    # Test basic connection works
    assert isinstance(conn, sqlite3.Connection)

    # Test PRAGMA settings
    cur = conn.cursor()
    for pragma, expected_value in Settings.DB_PRAGMA_SETTINGS.items():
        cur.execute(f"PRAGMA {pragma}")
        value = cur.fetchone()[0]
        # Strip quotes from expected value for comparison
        expected = expected_value.strip("'")
        assert str(value) == "1" if expected.upper() == "ON" else str(value).upper() == expected.upper()

    manager.close_all()


def test_init_database(temp_db):
    """Test database initialization with enhanced schema."""
    load = Load(db_path=temp_db)
    load.init_database()

    # Verify database was created
    assert os.path.exists(temp_db)

    manager = DatabaseManager(temp_db)
    conn = manager.get_connection()
    cur = conn.cursor()

    # Check tables and their constraints
    cur.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type='table'
        AND name IN ('storms', 'observations')
        """
    )
    table_schemas = cur.fetchall()
    schema_text = " ".join(schema[0] for schema in table_schemas).upper()

    # Verify enhanced constraints (schema is returned in uppercase)
    assert "CONSTRAINT VALID_BASIN" in schema_text
    assert "CONSTRAINT VALID_CYCLONE_NUMBER" in schema_text
    assert "CONSTRAINT VALID_YEAR" in schema_text
    assert "ON DELETE CASCADE" in schema_text

    # Check spatial metadata
    cur.execute("SELECT srid FROM spatial_ref_sys WHERE srid=4326")
    assert cur.fetchone() is not None

    # Check triggers
    cur.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type='trigger'
        AND name IN ('observations_geom_validate', 'observations_validate')
        """
    )
    triggers = cur.fetchall()
    assert len(triggers) == 2

    manager.return_connection(conn)
    manager.close_all()


def test_insert_storms_with_batch_size(temp_db, sample_storm):
    """Test inserting observations with custom batch size."""
    load = Load(db_path=temp_db, batch_size=1)
    load.init_database()

    # Test with small batch size
    load.insert_storms([sample_storm])

    manager = DatabaseManager(temp_db)
    conn = manager.get_connection()
    cur = conn.cursor()

    # Verify all data was inserted correctly
    cur.execute("SELECT COUNT(*) FROM observations")
    assert cur.fetchone()[0] == len(sample_storm.observations)

    manager.return_connection(conn)
    manager.close_all()


def test_validate_database_enhanced(temp_db, sample_storm):
    """Test enhanced database validation features."""
    load = Load(db_path=temp_db)
    load.init_database()
    load.insert_storms([sample_storm])

    validation_results = load.validate_database()

    # Check enhanced basin stats
    basin_stats = validation_results["basin_stats"]
    assert len(basin_stats) == 1
    basin, count, start, end, years, avg_obs = basin_stats[0]
    assert basin == "AL"
    assert count == 1
    assert start == 2023
    assert end == 2023
    assert years == 1
    assert avg_obs == 2.0  # Two observations per storm

    # Check enhanced intensity stats
    intensity_stats = validation_results["intensity_stats"]
    for stat in intensity_stats:
        category, count, min_p, avg_p, max_w, earliest, latest = stat
        assert category in ("TD", "TS", "Cat1-2", "Cat3+")
        assert count > 0
        assert min_p is not None
        assert avg_p is not None
        assert max_w is not None
        assert earliest == "2023"
        assert latest == "2023"

    # Check enhanced spatial stats
    spatial_stats = validation_results["spatial_stats"]
    min_lon, max_lon, min_lat, max_lat, obs_count, months, storm_count = spatial_stats
    assert -80.0 <= min_lon <= -79.0
    assert -80.0 <= max_lon <= -79.0
    assert 25.0 <= min_lat <= 26.0
    assert 25.0 <= max_lat <= 26.0
    assert obs_count == 2
    assert months == 1  # January only
    assert storm_count == 1


def test_error_handling(temp_db, sample_storm):
    """Test specific error handling cases."""
    # Test database initialization error
    load = Load(db_path="/invalid/path/db.sqlite")
    with pytest.raises(DatabaseInitializationError):
        load.init_database()

    # Test invalid storm data
    load = Load(db_path=temp_db)
    load.init_database()

    # Modify sample storm to have invalid basin
    invalid_storm = sample_storm
    invalid_storm.basin = "XX"  # This will pass Pydantic but fail DB constraint

    with pytest.raises(DatabaseInsertionError, match="CHECK constraint failed: valid_basin"):
        load.insert_storms([invalid_storm])

    # Test validation error
    os.remove(temp_db)  # Remove database file
    with pytest.raises(DatabaseValidationError):
        load.validate_database()


def test_input_validation(temp_db, sample_storm):
    """Test input validation and sanitization."""
    load = Load(db_path=temp_db)
    load.init_database()

    # Test empty storm list
    with pytest.raises(ValueError, match="No storm data provided for insertion"):
        load.insert_storms([])

    # Test invalid batch size
    with pytest.raises(ValueError, match="Batch size must be positive"):
        Load(db_path=temp_db, batch_size=0)

    # Test invalid coordinates
    invalid_obs = sample_storm.observations[0]
    invalid_obs.location = Point(latitude=91.0, longitude=0.0)  # Invalid latitude
    with pytest.raises(DatabaseInsertionError, match="Latitude out of range"):
        load.insert_storms([sample_storm])


def test_missing_values(temp_db, sample_storm):
    """Test handling of missing values."""
    load = Load(db_path=temp_db)
    load.init_database()

    # Test various missing value scenarios
    sample_storm.observations[0].max_wind = -999  # Missing value
    sample_storm.observations[0].min_pressure = -99  # Alternative missing value
    sample_storm.observations[1].max_wind = None  # NULL value
    sample_storm.observations[1].min_pressure = None  # NULL value

    # Should not raise any exceptions
    load.insert_storms([sample_storm])

    manager = DatabaseManager(temp_db)
    conn = manager.get_connection()
    cur = conn.cursor()

    # Verify missing values were stored correctly
    cur.execute(
        """
        SELECT max_wind, min_pressure
        FROM observations
        ORDER BY date
        """
    )
    results = cur.fetchall()
    assert results[0] == (-999, -99)  # Missing values preserved
    assert results[1] == (None, None)  # NULL values preserved

    manager.return_connection(conn)
    manager.close_all()
