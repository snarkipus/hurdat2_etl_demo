"""Tests for the DatabaseOperations class using the Load initialization pattern."""

import os
import pytest
from datetime import datetime
from hurdat2_etl.load.load import Load
from hurdat2_etl.load.operations import DatabaseOperations
from hurdat2_etl.load.connection import DatabaseManager
from hurdat2_etl.models import Storm, Observation, Point
from hurdat2_etl.extract.types import StormStatus

@pytest.fixture
def temp_db(tmp_path):
    """Provide a temporary database file path."""
    db_file = tmp_path / "test.db"
    return str(db_file)

def test_insert_storms_valid_data(temp_db):
    """Test inserting valid storm data into the database."""
    # Initialize the database using the Load class
    load = Load(db_path=temp_db)
    load.init_database()  # This should create all required tables

    # Add debug logging to verify table creation
    manager = DatabaseManager(temp_db)
    conn = manager.get_connection()
    cur = conn.cursor()

    # Check all tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cur.fetchall()
    print(f"Created tables: {[table[0] for table in tables]}")  # Debug print

    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='storms'")
    storms_table = cur.fetchone()
    assert storms_table is not None, "Storms table not created"

    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='observations'")
    obs_table = cur.fetchone()
    assert obs_table is not None, "Observations table not created"
    manager.return_connection(conn)
    manager.close_all()

    # Create a list of valid Storm objects
    storms = [
        Storm(
            basin="AL",
            cyclone_number=1,
            year=2023,
            name="TEST1",
            observations=[
                Observation(
                    date=datetime(2023, 1, 1),
                    record_identifier="TEST1",
                    status=StormStatus.TROPICAL_STORM,
                    location=Point(latitude=25.0, longitude=-80.0),
                    max_wind=50,
                    min_pressure=995,
                )
            ],
        ),
        Storm(
            basin="AL",
            cyclone_number=2,
            year=2023,
            name="TEST2",
            observations=[
                Observation(
                    date=datetime(2023, 1, 2),
                    record_identifier="TEST2",
                    status=StormStatus.HURRICANE,
                    location=Point(latitude=26.0, longitude=-79.0),
                    max_wind=100,
                    min_pressure=950,
                )
            ],
        ),
    ]

    # Insert the storms into the database using DatabaseOperations
    db_operations = DatabaseOperations(temp_db)
    db_operations.insert_storms(storms, batch_size=100)

    # Verify that the storms were inserted correctly
    manager = DatabaseManager(temp_db)
    conn = manager.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM storms")
    count = cur.fetchone()[0]
    assert count == 2, f"Expected 2 storms, got {count}"
    manager.return_connection(conn)
    manager.close_all()
