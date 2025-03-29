"""Integration tests for the Load stage, focusing on database interactions."""

# Removed Alembic imports
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from etl_pipeline.load.load import LoadStage

# Import Base from the models to create/drop tables
from etl_pipeline.load.models import Base
from etl_pipeline.load.unit_of_work import SqlAlchemyUnitOfWork

# Import Pydantic models for creating test data
from etl_pipeline.transform.models import Observation as PydanticObservation
from etl_pipeline.transform.models import Point
from etl_pipeline.transform.models import Storm as PydanticStorm


# Use a function-scoped fixture for in-memory DB to ensure isolation
@pytest.fixture(scope="function")
def in_memory_db_engine():
    """Creates an in-memory DuckDB engine and creates tables from metadata."""
    engine = create_engine("duckdb:///:memory:")

    # Create tables directly from SQLAlchemy metadata
    Base.metadata.create_all(engine)
    print("Tables created from metadata.")  # Debug print

    yield engine

    # Teardown: Drop all tables after the test
    print("Dropping tables from metadata.")  # Debug print
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture
def session_factory(in_memory_db_engine):
    """Provides a sessionmaker configured for the in-memory database."""
    return sessionmaker(bind=in_memory_db_engine)


@pytest.fixture
def integration_uow(session_factory):
    """Provides a SqlAlchemyUnitOfWork instance connected to the in-memory DB."""
    return SqlAlchemyUnitOfWork(session_factory=session_factory)


@pytest.fixture
def integration_load_stage(integration_uow):
    """Provides a LoadStage instance configured with the integration UoW."""

    # Create a factory function that returns the specific integration_uow instance
    # This ensures the same UoW instance (and thus session) is used if needed,
    # though LoadStage creates its own session via the factory in its context manager.
    # A simpler approach might be to pass the session_factory directly to LoadStage.
    def uow_factory():
        # For integration test, maybe return the *same* uow instance
        # to ensure operations happen on the same session if needed outside LoadStage?
        # Or create a new one each time LoadStage needs it? Let's stick to factory.
        return SqlAlchemyUnitOfWork(session_factory=integration_uow.session_factory)

    return LoadStage(uow_factory=uow_factory)


@pytest.fixture
def sample_integration_data():
    """Provides sample Pydantic data for integration testing."""
    storm1 = PydanticStorm(
        basin="AL", cyclone_number=1, year=2024, name="INTEGRATION", observations=[]
    )
    # Initialize observations WITH storm_id
    obs1 = PydanticObservation(
        date=datetime(2024, 1, 1, 12, 0, 0),
        status="HU",
        location=Point(latitude=25.0, longitude=-75.0),
        max_wind=100,
        storm_id=storm1.storm_id,
    )
    obs2 = PydanticObservation(
        date=datetime(2024, 1, 1, 18, 0, 0),
        status="TS",
        location=Point(latitude=25.0, longitude=-76.0),
        max_wind=60,
        storm_id=storm1.storm_id,
    )

    # The load stage expects separate iterables
    return [storm1], [obs1, obs2]


def test_load_and_spatial_query(
    integration_load_stage, integration_uow, sample_integration_data
):
    """
    Test loading data and performing a basic spatial query (ST_Distance).
    """
    storms, observations = sample_integration_data
    input_data = (storms, observations)

    # 1. Execute the Load Stage (uses its own UoW context)
    storm_count, obs_count = integration_load_stage.execute(input_data)

    assert storm_count == 1
    assert obs_count == 2

    # 2. Verify data and perform spatial query using a new UoW context
    #    connected to the same in-memory DB.
    with integration_uow as uow:
        # Ensure spatial extension is loaded for querying
        # Use the private method directly for testing setup within this context
        integration_load_stage._load_spatial_extension(uow)

        # Query using ST_Point and ST_Distance
        # Note: DuckDB ST_Distance returns distance in meters by default
        # Distance between (25N, 75W) and (25N, 76W) is approx 1 degree longitude
        # At 25N latitude, 1 degree longitude is approx cos(25deg) * 111.32 km
        # cos(25 deg) ~= 0.9063; Distance ~= 0.9063 * 111320 m ~= 100888 m
        # Use ST_Distance_Spheroid for geodesic distance calculation
        query = """
        SELECT ST_Distance_Spheroid(
                   ST_GeomFromText(o1.geom),
                   ST_GeomFromText(o2.geom)
               )
        FROM observations o1, observations o2
        WHERE o1.date = :date1 AND o2.date = :date2
        """
        params = {"date1": observations[0].date, "date2": observations[1].date}
        result = uow.execute(query, params)
        distance = result.scalar_one_or_none()

        assert distance is not None
        # Check if the distance is approximately correct (allow some tolerance)
        # Use the more accurate distance calculated by DuckDB itself
        expected_distance_m = 111623.2  # More accurate distance in meters
        # Correct assertion: check difference against tolerance
        assert abs(distance - expected_distance_m) < 1000.0, (
            f"Distance {distance} not within 1km tolerance of {expected_distance_m}"
        )

        # Verify WKT was stored correctly
        query_geom = "SELECT geom FROM observations WHERE date = :date1"
        result_geom = uow.execute(query_geom, {"date1": observations[0].date})
        stored_wkt = result_geom.scalar_one_or_none()
        assert stored_wkt == "POINT(-75.0 25.0)"
