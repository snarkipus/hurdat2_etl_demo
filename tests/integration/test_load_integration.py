"""Integration tests for the Load stage, focusing on database interactions."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

from etl_pipeline.exceptions import LoadError
from etl_pipeline.load.load import LoadStage
from etl_pipeline.load.unit_of_work import SqlAlchemyUnitOfWork
from etl_pipeline.migrations import initialize_database

# Import Pydantic models for creating test data
from etl_pipeline.transform.models import Observation as PydanticObservation
from etl_pipeline.transform.models import Point, StormStatus
from etl_pipeline.transform.models import Storm as PydanticStorm


# Use a function-scoped fixture for in-memory DB to ensure isolation
@pytest.fixture(scope="function")
def in_memory_db_engine():
    """Creates an in-memory DuckDB engine initialized by packaged migrations."""
    engine = create_engine("duckdb:///:memory:")

    try:
        with engine.begin() as connection:
            initialize_database(connection)
        yield engine
    finally:
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
        status=StormStatus.HURRICANE,
        location=Point(latitude=25.0, longitude=-75.0),
        max_wind=100,
        storm_id=storm1.storm_id,
    )
    obs2 = PydanticObservation(
        date=datetime(2024, 1, 1, 18, 0, 0),
        status=StormStatus.TROPICAL_STORM,
        location=Point(latitude=25.0, longitude=-76.0),
        max_wind=60,
        storm_id=storm1.storm_id,
    )

    # The load stage expects separate iterables
    return [storm1], [obs1, obs2]


@pytest.mark.parametrize("initialize_then_drop", [False, True])
def test_missing_schema_is_not_created_or_repaired(
    sample_integration_data, initialize_then_drop
):
    engine = create_engine("duckdb:///:memory:")
    try:
        if initialize_then_drop:
            with engine.begin() as connection:
                initialize_database(connection)
                connection.execute(text("DROP TABLE observations"))
        factory = sessionmaker(bind=engine)
        stage = LoadStage(uow_factory=lambda: SqlAlchemyUnitOfWork(factory))
        with pytest.raises(LoadError):
            stage.execute(sample_integration_data)
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM information_schema.tables "
                        "WHERE table_name = 'observations'"
                    )
                ).scalar_one()
                == 0
            )
    finally:
        engine.dispose()


def test_load_preserves_utc_in_non_utc_session(
    session_factory, integration_load_stage, integration_uow, sample_integration_data
):
    """Aware source instants must not be cast to session-local wall time."""

    @event.listens_for(session_factory, "after_begin")
    def set_non_utc_timezone(session, transaction, connection):
        connection.execute(text("SET TimeZone = 'America/New_York'"))
        assert (
            connection.execute(text("SELECT current_setting('TimeZone')")).scalar_one()
            == "America/New_York"
        )

    storms, observations = sample_integration_data
    observations = [
        obs.model_copy(update={"date": obs.date.replace(tzinfo=UTC)})
        for obs in observations
    ]
    assert integration_load_stage.execute((storms, observations)) == (1, 2)

    with integration_uow as uow:
        assert uow.execute(
            "SELECT date, typeof(date) FROM observations ORDER BY date"
        ).all() == [
            (datetime(2024, 1, 1, 12), "TIMESTAMP"),
            (datetime(2024, 1, 1, 18), "TIMESTAMP"),
        ]


def test_load_and_spatial_query(
    integration_load_stage, integration_uow, sample_integration_data
):
    """
    Test loading data and interpreting known longitude/latitude point ordinates.
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

        # WKT X/Y are longitude/latitude. No geodesic function (whose axis
        # convention may differ) is needed to establish these known points.
        query = """
        SELECT ST_X(ST_GeomFromText(geom)), ST_Y(ST_GeomFromText(geom))
        FROM observations ORDER BY date
        """
        assert uow.execute(query).all() == [(-75.0, 25.0), (-76.0, 25.0)]

        # Verify WKT was stored correctly
        query_geom = "SELECT geom FROM observations WHERE date = :date1"
        result_geom = uow.execute(query_geom, {"date1": observations[0].date})
        stored_wkt = result_geom.scalar_one_or_none()
        assert stored_wkt == "POINT(-75.0 25.0)"
