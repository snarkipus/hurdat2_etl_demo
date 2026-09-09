"""Verification uses migrated, committed storage, never loader counters."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

from etl_pipeline.exceptions import ETLError
from etl_pipeline.load.load import LoadStage
from etl_pipeline.load.unit_of_work import SqlAlchemyUnitOfWork
from etl_pipeline.load.verification import verify_persisted_dataset
from etl_pipeline.migrations import initialize_database
from etl_pipeline.transform.models import Observation, Point, Storm, StormStatus


@pytest.fixture
def committed_engine(tmp_path):
    engine = create_engine(f"duckdb:///{tmp_path / 'verification.duckdb'}")
    try:
        with engine.begin() as connection:
            initialize_database(connection)
        factory = sessionmaker(bind=engine)
        storm = Storm(
            basin="AL", cyclone_number=1, year=2024, name="VERIFY", observations=[]
        )
        observations = [
            Observation(
                storm_id=storm.storm_id,
                date=datetime(2024, 1, 1, hour, tzinfo=UTC),
                status=StormStatus.TROPICAL_STORM,
                max_wind=40,
                location=Point(longitude=-75, latitude=25),
            )
            for hour in (0, 6)
        ]
        LoadStage(uow_factory=lambda: SqlAlchemyUnitOfWork(factory)).execute(
            ([storm], observations)
        )
        # Force the verifier to open a new physical connection to committed data.
        engine.dispose()
        yield engine
    finally:
        engine.dispose()


@pytest.mark.parametrize("expected", [(1, 2), (0, 2), (1, 3)])
def test_committed_counts(committed_engine, expected):
    if expected == (1, 2):
        verify_persisted_dataset(committed_engine, *expected)
    else:
        with pytest.raises(ETLError, match="Persisted count mismatch") as failure:
            verify_persisted_dataset(committed_engine, *expected)
        assert f"storms expected={expected[0]}, actual=1" in str(failure.value)
        assert f"observations expected={expected[1]}, actual=2" in str(failure.value)


def test_empty_committed_dataset(committed_engine):
    with committed_engine.begin() as connection:
        connection.execute(text("DELETE FROM observations"))
    with committed_engine.begin() as connection:
        connection.execute(text("DELETE FROM storms"))
    verify_persisted_dataset(committed_engine, 0, 0)


@pytest.mark.parametrize(
    ("wkt", "valid"),
    [
        ("POINT(-75 25)", True),
        ("POINT(-180 -90)", True),
        ("POINT(180 90)", True),
        ("POINT(270 25)", False),
        ("POINT(0 91)", False),
        ("POINT(NaN 25)", False),
        ("POINT(0 Infinity)", False),
        ("POINT EMPTY", False),
        ("LINESTRING(0 0, 1 1)", False),
        ("POINT(25)", False),
        ("not WKT", False),
    ],
)
def test_all_rows_point_acceptance_without_repair(committed_engine, wkt, valid):
    if wkt == "POINT(270 25)":
        assert Point.convert_hurdat2_coordinates("270E", is_latitude=False) == 270.0
    # The first row stays valid: a sample-row check would miss the invalid second.
    with committed_engine.begin() as connection:
        connection.execute(
            text("UPDATE observations SET geom = :wkt WHERE id = 2"),
            {"wkt": wkt},
        )
    if valid:
        verify_persisted_dataset(committed_engine, 1, 2)
    else:
        with pytest.raises(ETLError, match="point geometry/coordinate"):
            verify_persisted_dataset(committed_engine, 1, 2)
    with committed_engine.connect() as connection:
        assert connection.execute(
            text("SELECT geom FROM observations ORDER BY id")
        ).scalars().all() == ["POINT(-75.0 25.0)", wkt]


@pytest.mark.parametrize("operation", ["counts", "extension", "geometry", "empty"])
def test_required_query_errors_fail_closed(committed_engine, operation):
    if operation == "empty":
        with committed_engine.begin() as connection:
            connection.execute(text("DELETE FROM observations"))

    def fail_query(conn, cursor, statement, parameters, context, executemany):
        target = {
            "counts": "SELECT (SELECT count(*)",
            "extension": "LOAD spatial",
            "geometry": "WITH geometries",
            "empty": "WITH geometries",
        }[operation]
        if target in statement:
            raise RuntimeError("injected query failure")

    event.listen(committed_engine, "before_cursor_execute", fail_query)
    with pytest.raises(ETLError, match="Persisted verification .* failed") as failure:
        verify_persisted_dataset(committed_engine, 1, 0 if operation == "empty" else 2)
    assert isinstance(failure.value.__cause__, RuntimeError)
