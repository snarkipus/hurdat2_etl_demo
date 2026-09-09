"""Exercise the shipped revisions against DuckDB, not a stamped ORM schema."""

import logging
from datetime import datetime
from unittest.mock import Mock

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, insert, text
from sqlalchemy.exc import IntegrityError

from etl_pipeline.load.models import Base, Observation, Storm
from etl_pipeline.migrations import initialize_database

INITIAL = "53dc9abc36a8"
HEAD = "6accd1b8062d"


@pytest.fixture
def engine(tmp_path):
    engine = create_engine(f"duckdb:///{tmp_path / 'migration.duckdb'}")
    try:
        yield engine
    finally:
        engine.dispose()


def migration_config(connection):
    config = Config()
    config.set_main_option("script_location", "etl_pipeline:migrations")
    config.attributes["connection"] = connection
    return config


def schema(connection):
    columns = connection.execute(
        text("""
        SELECT table_name, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name IN ('storms', 'observations')
        ORDER BY table_name, ordinal_position
    """)
    ).all()
    constraints = connection.execute(
        text("""
        SELECT table_name, constraint_type, constraint_column_names,
               expression, referenced_table, referenced_column_names
        FROM duckdb_constraints()
        WHERE table_name IN ('storms', 'observations')
        ORDER BY table_name, constraint_type, constraint_column_names, expression
    """)
    ).all()
    return columns, constraints


def test_fresh_head_matches_persistence_without_config_side_effects(
    engine, tmp_path, monkeypatch
):
    # The autouse fixture puts cwd outside the checkout, with no alembic.ini.
    forbidden_engine = Mock(side_effect=AssertionError("unexpected migration engine"))
    forbidden_logging = Mock(side_effect=AssertionError("logging reset"))
    monkeypatch.setattr("sqlalchemy.engine_from_config", forbidden_engine)
    monkeypatch.setattr("logging.config.fileConfig", forbidden_logging)
    logger = logging.getLogger("etl_pipeline.migration_test")
    handlers = list(logging.getLogger().handlers)
    with engine.begin() as connection:
        transaction = connection.get_transaction()
        assert transaction is not None
        initialize_database(connection)
        assert not connection.closed
        assert connection.get_transaction() is transaction
        assert transaction.is_active
        assert (
            connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            == HEAD
        )
        migrated_schema = schema(connection)
    assert logging.getLogger().handlers == handlers
    assert not logger.disabled
    forbidden_engine.assert_not_called()
    forbidden_logging.assert_not_called()
    assert not (tmp_path / "data" / "hurdat.duckdb").exists()

    reference = create_engine("duckdb:///:memory:")
    try:
        with reference.begin() as connection:
            Base.metadata.create_all(connection)
            assert migrated_schema == schema(connection)
    finally:
        reference.dispose()


def test_initial_to_head_rename_preserves_values(engine):
    with engine.begin() as connection:
        config = migration_config(connection)
        script = ScriptDirectory.from_config(config)
        assert [(r.revision, r.down_revision) for r in script.walk_revisions()] == [
            (HEAD, INITIAL),
            (INITIAL, None),
        ]
        command.upgrade(config, INITIAL)
        assert (
            connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            == INITIAL
        )
        connection.execute(insert(Storm), storm())
        connection.execute(
            text("""
            INSERT INTO observations
                (id, storm_id, date, record_identifier, status, location_wkt,
                 max_wind, max_wind_mph, min_pressure, ne34, se34, sw34, nw34)
            VALUES (1, 'AL012023', '2023-06-01 12:00:00', 'L', 'TS',
                    'POINT(-80.5 25.5)', 40, 46.0, 1002, 10, 20, 30, 40)
        """)
        )
        before = connection.execute(text("SELECT * FROM observations")).one()
    with engine.begin() as connection:
        initialize_database(connection)
        assert (
            connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            == HEAD
        )
        after = connection.execute(text("SELECT * FROM observations")).one()
        assert after == before
        assert after._mapping["geom"] == "POINT(-80.5 25.5)"
        assert "location_wkt" not in after._mapping
        assert after._mapping["date"] == datetime(2023, 6, 1, 12)
        assert after._mapping["ne50"] is None
        assert connection.execute(text("SELECT * FROM storms")).one() == (
            "AL012023",
            "AL",
            1,
            2023,
            "ARLENE",
        )


def test_supplied_connection_ignores_ini_logging_and_database(engine, tmp_path):
    with engine.begin() as connection:
        config = migration_config(connection)
        # A developer Config may carry an ini filename too. Neither its logging
        # configuration nor its default database should be used in this path.
        config.config_file_name = str(tmp_path / "nonexistent.ini")
        config.set_main_option("sqlalchemy.url", "duckdb:///data/hurdat.duckdb")
        command.upgrade(config, "head")
        assert not connection.closed
        assert (
            connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            == HEAD
        )
    assert not (tmp_path / "data").exists()


def storm():
    return dict(
        storm_id="AL012023", basin="AL", cyclone_number=1, year=2023, name="ARLENE"
    )


def observation():
    return dict(
        id=1,
        storm_id="AL012023",
        date=datetime(2023, 6, 1, 12),
        status="TS",
        geom="POINT(-80.5 25.5)",
    )


@pytest.mark.parametrize(
    "invalid, message",
    [
        ({"storm_id": "AL992023"}, "foreign key"),
        ({"geom": None}, "NOT NULL"),
        ({"date": None}, "NOT NULL"),
    ],
)
def test_required_values_and_relationship_enforced(engine, invalid, message):
    with engine.begin() as connection:
        initialize_database(connection)
        connection.execute(insert(Storm), storm())
    with pytest.raises(IntegrityError, match=message):
        with engine.begin() as connection:
            connection.execute(insert(Observation), observation() | invalid)


def test_optional_values_can_be_null(engine):
    with engine.begin() as connection:
        initialize_database(connection)
        connection.execute(insert(Storm), storm())
        connection.execute(insert(Observation), observation())
    with engine.connect() as connection:
        row = connection.execute(Observation.__table__.select()).one()._mapping
        for column in Observation.__table__.columns:
            if column.nullable:
                assert row[column.name] is None
