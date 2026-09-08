"""Initialize a database using the migrations shipped with this package."""

from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Connection


def initialize_database(connection: Connection) -> None:
    """Apply the existing chain to head on a caller-owned connection.

    The caller owns the connection and transaction (normally ``engine.begin()``).
    No repository configuration or application logging configuration is loaded.
    """
    config = Config()
    config.set_main_option("script_location", "etl_pipeline:migrations")
    config.attributes["connection"] = connection
    command.upgrade(config, "head")
