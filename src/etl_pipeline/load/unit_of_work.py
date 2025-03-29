"""
Defines the Unit of Work pattern for managing database transactions and repositories.

This pattern ensures atomic operations by grouping database interactions within a
single transaction, which is either committed upon successful completion or rolled
back in case of errors. It also provides a consistent interface for accessing
data repositories.
"""

import abc
import logging
from collections.abc import Mapping
from types import TracebackType
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Result
from sqlalchemy.orm import Session, sessionmaker

# Import Base from models to access metadata
from etl_pipeline.load.models import Base, Observation, Storm
from etl_pipeline.load.repository import SqlAlchemyRepository

logger = logging.getLogger(__name__)


class AbstractUnitOfWork(abc.ABC):
    """Abstract base class defining the Unit of Work interface."""

    storms: SqlAlchemyRepository[Storm]
    observations: SqlAlchemyRepository[Observation]

    def __enter__(self) -> "AbstractUnitOfWork":
        """Enter the runtime context related to this object."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,  # Use built-in type
        exc_val: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Exit the runtime context, rolling back or committing the transaction.

        Args:
            exc_type: The type of the exception raised, if any.
            exc_val: The exception instance raised, if any.
            traceback: The traceback object, if any.
        """
        if exc_type:
            logger.error(f"Rolling back transaction due to error: {exc_val}")
            self.rollback()
        else:
            self.commit()

    @abc.abstractmethod
    def commit(self) -> None:
        """Commits the current transaction."""
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self) -> None:
        """Rolls back the current transaction."""
        raise NotImplementedError

    @abc.abstractmethod
    def execute(
        self, statement: str, params: Mapping[str, Any] | None = None
    ) -> Result[Any]:
        """
        Executes a raw SQL statement within the current transaction.

        Args:
            statement: The SQL statement string to execute.
            params: A mapping of parameters to bind to the statement.

        Returns:
            A SQLAlchemy Result object.
        """
        raise NotImplementedError


# Default session factory configured for the DuckDB database
DEFAULT_SESSION_FACTORY = sessionmaker(
    bind=create_engine("duckdb:///data/hurdat.duckdb")
)


class SqlAlchemyUnitOfWork(AbstractUnitOfWork):
    """
    Concrete implementation of the Unit of Work pattern using SQLAlchemy.

    Manages a SQLAlchemy session and transaction lifecycle.
    """

    def __init__(
        self, session_factory: sessionmaker[Session] = DEFAULT_SESSION_FACTORY
    ):
        """
        Initializes the Unit of Work with a session factory.

        Args:
            session_factory: A SQLAlchemy sessionmaker instance.
        """
        self.session_factory = session_factory
        self.session: Session | None = None  # Session initialized in __enter__

    def __enter__(self) -> "SqlAlchemyUnitOfWork":
        """Start session, create schema if needed, init repositories."""
        self.session = self.session_factory()
        # Ensure the engine is available for create_all
        engine = self.session.bind
        if not isinstance(engine, Engine):
            # This case should ideally not happen if session_factory is
            # configured correctly
            raise TypeError("Session is not bound to a valid SQLAlchemy Engine.")

        # Create tables based on the metadata defined in models.py
        logger.debug("Ensuring database schema exists...")
        Base.metadata.create_all(engine)
        logger.debug("Database schema check complete.")

        self.storms = SqlAlchemyRepository(self.session, Storm)
        self.observations = SqlAlchemyRepository(self.session, Observation)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,  # Use built-in type
        exc_val: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Commits or rolls back the session and closes it."""
        if self.session:  # Ensure session was initialized
            super().__exit__(exc_type, exc_val, traceback)
            self.session.close()

    def commit(self) -> None:
        """Commits the current SQLAlchemy session."""
        if not self.session:
            raise RuntimeError("Session not initialized. Use within a 'with' block.")
        logger.debug("Committing transaction.")
        self.session.commit()

    def rollback(self) -> None:
        """Rolls back the current SQLAlchemy session."""
        if not self.session:
            raise RuntimeError("Session not initialized. Use within a 'with' block.")
        logger.debug("Rolling back transaction.")
        self.session.rollback()

    def execute(
        self, statement: str, params: Mapping[str, Any] | None = None
    ) -> Result[Any]:
        """Executes a raw SQL statement using the SQLAlchemy session."""
        if not self.session:
            raise RuntimeError("Session not initialized. Use within a 'with' block.")
        logger.debug(f"Executing statement: {statement} with params: {params}")
        return self.session.execute(text(statement), params=params)
