"""
Defines the Repository pattern for abstracting data access logic.

This module provides an abstract base class for repositories and a concrete
implementation using SQLAlchemy for interacting with the database session.
"""

import abc
from typing import Any, Generic, TypeVar

from sqlalchemy.orm import Session

from etl_pipeline.load.models import Base

# Generic Type Variable for SQLAlchemy models, bound to the Base model
ModelType = TypeVar("ModelType", bound=Base)


class AbstractRepository(abc.ABC, Generic[ModelType]):
    """
    Abstract base class defining the common interface for data repositories.

    This ensures that all repository implementations provide a consistent set of
    methods for basic CRUD operations.
    """

    def __init__(self, session: Session, model_class: type[ModelType]):
        """
        Initializes the repository with a database session and the model class.

        Args:
            session: The SQLAlchemy session instance.
            model_class: The SQLAlchemy model class this repository manages.
        """
        self.session = session
        self.model_class = model_class

    @abc.abstractmethod
    def add(self, entity: ModelType) -> None:
        """
        Marks an entity for addition to the database session.

        Args:
            entity: The model instance to add.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, entity_id: Any) -> ModelType | None:
        """
        Retrieves an entity by its primary key.

        Args:
            entity_id: The primary key value of the entity to retrieve.

        Returns:
            The retrieved model instance, or None if not found.
        """
        raise NotImplementedError

    # Consider adding other common repository methods like:
    # @abc.abstractmethod
    # def list(self, **filters) -> list[ModelType]:
    #     """Retrieves a list of entities based on optional filters."""
    #     raise NotImplementedError
    #
    # @abc.abstractmethod
    # def delete(self, entity: ModelType) -> None:
    #     """Marks an entity for deletion from the database session."""
    #     raise NotImplementedError


class SqlAlchemyRepository(AbstractRepository[ModelType]):
    """
    Concrete implementation of the Repository pattern using SQLAlchemy.

    Provides methods to interact with the database session for a specific model.
    """

    def add(self, entity: ModelType) -> None:
        """Adds a new entity to the SQLAlchemy session."""
        self.session.add(entity)

    def get(self, entity_id: Any) -> ModelType | None:
        """Retrieves an entity by its primary key using the SQLAlchemy session."""
        # Uses session.get for efficient primary key lookup
        return self.session.get(self.model_class, entity_id)

    # Implement other methods like list, delete if required by the application
