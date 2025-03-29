"""Unit tests for the SQLAlchemy repository implementation using pytest-mock."""

import pytest
from sqlalchemy.orm import Session

from etl_pipeline.load.models import Storm  # Import a concrete model for testing
from etl_pipeline.load.repository import SqlAlchemyRepository


@pytest.fixture
def mock_session(mocker):
    """Provides a mocked SQLAlchemy Session using pytest-mock."""
    # Use mocker.Mock for creating mocks consistent with pytest-mock
    return mocker.Mock(spec=Session)


@pytest.fixture
def storm_repository(mock_session):
    """Provides a SqlAlchemyRepository instance configured for the Storm model."""
    # Pass the mocked session to the repository
    return SqlAlchemyRepository(mock_session, Storm)


def test_repository_add(storm_repository, mock_session):
    """Test adding an entity via the repository adds it to the session."""
    # Create a real Storm instance for testing data structure
    mock_storm = Storm(
        storm_id="AL01TEST", basin="AL", cyclone_number=1, year=2024, name="TEST"
    )
    storm_repository.add(mock_storm)
    # Assert that the session's add method was called correctly
    mock_session.add.assert_called_once_with(mock_storm)


def test_repository_get(storm_repository, mock_session):
    """Test getting an entity via the repository calls session.get."""
    mock_storm_id = "AL01TEST"
    # Create a real Storm instance to be returned by the mock
    expected_storm = Storm(
        storm_id=mock_storm_id, basin="AL", cyclone_number=1, year=2024, name="TEST"
    )
    # Configure the mock session's get method
    mock_session.get.return_value = expected_storm

    retrieved_storm = storm_repository.get(mock_storm_id)

    # Assert that the session's get method was called with correct arguments
    mock_session.get.assert_called_once_with(Storm, mock_storm_id)
    # Assert that the correct object was returned
    assert retrieved_storm == expected_storm


def test_repository_get_not_found(storm_repository, mock_session):
    """Test getting a non-existent entity returns None."""
    mock_storm_id = "AL02NONEXIST"
    # Configure the mock session's get method to return None
    mock_session.get.return_value = None

    retrieved_storm = storm_repository.get(mock_storm_id)

    # Assert that the session's get method was called
    mock_session.get.assert_called_once_with(Storm, mock_storm_id)
    # Assert that None was returned
    assert retrieved_storm is None
