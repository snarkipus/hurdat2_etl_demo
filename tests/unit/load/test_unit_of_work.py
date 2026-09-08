"""Unit tests for the SQLAlchemy Unit of Work implementation using pytest-mock."""

import pytest
from sqlalchemy.engine import Engine, Result  # Import Engine

# Removed unittest.mock import, will use mocker fixture
from sqlalchemy.orm import Session, sessionmaker

from etl_pipeline.load.models import Observation, Storm
from etl_pipeline.load.repository import SqlAlchemyRepository
from etl_pipeline.load.unit_of_work import SqlAlchemyUnitOfWork


@pytest.fixture
def mock_session(mocker):
    """Provides a mocked SQLAlchemy Session using pytest-mock."""
    session = mocker.Mock(spec=Session)
    # Configure the mock session to have a 'bind' attribute which is a mock Engine
    session.bind = mocker.Mock(spec=Engine)
    return session


@pytest.fixture
def mock_session_factory(mocker, mock_session):
    """Provides a mocked sessionmaker that returns the mock_session."""
    factory = mocker.Mock(spec=sessionmaker)
    factory.return_value = mock_session
    return factory


@pytest.fixture
def uow(mock_session_factory):
    """Provides a SqlAlchemyUnitOfWork instance with a mocked factory."""
    return SqlAlchemyUnitOfWork(session_factory=mock_session_factory)


def test_uow_enter_initializes_session_and_repositories(
    uow, mock_session_factory, mock_session
):
    """Test __enter__ creates session and repositories."""
    with uow:
        mock_session_factory.assert_called_once()
        assert uow.session is mock_session
        assert isinstance(uow.storms, SqlAlchemyRepository)
        assert uow.storms.session is mock_session
        assert uow.storms.model_class is Storm
        assert isinstance(uow.observations, SqlAlchemyRepository)
        assert uow.observations.session is mock_session
        assert uow.observations.model_class is Observation


def test_uow_exit_commits_on_success(uow, mock_session, mocker):  # Add mocker fixture
    """Test __exit__ calls commit on the session if no exception occurred."""
    with uow:
        # Use mocker.MagicMock for creating arbitrary objects if needed inside test
        uow.storms.add(
            Storm(storm_id="S1", basin="AL", cyclone_number=1, year=2024, name="Test")
        )

    mock_session.commit.assert_called_once()
    mock_session.rollback.assert_not_called()
    mock_session.close.assert_called_once()


def test_uow_exit_rolls_back_on_exception(
    uow, mock_session, mocker
):  # Add mocker fixture
    """Test __exit__ calls rollback on the session if an exception occurs."""
    with pytest.raises(ValueError, match="Something went wrong"):
        with uow:
            # Use mocker.MagicMock for creating arbitrary objects if needed inside test
            mock_date = mocker.MagicMock()
            uow.observations.add(
                Observation(
                    storm_id="S1", date=mock_date, status="HU", geom="POINT(1 1)"
                )
            )
            raise ValueError("Something went wrong")

    mock_session.commit.assert_not_called()
    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()


def test_uow_commit(uow, mock_session):
    """Test explicit commit call."""
    with uow:
        uow.commit()
    # Commit is called explicitly, and then again by __exit__
    assert mock_session.commit.call_count >= 1
    mock_session.rollback.assert_not_called()


def test_uow_rollback(uow, mock_session):
    """Test explicit rollback call."""
    with uow:
        uow.rollback()
    # Rollback called explicitly. __exit__ will still run but might not commit.
    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()


def test_uow_execute(uow, mock_session, mocker):
    """Test execute method calls session.execute with text()."""
    mock_result = mocker.Mock(spec=Result)
    mock_session.execute.return_value = mock_result
    statement = "SELECT 1"
    params = {"p1": "value"}

    with uow:
        result = uow.execute(statement, params)

    # Check session.execute was called correctly with text()
    assert mock_session.execute.call_count == 1
    call_args = mock_session.execute.call_args
    # Check the first positional argument is a TextClause (or similar)
    # and its string representation matches the statement
    assert hasattr(call_args[0][0], "text") and str(call_args[0][0]) == statement
    assert call_args[1]["params"] == params
    assert result is mock_result


def test_uow_raises_if_used_outside_context(mock_session_factory):
    """Test accessing session properties outside 'with' raises error."""
    uow_instance = SqlAlchemyUnitOfWork(session_factory=mock_session_factory)
    with pytest.raises(RuntimeError, match="Session not initialized"):
        uow_instance.commit()
    with pytest.raises(RuntimeError, match="Session not initialized"):
        uow_instance.rollback()
    with pytest.raises(RuntimeError, match="Session not initialized"):
        uow_instance.execute("SELECT 1")


def test_partial_entry_releases_session(uow, mock_session, mocker):
    error = RuntimeError("repository initialization failed")
    mocker.patch(
        "etl_pipeline.load.unit_of_work.SqlAlchemyRepository", side_effect=error
    )
    with pytest.raises(RuntimeError) as raised:
        with uow:
            pytest.fail("entry must fail")
    assert raised.value is error
    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()
    mock_session.bind.dispose.assert_not_called()
    assert uow.session is None


def test_invalid_bind_releases_session(uow, mock_session):
    mock_session.bind = None
    with pytest.raises(TypeError, match="not bound"):
        with uow:
            pytest.fail("entry must fail")
    mock_session.close.assert_called_once()


def test_session_creation_failure_has_nothing_to_close(uow, mock_session_factory):
    mock_session_factory.side_effect = RuntimeError("session creation failed")
    with pytest.raises(RuntimeError, match="session creation failed"):
        with uow:
            pytest.fail("entry must fail")
    assert uow.session is None


@pytest.mark.parametrize("failure_at", ["body", "commit"])
def test_primary_failure_survives_rollback_and_close_errors(
    uow, mock_session, failure_at
):
    primary = RuntimeError(f"{failure_at} failed")
    mock_session.rollback.side_effect = RuntimeError("rollback failed")
    mock_session.close.side_effect = RuntimeError("close failed")
    if failure_at == "commit":
        mock_session.commit.side_effect = primary
    with pytest.raises(RuntimeError) as raised:
        with uow:
            if failure_at == "body":
                raise primary
    assert raised.value is primary
    assert primary.__notes__ == [
        "Session rollback failed: rollback failed",
        "Session close failed: close failed",
    ]
    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()
    assert uow.session is None


def test_close_failure_after_commit_propagates(uow, mock_session):
    mock_session.close.side_effect = RuntimeError("close failed")
    with pytest.raises(RuntimeError, match="close failed"):
        with uow:
            pass
    mock_session.commit.assert_called_once()
    mock_session.rollback.assert_not_called()
    assert uow.session is None
