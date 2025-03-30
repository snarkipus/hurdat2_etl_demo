import pytest
from rich.progress import TaskID


@pytest.fixture
def mock_logger(mocker):
    """Create a mock logger for testing."""
    mock = mocker.patch("etl_pipeline.core.BaseLogger", autospec=True).return_value
    return mock


@pytest.fixture
def mock_console(mocker):
    """Create a mock console handler for testing."""
    # We're using ProgressManager instead of BaseConsole now
    mock = mocker.patch("etl_pipeline.core.ProgressManager", autospec=True).return_value
    mock.progress = mocker.MagicMock()

    # Add task dictionary to match our implementation
    mock.tasks = {
        "extract_rows": TaskID(1),
        "transform_group": TaskID(2),
        "transform_storms": TaskID(3),
        "db_setup": TaskID(4),
        "load_storms": TaskID(5),
        "load_observations": TaskID(6),
        "commit": TaskID(7),
    }

    # Add methods used in tests
    mock.add_task.return_value = TaskID(8)
    mock.update = mocker.MagicMock()
    mock.complete_task = mocker.MagicMock()

    return mock
