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
    mock = mocker.patch("etl_pipeline.core.BaseConsole", autospec=True).return_value
    mock.progress = mocker.MagicMock()
    mock.create_task.return_value = TaskID(1)
    return mock
