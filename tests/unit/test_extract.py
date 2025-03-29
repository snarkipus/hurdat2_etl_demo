import csv

import pytest

from etl_pipeline.exceptions import ExtractionError
from etl_pipeline.extract.extract import ExtractStage


@pytest.fixture
def extract_stage(mock_logger, mock_console):
    """Create an ExtractStage instance with mocked dependencies."""
    stage = ExtractStage()
    stage.logger = mock_logger
    stage.console_handler = mock_console
    return stage


@pytest.fixture
def mock_csv_data():
    """Provide sample CSV data for testing."""
    return (
        "AL092021,IDA,40,\n"
        "20210826,1200,,TD,16.5N,78.9W,30,1006,0,0,0,0,0,0,0,0,0,0,0,0,60\n"
        "20210826,1800,,TS,17.4N,79.5W,35,1006,60,0,0,0,0,0,0,0,0,0,0,0,50"
    )


@pytest.fixture
def mock_file_open(mocker, mock_csv_data):
    """Mock the open function to return test CSV data."""
    return mocker.patch("builtins.open", mocker.mock_open(read_data=mock_csv_data))


@pytest.fixture
def expected_csv_rows():
    """Provide expected parsed CSV rows for assertions."""
    return [
        ["AL092021", "IDA", "40", ""],
        [
            "20210826",
            "1200",
            "",
            "TD",
            "16.5N",
            "78.9W",
            "30",
            "1006",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "60",
        ],
        [
            "20210826",
            "1800",
            "",
            "TS",
            "17.4N",
            "79.5W",
            "35",
            "1006",
            "60",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "50",
        ],
    ]


class TestExtractStage:
    """Tests for the ExtractStage class."""

    def test_init(self):
        """Verify ExtractStage initializes with correct attributes."""
        stage = ExtractStage(name="test_extract")
        assert stage.name == "test_extract"
        assert stage._task_id is None

    def test_process_method(self, extract_stage, mock_file_open, expected_csv_rows):
        """Verify _process correctly parses CSV data."""
        input_data = {"file_path": "fake_path.csv"}
        results = list(extract_stage._process(input_data))

        assert len(results) == 3
        assert results == expected_csv_rows

    @pytest.mark.parametrize(
        "exception, error_msg, log_fragment",
        [
            (
                csv.Error("CSV parsing failed"),
                "Error parsing CSV file",
                "CSV parsing error",
            ),
            (
                OSError("OS error during read"),
                "IO error reading file",
                "IO error reading file",
            ),
        ],
    )
    def test_process_errors(
        self, extract_stage, mocker, exception, error_msg, log_fragment
    ):
        """Verify _process properly handles and reports CSV and IO errors."""
        mocker.patch("builtins.open", mocker.mock_open())
        mocker.patch("csv.reader", side_effect=exception)

        input_data = {"file_path": "fake_path.csv"}
        with pytest.raises(ExtractionError) as excinfo:
            list(extract_stage._process(input_data))

        assert error_msg in str(excinfo.value)
        extract_stage.logger.error.assert_called_once()
        assert log_fragment in extract_stage.logger.error.call_args[0][0]

    def test_execute_method(self, extract_stage, mocker):
        """Verify execute handles the full extraction workflow."""
        mocker.patch("builtins.open", mocker.mock_open())

        mock_data = [
            ["AL092021", "IDA", "40"],
            [
                "20210826",
                "1200",
                "",
                "TD",
                "16.5N",
                "78.9W",
                "30",
                "1006",
                "0",
                "0",
                "0",
                "0",
                "0",
                "0",
                "0",
                "0",
                "0",
                "0",
                "0",
                "0",
                "60",
            ],
        ]
        mock_process = mocker.patch(
            "etl_pipeline.extract.extract.ExtractStage._process",
            return_value=iter(mock_data),
        )

        input_data = {"file_path": "test.csv"}
        result = list(extract_stage.execute(input_data))

        assert result == mock_data
        extract_stage.logger.info.assert_any_call(
            "Starting execution of stage: extract"
        )
        extract_stage.logger.info.assert_any_call(
            "Attempting extraction from: test.csv"
        )
        extract_stage.console_handler.create_task.assert_called_once_with(
            description="Extracting data", total=None
        )
        mock_process.assert_called_once_with(input_data)
        extract_stage.console_handler.progress.stop_task.assert_called_once()

    def test_execute_file_access_error(self, extract_stage, mocker):
        """Verify execute handles file access errors."""
        mocker.patch("builtins.open", side_effect=FileNotFoundError("File not found"))

        input_data = {"file_path": "nonexistent.csv"}
        with pytest.raises(ExtractionError) as excinfo:
            list(extract_stage.execute(input_data))

        assert "Could not access file" in str(excinfo.value)
        extract_stage.logger.error.assert_called_once()
        assert "Error accessing file" in extract_stage.logger.error.call_args[0][0]

    def test_execute_process_exception(self, extract_stage, mocker):
        """Verify execute properly wraps unexpected exceptions."""
        mocker.patch("builtins.open", mocker.mock_open())
        mocker.patch(
            "etl_pipeline.extract.extract.ExtractStage._process",
            side_effect=ValueError("Process failed unexpectedly"),
        )

        input_data = {"file_path": "test.csv"}
        with pytest.raises(ExtractionError) as excinfo:
            list(extract_stage.execute(input_data))

        assert "Extraction failed unexpectedly" in str(excinfo.value)
        assert isinstance(excinfo.value.__cause__, ValueError)
        extract_stage.logger.error.assert_called_once()
        extract_stage.console_handler.progress.stop_task.assert_called_once()

    @pytest.mark.parametrize(
        "invalid_data", [None, "string", 123, [], {"wrong_key": "path"}]
    )
    def test_execute_invalid_input_type(self, extract_stage, invalid_data):
        """Verify execute validates input data structure."""
        with pytest.raises(TypeError) as excinfo:
            list(extract_stage.execute(invalid_data))
        assert "Input data must be a dict with a 'file_path' key" in str(excinfo.value)

    def test_execute_invalid_filepath_type(self, extract_stage):
        """Verify execute validates file_path is a string."""
        input_data = {"file_path": 123}
        with pytest.raises(ValueError) as excinfo:
            list(extract_stage.execute(input_data))
        assert "'file_path' must be a string" in str(excinfo.value)
