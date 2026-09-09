import csv

import pytest

from etl_pipeline.exceptions import ExtractionError
from etl_pipeline.extract.extract import ExtractStage


@pytest.fixture
def extract_stage(mock_logger, mock_console):
    """Create an ExtractStage instance with mocked dependencies."""
    stage = ExtractStage()
    stage.logger = mock_logger
    stage.progress_manager = mock_console
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
        # Note: _task_id removed from extract module
        assert hasattr(stage, "row_count")
        assert stage.row_count == 0

    def test_process_method(self, extract_stage, mock_file_open, expected_csv_rows):
        """Verify _process correctly parses CSV data."""
        input_data = {"file_path": "fake_path.csv"}
        results = list(extract_stage._process(input_data))
        assert results == expected_csv_rows
        extract_stage.logger.info.assert_any_call(
            "Starting extraction from: fake_path.csv"
        )
        extract_stage.logger.info.assert_any_call(
            "Extraction completed. 3 rows processed."
        )

    def test_process_csv_error(self, extract_stage, mocker):
        """Test error handling for CSV errors."""
        # Set up a more complex mock scenario that ensures exceptions occur in the right places
        mock_file = mocker.mock_open()
        mock_file_handle = mocker.MagicMock()
        mock_file.return_value.__enter__.return_value = mock_file_handle

        # Create a mock CSV reader that raises csv.Error
        mock_csv_reader = mocker.patch("csv.reader")
        mock_csv_reader.side_effect = csv.Error("CSV parsing error")

        # Set up the mock open function with our configured mock
        mocker.patch("builtins.open", mock_file)

        input_data = {"file_path": "fake_path.csv"}
        with pytest.raises(ExtractionError, match="near line unknown") as exc_info:
            list(extract_stage._process(input_data))
        assert exc_info.value.__cause__ is mock_csv_reader.side_effect

        # Verify error was logged
        extract_stage.logger.error.assert_called()

    def test_process_os_error(self, extract_stage, mocker):
        """Test error handling for OS errors."""
        # Mock open to raise an OSError
        mocker.patch("builtins.open", side_effect=OSError("Read error"))

        input_data = {"file_path": "fake_path.csv"}
        with pytest.raises(ExtractionError, match="Could not access source file"):
            list(extract_stage._process(input_data))

        # Verify error was logged
        extract_stage.logger.error.assert_called()

    def test_execute_method(self, extract_stage, mock_file_open, expected_csv_rows):
        """Verify execute method calls _process and logs properly."""
        input_data = {"file_path": "fake_path.csv"}
        results = list(extract_stage.execute(input_data))

        assert results == expected_csv_rows
        extract_stage.logger.info.assert_any_call(
            "Starting execution of stage: extract"
        )
        extract_stage.logger.info.assert_any_call(
            "Completed execution of stage: extract"
        )

        # Check progress manager was used
        if extract_stage.progress_manager:
            extract_stage.progress_manager.add_task.assert_called()

    def test_execute_file_access_error(self, extract_stage, mocker):
        """Test error handling when file access fails."""
        # Mock open to raise FileNotFoundError
        mocker.patch("builtins.open", side_effect=FileNotFoundError("File not found"))

        input_data = {"file_path": "fake_path.csv"}
        with pytest.raises(ExtractionError, match="Could not access source file"):
            list(extract_stage.execute(input_data))

        extract_stage.logger.error.assert_called()

    def test_execute_process_exception(self, extract_stage, mocker):
        """Test error propagation from _process."""
        process_error = ExtractionError("Processing error")
        mocker.patch.object(extract_stage, "_process", side_effect=process_error)

        input_data = {"file_path": "fake_path.csv"}
        with pytest.raises(ExtractionError, match="Processing error"):
            list(extract_stage.execute(input_data))

    @pytest.mark.parametrize(
        "invalid_data",
        [None, "string", 123, {"wrong_key": "value"}, {"file_path": 123}],
    )
    def test_execute_invalid_input_type(self, extract_stage, invalid_data):
        """Test error handling for invalid input types."""
        with pytest.raises((TypeError, ValueError)):
            list(extract_stage.execute(invalid_data))

    def test_execute_invalid_filepath_type(self, extract_stage):
        """Test error handling for invalid file_path type."""
        input_data = {"file_path": 123}  # Not a string
        with pytest.raises(ValueError, match="'file_path' value must be a string"):
            list(extract_stage.execute(input_data))

    def test_ordered_trimmed_fields(self, extract_stage, tmp_path):
        source = tmp_path / "source.txt"
        # Whitespace trimming is per field; CSV quoting uses the reader's
        # existing default (no whitespace before an opening quote).
        source.write_text(
            ' AL092021 , IDA , 40 , \n20210826, 1200 , , TD,"quoted, field" \n',
            encoding="utf-8",
        )
        assert list(extract_stage.execute({"file_path": str(source)})) == [
            ["AL092021", "IDA", "40", ""],
            ["20210826", "1200", "", "TD", "quoted, field"],
        ]

    def test_utf8_decoding_failure(self, extract_stage, tmp_path):
        source = tmp_path / "invalid-utf8.txt"
        source.write_bytes(b"AL092021,IDA,40,\n\xff\n")
        with pytest.raises(ExtractionError, match=str(source)) as exc_info:
            list(extract_stage.execute({"file_path": str(source)}))
        assert isinstance(exc_info.value.__cause__, UnicodeDecodeError)
        extract_stage.logger.error.assert_called()

    @pytest.mark.parametrize(
        "error, diagnostic",
        [
            (OSError("Read interrupted"), "OS error reading file"),
            (csv.Error("Invalid CSV"), "Error parsing CSV file"),
        ],
    )
    def test_failure_during_iteration(
        self, extract_stage, tmp_path, mocker, error, diagnostic
    ):
        source = tmp_path / "source.txt"
        source.write_text("AL092021,IDA,40,\n", encoding="utf-8")

        def interrupted_reader(_file):
            yield ["AL092021", "IDA", "40", ""]
            raise error

        reader = mocker.patch("csv.reader", side_effect=interrupted_reader)
        rows = extract_stage.execute({"file_path": str(source)})
        assert next(rows) == ["AL092021", "IDA", "40", ""]
        with pytest.raises(ExtractionError, match=diagnostic) as exc_info:
            next(rows)
        assert exc_info.value.__cause__ is error
        assert reader.call_args.args[0].closed
