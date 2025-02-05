"""Tests for ETL pipeline and progress tracking functionality."""

import logging
from datetime import datetime
from unittest.mock import MagicMock, patch


from hurdat2_etl.core import ETLPipeline, ETLStage
from hurdat2_etl.extract.types import StormStatus
from hurdat2_etl.models import Observation, Point
from hurdat2_etl.extract.extract import Extract
from hurdat2_etl.load.load import Load
from hurdat2_etl.main import parse_args, run_etl
from hurdat2_etl.models import Storm
from hurdat2_etl.transform.transform import Transform


class MockStage(ETLStage[str, str]):
    """Mock ETL stage for testing progress tracking."""

    def process(self, data: str) -> str:
        self.init_progress(3, "Processing")
        for _ in range(3):
            self.update_progress()
        self.close_progress()
        return data


def test_etl_stage_progress_tracking():
    """Test progress tracking in base ETLStage class."""
    with patch("hurdat2_etl.core.tqdm") as mock_tqdm:
        # Setup mock tqdm instance
        mock_progress = MagicMock()
        mock_tqdm.return_value = mock_progress

        # Test with progress enabled
        stage = MockStage(progress_enabled=True)
        stage.process("test")

        # Verify tqdm was initialized correctly
        mock_tqdm.assert_called_once_with(total=3, desc="Processing")
        assert mock_progress.update.call_count == 3
        mock_progress.close.assert_called_once()

        # Test with progress disabled
        mock_tqdm.reset_mock()
        stage = MockStage(progress_enabled=False)
        stage.process("test")

        # Verify tqdm was not used
        mock_tqdm.assert_not_called()


def test_extract_progress_tracking(tmp_path):
    """Test progress tracking in Extract stage."""
    # Create test data file
    test_file = tmp_path / "test.txt"
    test_file.write_text(
        "AL122007,              KAREN,     2,\n"
        "20070925, 0000,  , TD, 10.0N,  35.9W,  30, 1006,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0, -999\n"
        "20070925, 0600,  , TS, 10.3N,  37.0W,  35, 1005,   40,   30,    0,   40,    0,    0,    0,    0,    0,    0,    0,    0, -999\n"
    )

    with patch("hurdat2_etl.core.tqdm") as mock_tqdm:
        mock_progress = MagicMock()
        mock_tqdm.return_value = mock_progress

        extract = Extract(input_path=test_file, progress_enabled=True)
        list(extract.process(None))  # Materialize iterator

        # Verify progress tracking
        mock_tqdm.assert_called_once()
        assert mock_progress.update.call_count == 3  # Header + 2 observations
        mock_progress.close.assert_called_once()


def test_transform_progress_tracking():
    """Test progress tracking in Transform stage."""
    test_storms = [
        Storm(basin="AL", cyclone_number=1, year=2007, name="TEST1", observations=[]),
        Storm(basin="AL", cyclone_number=2, year=2007, name="TEST2", observations=[]),
    ]

    with patch("hurdat2_etl.core.tqdm") as mock_tqdm:
        mock_progress = MagicMock()
        mock_tqdm.return_value = mock_progress

        transform = Transform(progress_enabled=True)
        list(transform.process(iter(test_storms)))  # Materialize iterator

        # Verify progress tracking
        mock_tqdm.assert_called_once_with(total=2, desc="Transforming storms")
        assert mock_progress.update.call_count == 2
        mock_progress.close.assert_called_once()


def test_load_progress_tracking(tmp_path):
    """Test progress tracking in Load stage."""
    test_storms = [
        Storm(
            basin="AL",
            cyclone_number=1,
            year=2007,
            name="TEST1",
            observations=[
                Observation(
                    date=datetime(2007, 1, 1),
                    record_identifier="TEST1",
                    status=StormStatus.TROPICAL_STORM,
                    location=Point(latitude=25.0, longitude=-80.0),
                    max_wind=50,
                    min_pressure=995,
                )
            ],
        ),
        Storm(
            basin="AL",
            cyclone_number=2,
            year=2007,
            name="TEST2",
            observations=[
                Observation(
                    date=datetime(2007, 1, 1),
                    record_identifier="TEST2",
                    status=StormStatus.TROPICAL_STORM,
                    location=Point(latitude=26.0, longitude=-79.0),
                    max_wind=55,
                    min_pressure=990,
                )
            ],
        ),
    ]

    with patch("hurdat2_etl.core.tqdm") as mock_tqdm:
        mock_progress = MagicMock()
        mock_tqdm.return_value = mock_progress

        load = Load(db_path=tmp_path / "test.db", progress_enabled=True)
        load.process(iter(test_storms))

        # Verify progress tracking
        mock_tqdm.assert_called_with(total=2, desc="Loading storms")
        assert mock_progress.update.call_count == 2
        mock_progress.close.assert_called()


def test_pipeline_integration(tmp_path, caplog):
    """Test full pipeline integration with progress tracking."""
    caplog.set_level(logging.INFO)

    # Create test data file
    test_file = tmp_path / "test.txt"
    test_file.write_text(
        "AL122007,              KAREN,     1,\n"
        "20070925, 0000,  , TD, 10.0N,  35.9W,  30, 1006,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0, -999\n"
    )

    # Setup command line arguments
    with patch("sys.argv", ["prog", "--input", str(test_file), "--db", str(tmp_path / "test.db")]):
        args = parse_args()
        run_etl(args)

    # Verify pipeline execution
    assert "ETL pipeline completed successfully" in caplog.text


def test_progress_disabled(tmp_path):
    """Test pipeline with progress bars disabled."""
    with patch("hurdat2_etl.core.tqdm") as mock_tqdm:
        extract = Extract(input_path=tmp_path / "test.txt", progress_enabled=False)
        transform = Transform(progress_enabled=False)
        load = Load(db_path=tmp_path / "test.db", progress_enabled=False)

        pipeline = ETLPipeline([extract, transform, load])

        # Run pipeline (will fail due to missing file, but that's not what we're testing)
        try:
            pipeline.run(None)
        except Exception:
            pass

        # Verify tqdm was never called
        mock_tqdm.assert_not_called()


def test_command_line_progress_control(tmp_path):
    """Test command line control of progress bars."""
    test_args = [
        (["prog"], True),  # Default: progress enabled
        (["prog", "--no-progress"], False),  # Explicitly disabled
    ]

    for argv, expected_enabled in test_args:
        with patch("sys.argv", argv):
            args = parse_args()
            assert not args.no_progress == expected_enabled
