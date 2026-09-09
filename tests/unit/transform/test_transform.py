"""Unit tests for the transformation stage orchestration."""

import pytest
from pydantic import ValidationError as PydanticValidationError

from etl_pipeline.core import ProgressManager
from etl_pipeline.exceptions import TransformError, ValidationError
from etl_pipeline.transform.models import Observation, Storm

# Removed unittest.mock import
from etl_pipeline.transform.transform import TransformStage


def test_group_progress_follows_actual_rows(mocker):
    manager = ProgressManager()
    stage = TransformStage(progress_manager=manager)
    rows = [SAMPLE_HEADER_1, *([SAMPLE_OBS_1_1] * 203), BLANK_ROW]
    checked = 0
    from etl_pipeline.transform.parser import is_header_line

    def check_row(row):
        nonlocal checked
        task = manager.progress.tasks[manager.tasks["transform_group"]]
        assert task.completed == (checked // 100) * 100
        assert not task.finished
        checked += 1
        return is_header_line(row)

    mocker.patch(
        "etl_pipeline.transform.transform.is_header_line", side_effect=check_row
    )
    updates = mocker.spy(manager, "update")
    assert len(stage.execute(rows)) == 1
    assert checked == 204
    group = manager.progress.tasks[manager.tasks["transform_group"]]
    assert group.completed == group.total == 205
    assert group.percentage == 100
    assert [
        call.kwargs["completed"]
        for call in updates.call_args_list
        if call.args[0] == "transform_group"
    ] == [100, 200, 205]


@pytest.mark.parametrize(("storm_count", "batch_size"), [(2, 1), (205, 2)])
def test_storm_progress_counts_attempts_including_rejections(
    mocker, storm_count, batch_size
):
    manager = ProgressManager()
    stage = TransformStage(progress_manager=manager)
    groups = [(SAMPLE_HEADER_1, [SAMPLE_OBS_1_1])] * storm_count
    mocker.patch.object(stage, "_group_into_storms", return_value=groups)
    create_storm = stage._create_storm
    attempted = 0

    def create(header, observations):
        nonlocal attempted
        task = manager.progress.tasks[manager.tasks["transform_storms"]]
        assert task.total == storm_count
        completed = attempted - attempted % batch_size
        assert task.completed == completed
        assert task.percentage == completed / storm_count * 100
        assert not task.finished
        attempted += 1
        if attempted == 1:
            raise TransformError("Rejected test storm")
        return create_storm(header, observations)

    mocker.patch.object(stage, "_create_storm", side_effect=create)
    assert len(stage.execute(RAW_DATA_VALID)) == 1  # Existing last-wins deduplication.
    assert attempted == storm_count
    task = manager.progress.tasks[manager.tasks["transform_storms"]]
    assert task.completed == task.total == storm_count
    assert task.description == f"Processed {storm_count - 1:,} valid storms"


def test_empty_transform_progress_has_zero_completed_rows():
    manager = ProgressManager()
    assert TransformStage(progress_manager=manager).execute([]) == []
    task = manager.progress.tasks[manager.tasks["transform_group"]]
    assert task.completed == task.total == 0
    assert "transform_storms" not in manager.tasks


# --- Sample Data ---
SAMPLE_HEADER_1 = ["AL011851", "UNNAMED", "14", ""]
SAMPLE_OBS_1_1 = [
    "18510625",
    "0000",
    " ",
    "HU",
    "28.0N",
    "94.8W",
    "80",
    "-999",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
]
SAMPLE_OBS_1_2 = [
    "18510625",
    "0600",
    " ",
    "HU",
    "28.0N",
    "95.4W",
    "80",
    "-999",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
]
INVALID_OBS_ROW = [
    "18510625",
    "1200",
    " ",
    "HU",
    "INVALID",
    "96.0W",
    "80",
    "-999",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
]  # Invalid Lat
INVALID_HEADER = ["XX01", "BADID", "10"]  # Invalid ID format

SAMPLE_HEADER_2 = ["AL021851", "STORM_TWO", "5", ""]
SAMPLE_OBS_2_1 = [
    "18510705",
    "1200",
    " ",
    "TS",
    "22.2N",
    "97.5W",
    "40",
    "-999",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
    " ",
]

EMPTY_ROW = ["", "", ""]
BLANK_ROW = [" ", " ", " "]

RAW_DATA_VALID = [
    SAMPLE_HEADER_1,
    SAMPLE_OBS_1_1,
    SAMPLE_OBS_1_2,
    SAMPLE_HEADER_2,
    SAMPLE_OBS_2_1,
]

RAW_DATA_WITH_INVALID_OBS = [
    SAMPLE_HEADER_1,
    SAMPLE_OBS_1_1,
    INVALID_OBS_ROW,
    SAMPLE_OBS_1_2,
]

RAW_DATA_WITH_INVALID_HEADER = [
    SAMPLE_HEADER_1,
    SAMPLE_OBS_1_1,
    INVALID_HEADER,
    SAMPLE_OBS_1_2,
]

RAW_DATA_WITH_BLANKS = [
    EMPTY_ROW,
    SAMPLE_HEADER_1,
    BLANK_ROW,
    SAMPLE_OBS_1_1,
    SAMPLE_HEADER_2,
    SAMPLE_OBS_2_1,
]

RAW_DATA_NO_OBS = [
    SAMPLE_HEADER_1,
    SAMPLE_HEADER_2,
    SAMPLE_OBS_2_1,
]

# --- Mock Parsed Data Structures (Values mocked inside tests) ---
MOCK_PARSED_HEADER_1_TUPLE = ("AL", 1, 1851, "UNNAMED")
MOCK_PARSED_OBS_1_1_DICT_STRUCTURE = {
    "date": None,
    "status": "HU",
    "location": {"latitude": "28.0N", "longitude": "94.8W"},
    "max_wind": 80,
    "min_pressure": None,
}
MOCK_PARSED_OBS_1_2_DICT_STRUCTURE = {
    "date": None,
    "status": "HU",
    "location": {"latitude": "28.0N", "longitude": "95.4W"},
    "max_wind": 80,
    "min_pressure": None,
}

MOCK_PARSED_HEADER_2_TUPLE = ("AL", 2, 1851, "STORM_TWO")
MOCK_PARSED_OBS_2_1_DICT_STRUCTURE = {
    "date": None,
    "status": "TS",
    "location": {"latitude": "22.2N", "longitude": "97.5W"},
    "max_wind": 40,
    "min_pressure": None,
}


# --- Fixtures ---


@pytest.fixture
def transform_stage(mocker):
    """Provides a TransformStage instance with mocked console/logger."""
    stage = TransformStage(name="test_transform")
    stage.logger = mocker.MagicMock()
    stage.progress_manager = mocker.MagicMock()
    return stage


# --- Test Cases ---


def test_group_into_storms_valid(transform_stage):
    """Test grouping valid raw data."""
    grouped = transform_stage._group_into_storms(RAW_DATA_VALID)
    assert len(grouped) == 2
    assert grouped[0] == (SAMPLE_HEADER_1, [SAMPLE_OBS_1_1, SAMPLE_OBS_1_2])
    assert grouped[1] == (SAMPLE_HEADER_2, [SAMPLE_OBS_2_1])


def test_group_into_storms_invalid_header(transform_stage):
    """Test grouping with an invalid header row."""
    grouped = transform_stage._group_into_storms(RAW_DATA_WITH_INVALID_HEADER)
    assert len(grouped) == 1
    assert grouped[0] == (
        SAMPLE_HEADER_1,
        [SAMPLE_OBS_1_1, INVALID_HEADER, SAMPLE_OBS_1_2],
    )
    transform_stage.logger.warning.assert_not_called()


def test_group_into_storms_blanks(transform_stage):
    """Test grouping with blank/empty rows."""
    grouped = transform_stage._group_into_storms(RAW_DATA_WITH_BLANKS)
    assert len(grouped) == 2
    assert grouped[0] == (SAMPLE_HEADER_1, [SAMPLE_OBS_1_1])  # Blank row skipped
    assert grouped[1] == (SAMPLE_HEADER_2, [SAMPLE_OBS_2_1])
    assert transform_stage.logger.debug.call_count >= 2


def test_group_into_storms_no_obs(transform_stage):
    """Test grouping where a header has no observations following it."""
    grouped = transform_stage._group_into_storms(RAW_DATA_NO_OBS)
    assert len(grouped) == 1
    assert grouped[0] == (SAMPLE_HEADER_2, [SAMPLE_OBS_2_1])
    # Match the updated warning message from the implementation
    transform_stage.logger.warning.assert_called_once_with(
        f"Header {SAMPLE_HEADER_1[0].strip()} found with no observations."
    )


def test_create_storm_valid(mocker, transform_stage):
    """Test creating a valid storm object."""
    mock_parse_header = mocker.patch(
        "etl_pipeline.transform.transform.parse_header_line"
    )
    mock_parse_track = mocker.patch("etl_pipeline.transform.transform.parse_track_line")
    # Patch the name as it's imported and used in transform.py
    mock_obs_model = mocker.patch(
        "etl_pipeline.transform.transform.PydanticObservation"
    )
    mock_storm_model = mocker.patch("etl_pipeline.transform.transform.PydanticStorm")

    mock_date_1 = mocker.MagicMock()
    mock_date_2 = mocker.MagicMock()
    mock_obs_1_data = {**MOCK_PARSED_OBS_1_1_DICT_STRUCTURE, "date": mock_date_1}
    mock_obs_2_data = {**MOCK_PARSED_OBS_1_2_DICT_STRUCTURE, "date": mock_date_2}

    mock_parse_header.return_value = MOCK_PARSED_HEADER_1_TUPLE
    mock_parse_track.side_effect = [mock_obs_1_data, mock_obs_2_data]

    mock_obs_instance_1 = mocker.MagicMock(spec=Observation)
    mock_obs_instance_2 = mocker.MagicMock(spec=Observation)
    mock_obs_model.side_effect = [mock_obs_instance_1, mock_obs_instance_2]

    mock_storm_instance = mocker.MagicMock(spec=Storm)
    mock_storm_model.return_value = mock_storm_instance

    result = transform_stage._create_storm(
        SAMPLE_HEADER_1, [SAMPLE_OBS_1_1, SAMPLE_OBS_1_2]
    )

    mock_parse_header.assert_called_once_with(SAMPLE_HEADER_1)
    assert mock_parse_track.call_count == 2
    assert mock_parse_track.call_args_list[0][0] == (SAMPLE_OBS_1_1,)
    assert mock_parse_track.call_args_list[1][0] == (SAMPLE_OBS_1_2,)

    assert mock_obs_model.call_count == 2
    assert mock_obs_model.call_args_list[0][1] == mock_obs_1_data
    assert mock_obs_model.call_args_list[1][1] == mock_obs_2_data

    mock_storm_model.assert_called_once_with(
        basin=MOCK_PARSED_HEADER_1_TUPLE[0],
        cyclone_number=MOCK_PARSED_HEADER_1_TUPLE[1],
        year=MOCK_PARSED_HEADER_1_TUPLE[2],
        name=MOCK_PARSED_HEADER_1_TUPLE[3],
        observations=[mock_obs_instance_1, mock_obs_instance_2],
    )
    assert result == mock_storm_instance


def test_create_storm_invalid_header(mocker, transform_stage):
    """Test storm creation fails if header parsing fails."""
    mock_parse_header = mocker.patch(
        "etl_pipeline.transform.transform.parse_header_line"
    )
    mock_parse_header.return_value = None

    with pytest.raises(TransformError, match="Invalid header format"):
        transform_stage._create_storm(INVALID_HEADER, [])
    mock_parse_header.assert_called_once_with(INVALID_HEADER)


def test_create_storm_no_valid_observations(mocker, transform_stage):
    """Test storm creation fails if no observations are valid."""
    mock_parse_header = mocker.patch(
        "etl_pipeline.transform.transform.parse_header_line"
    )
    mock_parse_track = mocker.patch("etl_pipeline.transform.transform.parse_track_line")

    mock_parse_header.return_value = MOCK_PARSED_HEADER_1_TUPLE
    mock_parse_track.return_value = None

    with pytest.raises(TransformError, match="No valid observations found"):
        transform_stage._create_storm(
            SAMPLE_HEADER_1, [INVALID_OBS_ROW, INVALID_OBS_ROW]
        )
    assert mock_parse_track.call_count == 2


def test_create_storm_skips_invalid_observation(mocker, transform_stage):
    """Test that invalid observations are skipped during storm creation."""
    mock_parse_header = mocker.patch(
        "etl_pipeline.transform.transform.parse_header_line"
    )
    mock_parse_track = mocker.patch("etl_pipeline.transform.transform.parse_track_line")
    # Patch the name as it's imported and used in transform.py
    mock_obs_model = mocker.patch(
        "etl_pipeline.transform.transform.PydanticObservation"
    )
    mock_storm_model = mocker.patch("etl_pipeline.transform.transform.PydanticStorm")

    mock_date_1 = mocker.MagicMock()
    mock_date_2 = mocker.MagicMock()
    mock_obs_1_data = {**MOCK_PARSED_OBS_1_1_DICT_STRUCTURE, "date": mock_date_1}
    mock_obs_2_data = {**MOCK_PARSED_OBS_1_2_DICT_STRUCTURE, "date": mock_date_2}

    mock_parse_header.return_value = MOCK_PARSED_HEADER_1_TUPLE
    mock_obs_instance = mocker.MagicMock(spec=Observation)
    mock_storm_instance = mocker.MagicMock(spec=Storm)

    mock_parse_track.side_effect = [mock_obs_1_data, mock_obs_2_data]
    mock_obs_model.side_effect = [
        mock_obs_instance,
        PydanticValidationError.from_exception_data("Mock validation error", []),
    ]
    mock_storm_model.return_value = mock_storm_instance

    def storm_side_effect(**kwargs):
        mock_storm_instance.observations = kwargs.get("observations", [])
        return mock_storm_instance

    mock_storm_model.side_effect = storm_side_effect

    result = transform_stage._create_storm(
        SAMPLE_HEADER_1, [SAMPLE_OBS_1_1, INVALID_OBS_ROW]
    )

    assert result == mock_storm_instance
    final_call_args = mock_storm_model.call_args[1]
    assert len(final_call_args["observations"]) == 1
    assert final_call_args["observations"][0] == mock_obs_instance
    transform_stage.logger.error.assert_called_once()
    assert "Skipping observation 2" in transform_stage.logger.error.call_args[0][0]


def test_create_observation_valid(mocker, transform_stage):
    """Test creating a valid observation."""
    mock_parse_track = mocker.patch("etl_pipeline.transform.transform.parse_track_line")
    # Patch the name as it's imported and used in transform.py
    mock_obs_model = mocker.patch(
        "etl_pipeline.transform.transform.PydanticObservation", wraps=Observation
    )
    mock_date = mocker.MagicMock()
    mock_obs_data = {**MOCK_PARSED_OBS_1_1_DICT_STRUCTURE, "date": mock_date}
    dummy_storm_id = "AL01TEST"

    mock_parse_track.return_value = mock_obs_data
    # Pass dummy storm_id
    obs = transform_stage._create_observation(SAMPLE_OBS_1_1, dummy_storm_id)

    mock_parse_track.assert_called_once_with(SAMPLE_OBS_1_1)
    # Check that storm_id was added before calling the model constructor
    expected_call_data = {**mock_obs_data, "storm_id": dummy_storm_id}
    mock_obs_model.assert_called_once_with(**expected_call_data)
    assert isinstance(obs, Observation)


def test_create_observation_parser_fails(mocker, transform_stage):
    """Test observation creation fails if parser returns None."""
    mock_parse_track = mocker.patch("etl_pipeline.transform.transform.parse_track_line")
    mock_parse_track.return_value = None
    dummy_storm_id = "AL01TEST"

    with pytest.raises(TransformError, match="Parser failed to extract essential data"):
        # Pass dummy storm_id
        transform_stage._create_observation(INVALID_OBS_ROW, dummy_storm_id)
    mock_parse_track.assert_called_once_with(INVALID_OBS_ROW)


def test_create_observation_validation_fails(mocker, transform_stage):
    """Test observation creation fails if Pydantic validation fails."""
    mock_parse_track = mocker.patch("etl_pipeline.transform.transform.parse_track_line")
    # Patch the name as it's imported and used in transform.py
    mock_obs_model = mocker.patch(
        "etl_pipeline.transform.transform.PydanticObservation",
        side_effect=PydanticValidationError.from_exception_data("mock error", []),
    )
    mock_date = mocker.MagicMock()
    mock_obs_data = {**MOCK_PARSED_OBS_1_1_DICT_STRUCTURE, "date": mock_date}

    mock_parse_track.return_value = mock_obs_data
    dummy_storm_id = "AL01TEST"

    with pytest.raises(ValidationError):
        # Pass dummy storm_id
        transform_stage._create_observation(SAMPLE_OBS_1_1, dummy_storm_id)

    mock_parse_track.assert_called_once_with(SAMPLE_OBS_1_1)
    # Check that storm_id was added before calling the model constructor
    expected_call_data = {**mock_obs_data, "storm_id": dummy_storm_id}
    mock_obs_model.assert_called_once_with(**expected_call_data)


def test_process_integration(mocker, transform_stage):
    """Test the main _process method integrates grouping and storm creation."""
    mock_group = mocker.patch.object(transform_stage, "_group_into_storms")
    mock_create = mocker.patch.object(transform_stage, "_create_storm")

    mock_storm_1 = mocker.MagicMock(spec=Storm, storm_id="AL011851")
    mock_storm_2 = mocker.MagicMock(spec=Storm, storm_id="AL021851")
    mock_group.return_value = [
        (SAMPLE_HEADER_1, [SAMPLE_OBS_1_1, SAMPLE_OBS_1_2]),
        (SAMPLE_HEADER_2, [SAMPLE_OBS_2_1]),
    ]
    mock_create.side_effect = [mock_storm_1, mock_storm_2]

    # Test the main _process method directly
    result = transform_stage._process(RAW_DATA_VALID)

    mock_group.assert_called_once_with(RAW_DATA_VALID)
    assert mock_create.call_count == 2
    assert mock_create.call_args_list[0][0] == (
        SAMPLE_HEADER_1,
        [SAMPLE_OBS_1_1, SAMPLE_OBS_1_2],
    )
    assert mock_create.call_args_list[1][0] == (SAMPLE_HEADER_2, [SAMPLE_OBS_2_1])

    assert result == [mock_storm_1, mock_storm_2]
    # Check progress manager was used if available
    if transform_stage.progress_manager:
        # Just make sure any progress management method was called
        assert transform_stage.progress_manager.mock_calls
    transform_stage.logger.info.assert_any_call(
        "Transformation complete: 2 unique storms processed."
    )


def test_process_integration_skips_errors(mocker, transform_stage):
    """Test that _process skips storms where _create_storm raises errors."""
    mock_group = mocker.patch.object(transform_stage, "_group_into_storms")
    mock_create = mocker.patch.object(transform_stage, "_create_storm")

    mock_storm_1 = mocker.MagicMock(spec=Storm, storm_id="AL011851")
    mock_group.return_value = [
        (SAMPLE_HEADER_1, [SAMPLE_OBS_1_1]),
        (SAMPLE_HEADER_2, [SAMPLE_OBS_2_1]),
    ]
    mock_create.side_effect = [mock_storm_1, TransformError("Failed to create storm 2")]

    result = transform_stage._process(RAW_DATA_VALID)

    assert mock_create.call_count == 2
    assert result == [mock_storm_1]
    transform_stage.logger.error.assert_called_once()
    # We don't use "Skipping Storm 2" pattern anymore, so just check for any error
    transform_stage.logger.error.assert_called()
    transform_stage.logger.info.assert_any_call(
        "Transformation complete: 1 unique storms processed."
    )
