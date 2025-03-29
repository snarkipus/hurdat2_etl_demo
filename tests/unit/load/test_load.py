"""Unit tests for the Load stage implementation using pytest-mock."""

from collections.abc import Callable  # Import Callable for type hint

import pytest

from etl_pipeline.load.load import LoadStage
from etl_pipeline.load.unit_of_work import AbstractUnitOfWork

# Import Pydantic models for creating test data
from etl_pipeline.transform.models import Observation as PydanticObservation
from etl_pipeline.transform.models import Point  # Needed for Observation
from etl_pipeline.transform.models import Storm as PydanticStorm


@pytest.fixture
def mock_uow(mocker):
    """Provides a mocked AbstractUnitOfWork using pytest-mock."""
    uow = mocker.Mock(spec=AbstractUnitOfWork)
    uow.storms = mocker.Mock()
    uow.observations = mocker.Mock()
    uow.__enter__ = mocker.Mock(return_value=uow)
    uow.__exit__ = mocker.Mock(return_value=None)
    # Mock execute to return a mock result that supports scalar_one_or_none
    mock_result = mocker.Mock()
    mock_result.scalar_one_or_none.return_value = True  # Assume loaded by default
    uow.execute.return_value = mock_result
    return uow


@pytest.fixture
def mock_uow_factory(mocker, mock_uow) -> Callable[[], AbstractUnitOfWork]:
    """Provides a mocked factory that returns the mock_uow."""
    factory = mocker.Mock()
    factory.return_value = mock_uow
    return factory


@pytest.fixture
def load_stage(mock_uow_factory, mock_console, mocker):  # Use mock_console fixture
    """Provides a LoadStage instance with mocked dependencies."""
    stage = LoadStage(uow_factory=mock_uow_factory)
    stage.console_handler = mock_console
    # Mock the logger within the stage instance
    stage.logger = mocker.MagicMock()
    return stage


@pytest.fixture
def sample_transformed_data(mocker):
    """Provides sample transformed data (Pydantic models)."""
    storm1 = PydanticStorm(
        basin="AL", cyclone_number=1, year=2023, name="TestStorm1", observations=[]
    )
    # Use mocker.MagicMock for datetime objects in tests
    # Initialize observations WITH storm_id, assuming PydanticObservation model includes it
    obs1 = PydanticObservation(
        date=mocker.MagicMock(),
        status="HU",
        location=Point(latitude=25.0, longitude=-75.0),
        max_wind=100,
        storm_id=storm1.storm_id,
    )
    obs2 = PydanticObservation(
        date=mocker.MagicMock(),
        status="TS",
        location=Point(latitude=26.0, longitude=-76.0),
        max_wind=60,
        storm_id=storm1.storm_id,
    )
    storm1.observations = [obs1, obs2]  # Link for storm object if needed

    storm2 = PydanticStorm(
        basin="AL", cyclone_number=2, year=2023, name="TestStorm2", observations=[]
    )
    obs3 = PydanticObservation(
        date=mocker.MagicMock(),
        status="TD",
        location=Point(latitude=30.0, longitude=-80.0),
        max_wind=30,
        storm_id=storm2.storm_id,
    )
    storm2.observations = [obs3]

    all_storms = [storm1, storm2]
    # The load stage expects a flat list of all observations
    all_observations = [obs1, obs2, obs3]
    return all_storms, all_observations


def test_load_stage_process(load_stage, mock_uow, sample_transformed_data):
    """Test the main _process method of LoadStage."""
    storms, observations = sample_transformed_data
    input_data = (storms, observations)

    storm_count, obs_count = load_stage._process(input_data)

    # Verify spatial extension loading was attempted
    mock_uow.execute.assert_any_call(statement="INSTALL spatial;")
    mock_uow.execute.assert_any_call(statement="LOAD spatial;")

    # Verify storms were added
    assert mock_uow.storms.add.call_count == len(storms)
    first_storm_call_args = mock_uow.storms.add.call_args_list[0][0][0]
    assert first_storm_call_args.storm_id == storms[0].storm_id

    # Verify observations were added with correct geom WKT
    assert mock_uow.observations.add.call_count == len(observations)
    first_obs_call_args = mock_uow.observations.add.call_args_list[0][0][0]
    assert first_obs_call_args.max_wind == observations[0].max_wind
    assert (
        first_obs_call_args.geom
        == f"POINT({observations[0].location.longitude} {observations[0].location.latitude})"
    )
    third_obs_call_args = mock_uow.observations.add.call_args_list[2][0][0]
    assert (
        third_obs_call_args.geom
        == f"POINT({observations[2].location.longitude} {observations[2].location.latitude})"
    )

    assert storm_count == len(storms)
    assert obs_count == len(observations)
    # Check logger info calls (adjust based on actual logging)
    load_stage.logger.info.assert_any_call(
        "Starting data loading process via Unit of Work..."
    )
    load_stage.logger.info.assert_any_call(
        f"Added {len(storms)} storms to the session."
    )
    load_stage.logger.info.assert_any_call(
        f"Added {len(observations)} observations to the session."
    )


def test_load_stage_process_missing_lat_lon(load_stage, mock_uow, mocker):
    """Test handling of observations where location longitude is None."""
    mock_date = mocker.MagicMock()
    # Create a valid observation first
    valid_obs_with_loc = PydanticObservation(
        date=mock_date,
        status="HU",
        location=Point(latitude=25.0, longitude=-75.0),  # Valid Point
        max_wind=100,
        storm_id="AL01MISSING",
    )
    # Mock the longitude attribute access on this specific observation's location
    mocker.patch.object(valid_obs_with_loc.location, "longitude", None)

    input_data = ([], [valid_obs_with_loc])  # Pass the modified observation

    storm_count, obs_count = load_stage._process(input_data)

    assert mock_uow.observations.add.call_count == 1
    added_obs = mock_uow.observations.add.call_args[0][0]
    assert added_obs.geom is None  # Geom should be None

    # Verify warning was logged using the stage's logger mock
    load_stage.logger.warning.assert_called_once()
    assert "Missing lat/lon" in load_stage.logger.warning.call_args[0][0]

    assert storm_count == 0
    assert obs_count == 1


def test_load_stage_process_invalid_input_type(load_stage):
    """Test _process raises TypeError for invalid input data format."""
    with pytest.raises(TypeError, match="Load stage expects a tuple"):
        load_stage._process("not a tuple")

    with pytest.raises(TypeError, match="Load stage expects a tuple"):
        load_stage._process(([],))  # Tuple with only one element


def test_load_spatial_extension_success(load_stage, mock_uow):
    """Test spatial extension loading succeeds."""
    load_stage._load_spatial_extension(mock_uow)
    # Check execute was called for INSTALL and LOAD
    mock_uow.execute.assert_any_call(statement="INSTALL spatial;")
    mock_uow.execute.assert_any_call(statement="LOAD spatial;")


def test_load_spatial_extension_already_loaded(load_stage, mock_uow, mocker):
    """Test spatial extension check when install fails but extension is loaded."""
    # Simulate INSTALL failing, then check succeeding
    mock_result_loaded = mocker.Mock()
    mock_result_loaded.scalar_one_or_none.return_value = True
    mock_uow.execute.side_effect = [
        Exception("Install failed"),  # First call (INSTALL) raises error
        mock_result_loaded,  # Second call (check) returns loaded=True
    ]

    load_stage._load_spatial_extension(mock_uow)

    # Check INSTALL was called, then the SELECT query
    assert mock_uow.execute.call_count == 2
    assert mock_uow.execute.call_args_list[0][1]["statement"] == "INSTALL spatial;"
    assert (
        "SELECT loaded FROM duckdb_extensions"
        in mock_uow.execute.call_args_list[1][1]["statement"]
    )
    load_stage.logger.warning.assert_called_once()  # Check warning was logged
    load_stage.logger.info.assert_any_call("DuckDB spatial extension confirmed loaded.")


def test_load_spatial_extension_fails_altogether(load_stage, mock_uow, mocker):
    """Test spatial extension loading fails completely."""
    # Simulate INSTALL failing, then check also failing or returning not loaded
    mock_result_not_loaded = mocker.Mock()
    mock_result_not_loaded.scalar_one_or_none.return_value = False
    # Simulate the check query raising an exception after install fails
    check_exception = Exception("DB error during check")
    mock_uow.execute.side_effect = [
        Exception("Install failed"),  # First call (INSTALL) raises error
        check_exception,  # Second call (check) raises error
    ]

    # Expect the error raised after the check fails
    with pytest.raises(
        RuntimeError, match="Failed to verify DuckDB spatial extension status."
    ):
        load_stage._load_spatial_extension(mock_uow)

    # Assertions after the expected exception
    assert mock_uow.execute.call_count == 2
    load_stage.logger.warning.assert_called_once()  # Install warning
    # Check the specific error log related to the check failing
    load_stage.logger.error.assert_any_call(
        f"Failed to check if spatial extension is loaded: {check_exception}"
    )
