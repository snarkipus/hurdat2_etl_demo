"""Transform module containing data transformation logic."""

from ..models import Observation, Storm


def transform_data(storm: Storm) -> Storm:
    """Perform basic data transformation."""
    # Currently a stub - to be expanded with actual transformation logic
    return storm


def normalize_data(observation: Observation) -> Observation:
    """Normalize observation data."""
    # Currently a stub - to be expanded with actual normalization logic
    return observation
