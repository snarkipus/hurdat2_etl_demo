"""Transform module containing data transformation logic."""

import logging
from collections.abc import Iterator

from ..core import ETLStage
from ..exceptions import TransformError
from ..models import Observation, Storm

logger = logging.getLogger(__name__)


def transform_data(storm: Storm) -> Storm:
    """Perform basic data transformation.

    Args:
        storm: Storm object to transform.

    Returns:
        Transformed Storm object.
    """
    # Currently a stub - to be expanded with actual transformation logic
    return storm


def normalize_data(observation: Observation) -> Observation:
    """Normalize observation data.

    Args:
        observation: Observation object to normalize.

    Returns:
        Normalized Observation object.
    """
    # Currently a stub - to be expanded with actual normalization logic
    return observation


class Transform(ETLStage[Iterator[Storm], Iterator[Storm]]):
    """Transform stage for HURDAT2 data.

    This stage applies data transformations to Storm objects while tracking progress.

    Args:
        progress_enabled: Whether to enable progress tracking.
    """

    def __init__(self, progress_enabled: bool = True) -> None:
        super().__init__(progress_enabled=progress_enabled)

    def process(self, data: Iterator[Storm]) -> Iterator[Storm]:
        """Process and transform Storm objects with progress tracking.

        Args:
            data: Iterator of Storm objects to transform.

        Returns:
            Iterator of transformed Storm objects.

        Raises:
            TransformError: If transformation fails.
        """
        logger.info("Starting data transformation")

        try:
            # Materialize iterator for progress tracking
            storms = list(data)
            self.init_progress(len(storms), "Transforming storms")

            try:
                for storm in storms:
                    try:
                        transformed_storm = transform_data(storm)
                        # Transform observations
                        transformed_storm.observations = [
                            normalize_data(obs) for obs in storm.observations
                        ]
                        self.update_progress()
                        yield transformed_storm

                    except Exception as e:
                        logger.error(
                            f"Failed to transform storm {storm.storm_id}: {e!s}"
                        )
                        raise TransformError(
                            f"Failed to transform storm {storm.storm_id}: {e!s}"
                        ) from e

            finally:
                self.close_progress()

        except Exception as e:
            logger.error(f"Transformation failed: {e!s}")
            raise TransformError(f"Failed to transform data: {e!s}") from e
