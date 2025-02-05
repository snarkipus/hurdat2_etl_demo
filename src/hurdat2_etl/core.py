"""Core interfaces and base classes for ETL pipeline."""

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from tqdm import tqdm

from .exceptions import ProgressError

T = TypeVar("T")
U = TypeVar("U")


class ETLStage(Generic[T, U], ABC):
    """Base class for ETL pipeline stages with progress tracking.

    Args:
        progress_enabled: Whether to enable progress tracking for this stage.

    Attributes:
        progress_enabled: Flag indicating if progress tracking is enabled.
        _progress_bar: Internal tqdm progress bar instance.

    Raises:
        ProgressError: If progress tracking operations fail.
    """

    def __init__(self, progress_enabled: bool = True) -> None:
        self.progress_enabled = progress_enabled
        self._progress_bar: tqdm[Any] | None = None

    def init_progress(self, total: int, desc: str) -> None:
        """Initialize progress bar if enabled.

        Args:
            total: Total number of items to process.
            desc: Description for the progress bar.

        Raises:
            ProgressError: If progress bar initialization fails.
            ValueError: If total is negative.
        """
        if total < 0:
            raise ValueError("Total must be non-negative")

        if self.progress_enabled:
            try:
                self._progress_bar = tqdm(total=total, desc=desc)  # type: ignore
            except Exception as e:
                raise ProgressError(f"Failed to initialize progress bar: {e!s}") from e

    def update_progress(self, n: int = 1) -> None:
        """Update progress by n steps.

        Args:
            n: Number of steps to increment progress by.

        Raises:
            ProgressError: If progress update fails.
            ValueError: If n is negative.
        """
        if n < 0:
            raise ValueError("Progress update value must be non-negative")

        if self._progress_bar:
            try:
                self._progress_bar.update(n)
            except Exception as e:
                raise ProgressError(f"Failed to update progress: {e!s}") from e

    def close_progress(self) -> None:
        """Close progress bar and clean up resources.

        This method should be called in a finally block to ensure proper cleanup.

        Raises:
            ProgressError: If progress bar cleanup fails.
        """
        if self._progress_bar:
            try:
                self._progress_bar.close()
            except Exception as e:
                raise ProgressError(f"Failed to close progress bar: {e!s}") from e
            finally:
                self._progress_bar = None

    @abstractmethod
    def process(self, data: T) -> U:
        """Process input data and return transformed data.

        Args:
            data: Input data of type T to process.

        Returns:
            Processed data of type U.

        Raises:
            ETLError: If processing fails.
            ProgressError: If progress tracking fails.
        """
        pass


class ETLPipeline:
    """Pipeline for executing ETL stages.

    Args:
        stages: List of ETL stages to execute in sequence.
    """

    def __init__(self, stages: list[ETLStage[Any, Any]]) -> None:
        if not stages:
            raise ValueError("Pipeline must contain at least one stage")
        self.stages = stages

    def run(self, input_data: Any) -> Any:
        """Execute all stages in the pipeline sequentially.

        Args:
            input_data: Initial input data for the pipeline.

        Returns:
            Final processed data after all stages complete.

        Raises:
            ETLError: If any stage fails.
            ProgressError: If progress tracking fails.
        """
        result = input_data
        for stage in self.stages:
            result = stage.process(result)
        return result
