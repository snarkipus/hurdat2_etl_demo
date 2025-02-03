"""Core interfaces and base classes for ETL pipeline."""

from abc import ABC, abstractmethod
from typing import Any


class ETLStage(ABC):
    """Base class for ETL pipeline stages."""

    @abstractmethod
    def process(self, data: Any) -> Any:
        """Process input data and return transformed data."""
        pass


class ETLPipeline:
    """Pipeline for executing ETL stages."""

    def __init__(self, stages: list[ETLStage]):
        self.stages = stages

    def run(self, input_data: Any) -> Any:
        result = input_data
        for stage in self.stages:
            result = stage.process(result)
        return result
