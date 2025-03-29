import logging
import os
from abc import ABC, abstractmethod
from typing import Any

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)


class BaseLogger:
    """Base class for file-based structured logs"""

    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    LOG_FILE = "logs/pipeline.log"

    def __init__(self, name: str, log_level: int = logging.INFO):
        # Create logs directory if it doesn't exist
        os.makedirs(os.path.dirname(self.LOG_FILE), exist_ok=True)

        self.name = name
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(log_level)

        # Ensure we don't add duplicate handlers if this class is initialized
        # multiple times
        if not any(isinstance(h, logging.FileHandler) for h in self.logger.handlers):
            # Create a file handler
            formatter = logging.Formatter(fmt=self.LOG_FORMAT, datefmt=self.DATE_FORMAT)
            file_handler = logging.FileHandler(self.LOG_FILE)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def debug(self, message: str) -> None:
        """Log a debug message."""
        self.logger.debug(message)

    def info(self, message: str) -> None:
        """Log an info message."""
        self.logger.info(message)

    def warning(self, message: str) -> None:
        """Log a warning message."""
        self.logger.warning(message)

    def error(self, message: str) -> None:
        """Log an error message."""
        self.logger.error(message)

    def critical(self, message: str) -> None:
        """Log a critical message."""
        self.logger.critical(message)


class BaseConsole:
    """Base class for Rich console functionality."""

    def __init__(self, console: Console | None = None):
        self.console = console or Console()
        self.progress = self._create_progress_bar()

    def _create_progress_bar(self) -> Progress:
        """Create a Rich progress bar with default formatting."""
        return Progress(
            SpinnerColumn(),
            TextColumn("[cyan]{task.description:<35}"),
            BarColumn(bar_width=80),
            TextColumn("{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console,
        )

    def print(self, *args: Any, **kwargs: Any) -> None:
        """Print to the console with Rich formatting."""
        self.console.print(*args, **kwargs)

    def update_progress(self, task_id: TaskID, advance: int = 1) -> None:
        """Update a task in the progress bar."""
        self.progress.update(task_id, advance=advance)

    def create_task(self, description: str, total: int | None = None) -> TaskID:
        """Create a new task in the progress bar."""
        # Rich Progress allows total=None for indeterminate tasks
        return self.progress.add_task(description, total=total)


class ETLStage(ABC):
    """Base class for ETL stages."""

    def __init__(
        self,
        name: str,
        console: Console | None = None,
        log_level: int = logging.INFO,
    ):
        self.name = name
        # Instantiate logger and console handler as attributes
        self.logger = BaseLogger(name, log_level)
        self.console_handler = BaseConsole(console)

    def execute(self, data: Any) -> Any:
        """Execute the ETL stage."""
        self.logger.info(f"Starting execution of stage: {self.name}")
        result = self._process(data)
        self.logger.info(f"Completed execution of stage: {self.name}")
        return result

    @abstractmethod
    def _process(self, data: Any) -> Any:
        """Internal processing method to be implemented by subclasses."""
        pass
