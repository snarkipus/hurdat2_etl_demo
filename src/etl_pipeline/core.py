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
    """
    Handles file-based structured logging for ETL stages.

    Configures a logger instance to write formatted messages to a specified log file.
    Ensures log directory exists and avoids duplicate handlers.
    """

    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    LOG_FILE = "logs/pipeline.log"

    def __init__(self, name: str, log_level: int = logging.INFO):
        """
        Initializes the logger.

        Args:
            name: The name of the logger (typically the stage name).
            log_level: The logging level (e.g., logging.INFO, logging.DEBUG).
        """
        # Create logs directory if it doesn't exist
        os.makedirs(os.path.dirname(self.LOG_FILE), exist_ok=True)

        self.name = name
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(log_level)

        # Ensure we don't add duplicate handlers if this class is initialized
        # multiple times (e.g., in tests or multiple stage instances)
        if not any(isinstance(h, logging.FileHandler) for h in self.logger.handlers):
            # Create and configure a file handler
            formatter = logging.Formatter(fmt=self.LOG_FORMAT, datefmt=self.DATE_FORMAT)
            file_handler = logging.FileHandler(self.LOG_FILE)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Logs a message with level DEBUG."""
        self.logger.debug(message, *args, **kwargs)

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Logs a message with level INFO."""
        self.logger.info(message, *args, **kwargs)

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Logs a message with level WARNING."""
        self.logger.warning(message, *args, **kwargs)

    def error(
        self, message: str, exc_info: bool = False, *args: Any, **kwargs: Any
    ) -> None:
        """
        Logs a message with level ERROR.

        Args:
            message: The message string to log.
            exc_info: If True, exception information is added to the log message.
            *args: Variable length argument list for string formatting.
            **kwargs: Arbitrary keyword arguments passed to the logger.
        """
        # Pass exc_info explicitly if True, otherwise rely on kwargs if passed
        # differently
        if exc_info:
            self.logger.error(message, *args, exc_info=True, **kwargs)
        else:
            self.logger.error(message, *args, **kwargs)

    def critical(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Logs a message with level CRITICAL."""
        self.logger.critical(message, *args, **kwargs)


class BaseConsole:
    """
    Provides Rich console and progress bar functionality for ETL stages.

    Manages a Rich Console instance and a pre-configured Progress bar.
    """

    def __init__(self, console: Console | None = None):
        """
        Initializes the console handler.

        Args:
            console: An optional existing Rich Console instance. If None,
                     a new one is created.
        """
        self.console = console or Console()
        self.progress = self._create_progress_bar()

    def _create_progress_bar(self) -> Progress:
        """Creates and configures a Rich Progress instance."""
        return Progress(
            SpinnerColumn(),
            TextColumn("[cyan]{task.description:<35}"),  # Task description
            BarColumn(bar_width=80),  # Progress bar
            TextColumn("{task.percentage:>3.0f}%"),  # Percentage complete
            TimeElapsedColumn(),  # Time elapsed
            console=self.console,  # Use the managed console
        )

    def print(self, *args: Any, **kwargs: Any) -> None:
        """Prints output to the Rich console."""
        self.console.print(*args, **kwargs)

    def update_progress(self, task_id: TaskID, advance: int = 1) -> None:
        """Updates the progress of a specific task in the progress bar."""
        self.progress.update(task_id, advance=advance)

    def create_task(self, description: str, total: int | None = None) -> TaskID:
        """
        Adds a new task to the progress bar.

        Args:
            description: Text description of the task.
            total: The total number of steps for the task. If None, the task
                   progress is indeterminate.

        Returns:
            The TaskID of the newly created task.
        """
        # Rich Progress allows total=None for indeterminate tasks
        return self.progress.add_task(description, total=total)


class ETLStage(ABC):
    """
    Abstract base class for all ETL pipeline stages (Extract, Transform, Load).

    Provides common functionality like logging and console/progress handling
    through composition of BaseLogger and BaseConsole. Requires subclasses
    to implement the `_process` method.
    """

    # Class-level type annotations for instance variables
    name: str
    logger: BaseLogger
    console_handler: BaseConsole

    def __init__(
        self,
        name: str,
        console: Console | None = None,
        log_level: int = logging.INFO,
    ):
        """
        Initializes the ETL stage.

        Args:
            name: The name of the stage (e.g., "extract", "transform").
            console: An optional Rich Console instance to use.
            log_level: The logging level for the stage's logger.
        """
        self.name = name
        # Instantiate logger and console handler
        self.logger = BaseLogger(name, log_level)
        self.console_handler = BaseConsole(console)

    def execute(self, data: Any) -> Any:
        """
        Public method to execute the ETL stage's processing logic.

        Logs the start and completion of the stage and calls the internal
        `_process` method.

        Args:
            data: Input data for the stage (type depends on the stage).

        Returns:
            The result of the stage's processing (type depends on the stage).
        """
        self.logger.info(f"Starting execution of stage: {self.name}")
        result = self._process(data)
        self.logger.info(f"Completed execution of stage: {self.name}")
        return result

    @abstractmethod
    def _process(self, data: Any) -> Any:
        """
        Abstract method for the core processing logic of the ETL stage.

        Subclasses must implement this method to define their specific
        data processing steps.

        Args:
            data: Input data for the stage.

        Returns:
            Processed data or result of the stage.
        """
        pass
