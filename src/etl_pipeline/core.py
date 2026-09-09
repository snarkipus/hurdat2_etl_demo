import logging
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
    Bind stage context without owning handlers or changing global logger levels.
    The CLI (or an embedding caller) owns diagnostic configuration.
    """

    def __init__(self, name: str, log_level: int = logging.INFO):
        """
        Initializes the logger.

        Args:
            name: The name of the logger (typically the stage name).
            log_level: Retained for stage-call compatibility; configured by the CLI.
        """
        self.name = name
        self.logger = logging.LoggerAdapter(
            logging.getLogger(f"etl_pipeline.{name}"), {"stage": name}, merge_extra=True
        )

    def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.logger.debug(message, *args, **kwargs)

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.logger.info(message, *args, **kwargs)

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.logger.warning(message, *args, **kwargs)

    def error(
        self, message: str, exc_info: bool = False, *args: Any, **kwargs: Any
    ) -> None:
        if exc_info:
            self.logger.error(message, *args, exc_info=True, **kwargs)
        else:
            self.logger.error(message, *args, **kwargs)

    def critical(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.logger.critical(message, *args, **kwargs)


class ProgressManager:
    """
    Centralized manager for all progress displays in the ETL pipeline.

    Provides a single consistent interface for creating and updating progress bars
    across all ETL stages, ensuring a coherent UI experience.
    """

    def __init__(self, console: Console | None = None):
        """
        Initialize the progress manager with a shared console.

        Args:
            console: An optional Rich Console to use. If None, creates a new one.
        """
        self.console = console or Console()
        self.progress = self._create_progress()
        self.active = False
        self.tasks: dict[str, TaskID] = {}

    def _create_progress(self) -> Progress:
        """Creates a consistent progress display configuration."""
        return Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description:<45}"),
            BarColumn(complete_style="bright_magenta", finished_style="bright_green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console,
            expand=True,
            refresh_per_second=15,
            auto_refresh=True,
        )

    def start(self) -> None:
        """Start the progress display if not already active."""
        if not self.active:
            # Use progress's live display property for checking
            if not hasattr(self.progress, "live") or not self.progress.live.is_started:
                self.progress.start()
            self.active = True

    def stop(self) -> None:
        """Stop the progress display if active."""
        if self.active:
            # Use progress's live display property for checking
            if hasattr(self.progress, "live") and self.progress.live.is_started:
                self.progress.stop()
            self.active = False

    def add_task(self, name: str, description: str, total: int | None = None) -> None:
        """
        Add a new task to the progress display.

        Args:
            name: A unique identifier for the task
            description: User-friendly description of the task
            total: The total steps for the task, or None for indeterminate
        """
        if name in self.tasks:
            return  # Avoid adding duplicate tasks

        task_id = self.progress.add_task(description, total=total)
        self.tasks[name] = task_id

    def update(
        self,
        name: str,
        advance: int = 1,
        completed: int | None = None,
        description: str | None = None,
    ) -> None:
        """
        Update a task's progress.

        Args:
            name: The task identifier
            advance: How much to advance the task
            completed: Directly set the completed status
            description: Update the task description
        """
        if name not in self.tasks:
            return

        # Build update kwargs to pass to progress
        update_kwargs: dict[str, Any] = {"advance": advance}
        if completed is not None:
            update_kwargs["completed"] = completed
        if description is not None:
            update_kwargs["description"] = description

        self.progress.update(self.tasks[name], **update_kwargs)

    def complete_task(self, name: str, description: str | None = None) -> None:
        """
        Mark a task as completed.

        Args:
            name: The task identifier
            description: Optional updated description for the completed task
        """
        if name not in self.tasks:
            return

        task_id = self.tasks[name]

        # Get the task's total value to ensure we set completed to 100%
        task = self.progress.tasks[task_id]
        total = task.total if task.total is not None else 0

        # Force completed to 100%
        if total > 0:
            # Update with direct completion value
            self.progress.update(task_id, completed=total, visible=True)
        else:
            # For indeterminate progress, just mark as completed=1 (100%)
            self.progress.update(task_id, completed=1, visible=True)

        # Update description if provided
        if description:
            self.progress.update(task_id, description=description)

    def remove_task(self, name: str) -> None:
        """
        Remove a task from the progress display.

        Args:
            name: The task identifier
        """
        if name not in self.tasks:
            return

        task_id = self.tasks[name]
        self.progress.remove_task(task_id)
        del self.tasks[name]


class ETLStage(ABC):
    """
    Abstract base class for all ETL pipeline stages (Extract, Transform, Load).

    Provides common functionality like logging and console/progress handling.
    Requires subclasses to implement the `_process` method.
    """

    def __init__(
        self,
        name: str,
        console: Console | None = None,
        progress_manager: ProgressManager | None = None,
        log_level: int = logging.INFO,
    ):
        """
        Initializes the ETL stage.

        Args:
            name: The name of the stage (e.g., "extract", "transform").
            console: An optional Rich Console instance to use.
            progress_manager: An optional shared progress manager.
            log_level: The logging level for the stage's logger.
        """
        self.name = name
        self.logger = BaseLogger(name, log_level)
        self.console = console or Console()
        self.progress_manager = progress_manager

    def print(self, message: str, **kwargs: Any) -> None:
        """Print a message to the console."""
        self.console.print(message, **kwargs)

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
