# TQDM Progress Bar Integration Plan

## Overview

This document outlines the plan for integrating tqdm progress bar functionality from the original `etl_script.py` into the refactored ETL pipeline. The goal is to provide visual feedback during the ETL process while maintaining clean separation of concerns.

## Architectural Decisions

### 1. Progress Tracking as Cross-Cutting Concern

The progress tracking functionality should be implemented at the ETLStage level since:
- It applies across all stages (Extract, Transform, Load)
- Each stage needs consistent progress reporting
- Progress tracking is orthogonal to the business logic
- ETLStage base class can provide a common interface

### 2. Separation of Concerns

- Parser module (`parser.py`) should focus solely on parsing logic
- Progress tracking should be handled by the stage classes
- Progress bars should be configurable (enabled/disabled)
- Each stage should handle its own progress tracking initialization

## Implementation Strategy

### 1. Core Changes

#### ETLStage Base Class

```python
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional
from tqdm import tqdm

T = TypeVar('T')
U = TypeVar('U')

class ETLStage(Generic[T, U], ABC):
    """Base class for ETL pipeline stages with progress tracking."""

    def __init__(self, progress_enabled: bool = True):
        self.progress_enabled = progress_enabled
        self._progress_bar: Optional[tqdm] = None

    def init_progress(self, total: int, desc: str) -> None:
        """Initialize progress bar if enabled."""
        if self.progress_enabled:
            self._progress_bar = tqdm(total=total, desc=desc)

    def update_progress(self, n: int = 1) -> None:
        """Update progress by n steps."""
        if self._progress_bar:
            self._progress_bar.update(n)

    def close_progress(self) -> None:
        """Close progress bar."""
        if self._progress_bar:
            self._progress_bar.close()
```

### 2. Extract Stage Changes

#### Extract Class

Modify to handle progress tracking:
- Add line counting for initialization
- Update progress during storm processing
- Close progress bar when complete

```python
class Extract(ETLStage[None, Storm]):
    def process(self, _: None) -> Iterator[Storm]:
        try:
            # Count total lines for progress bar
            total_lines = sum(1 for _ in open(self.input_path))
            self.init_progress(total_lines, "Extracting storms")

            for storm in parse_hurdat2(self.input_path):
                # Update progress (header + observation lines)
                self.update_progress(1 + len(storm.observations))
                yield storm

        finally:
            self.close_progress()
```

### 3. Transform Stage Changes

Add progress tracking for transformation operations:

```python
class Transform(ETLStage[Storm, Storm]):
    def process(self, data: Iterator[Storm]) -> Iterator[Storm]:
        storms = list(data)  # Materialize for count
        self.init_progress(len(storms), "Transforming storms")

        for storm in storms:
            # Transform logic here
            self.update_progress()
            yield storm
```

### 4. Load Stage Changes

Add progress tracking for database operations:

```python
class Load(ETLStage[Storm, None]):
    def process(self, data: Iterator[Storm]) -> None:
        storms = list(data)  # Materialize for count
        self.init_progress(len(storms), "Loading storms")

        for storm in storms:
            # Load logic here
            self.update_progress()
```

## Progress Tracking Considerations

### 1. Memory Usage

- Extract stage can count lines without loading whole file
- Transform/Load stages need to materialize iterator for accurate counts
- Consider batch processing for large datasets

### 2. Accuracy

- Extract stage tracks actual file lines processed
- Transform/Load stages track storm objects processed
- Progress updates should reflect actual work done

### 3. Configuration

- Progress bars should be configurable via settings
- Consider CLI flag for enabling/disabling progress
- Allow customization of progress bar format

## Testing Considerations

- Add unit tests for progress tracking initialization
- Test progress updates occur correctly
- Verify progress bars close properly
- Test with progress disabled
- Mock tqdm for testing

## Implementation Order

1. Update ETLStage base class with progress tracking
2. Modify Extract stage implementation
3. Update Transform stage
4. Update Load stage
5. Add configuration options
6. Add test coverage
7. Update documentation

## Dependencies

- Add tqdm to project dependencies
- Consider making progress bars optional if tqdm not installed
