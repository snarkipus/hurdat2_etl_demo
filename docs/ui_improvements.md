# Console UI Improvements

## Issues Fixed

The ETL pipeline had several UI issues that caused a "wonky" user experience:

1. **Independent Progress Contexts**: Each ETL stage was creating and destroying its own progress display, causing flickering and inconsistent UIs
2. **Fragmented Progress Tracking**: No centralized mechanism to coordinate progress bars across stages
3. **Competing Console Outputs**: Text mixed with progress bars disrupted the display
4. **Incomplete Progress Tasks**: Some tasks were created but never marked as completed

## Solution Implemented

### 1. Centralized Progress Management

Created a new `ProgressManager` class to:
- Maintain a single consistent progress display
- Provide a unified interface for all stages to access
- Track tasks with named identifiers
- Handle automatic task completion

```python
class ProgressManager:
    def __init__(self, console: Optional[Console] = None):
        self.console = console or Console()
        self.progress = self._create_progress()
        self.active = False
        self.tasks: Dict[str, TaskID] = {}
```

### 2. Rich Live Display

Used Rich's `Live` context manager to ensure stable UI updates:

```python
with Live(progress_manager.progress, refresh_per_second=10):
    progress_manager.start()
    # Execute ETL stages
```

### 3. Consistent Task Management

Added clear task lifecycle methods:
- `add_task(name, description, total)` - Creates identifiable tasks
- `update(name, advance)` - Updates task progress
- `complete_task(name, description)` - Explicitly marks tasks complete

### 4. Optimized Progress Updates

- Implemented throttling to prevent UI flicker (update every 10 or 50 rows)
- Batch updates for large operations to reduce overhead
- Explicit task completion for all stages

### 5. Adaptive Progress Bar Width

- Removed hard-coded width to adjust to terminal size
- Added `expand=True` to ensure progress bars use available space

## Results

The improved UI provides:
1. A stable, non-flickering progress display
2. Clearer hierarchical task organization
3. Consistent visual formatting
4. Better responsiveness
5. Proper task completion indicators

The UI now shows a coherent pipeline flow rather than disconnected, competing progress displays.