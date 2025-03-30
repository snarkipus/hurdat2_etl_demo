# Commands and Conventions for ETL Pipeline

## Commands
- **Install:** `poetry install`
- **Run ETL:** `poetry run etl-pipeline run`
- **Run Tests:** 
  - All tests: `poetry run pytest`
  - Single test: `poetry run pytest tests/path/to/test_file.py::test_name`
  - With coverage: `poetry run pytest --cov=src`
- **Type Check:** `poetry run mypy src tests`
- **Lint/Format:** `poetry run ruff check src tests` or `poetry run ruff format src tests`

## Code Style
- **Typing:** Strict static typing with mypy; all functions must be typed
- **Formatting:** 88-character line length, double quotes, 4-space indentation
- **Imports:** Sorted by stdlib, third-party, local; use absolute imports
- **Naming:** snake_case for variables/functions, PascalCase for classes, UPPER_CASE for constants
- **Error Handling:** Use custom exceptions inheriting from `ETLError`; explicit exception handling
- **Architecture:** Maintain separation between extract, transform, and load stages