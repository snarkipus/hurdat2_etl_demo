# Project Implmentation Plan: HURDAT2 ETL Pipeline

### Phase 1: Project Setup and Configuration

**Objective**: Establish a maintainable project foundation with testing tools integrated from the start.

**Testing Integration**:
- Configure Pytest in `pyproject.toml` to run tests with coverage reports using Pytest-cov.
- Create a basic test file in `tests/` to verify the testing framework is operational.

**Deliverable**: A Poetry-managed project with a clear structure, dependencies installed, and testing framework configured.

**Purpose**: Provides a consistent, organized starting point with testing tools ready for incremental validation.

**Tasks**:
- [ ] Initialize the project using Poetry with `poetry init`.
- [ ] Set up a directory structure:
  - `src/` for source code
  - `tests/` for test scripts
  - `logs/` for log files
- [ ] Configure static typing with Mypy and linting with Ruff in `pyproject.toml`.
- [ ] Define dependencies in `pyproject.toml`, including:
  - `pydantic` (for data validation)
  - `sqlalchemy` (for ORM and session management)
  - `alembic` (for schema migrations)
  - `duckdb` (for database storage with spatial extension)
  - `typer` (for CLI framework)
  - `rich` (for progress bars and console output)
  - `pytest` and `pytest-cov` (as dev dependencies for testing)
- [ ] Create a basic test file `tests/test_basic.py` with a simple passing test (e.g., `assert True`).
- [ ] Set up Pytest configuration in `pyproject.toml` to run tests from `tests/` and generate coverage reports for `src/`, e.g.:
  ```toml
  [tool.pytest.ini_options]
  testpaths = ["tests"]
  addopts = "--cov=src --cov-report=term-missing"
  ```

---

### PHASE 2: IMPLEMENT EXTRACT STAGE

**Objective**: Develop the functionality to parse the HURDAT2 CSV file and extract raw data.

**Testing Integration**:
- Write unit tests for the CSV parser using a sample dataset.
- Ensure the parser correctly identifies header and data lines, handles missing values, and extracts all fields as strings.

**Deliverable**: A working CSV parser that extracts storm headers and track points into Python data structures.

**Purpose**: Provides the foundation for data ingestion, enabling subsequent transformation and loading stages.

**Tasks**:
- [ ] Create a sample HURDAT2 CSV file in `tests/data/` for testing (e.g., a small excerpt with a few storms and track points).
- [ ] Implement the CSV parsing logic in `src/etl/extract.py`, producing a list of storm dictionaries with header metadata and track point lists.
- [ ] Define data structures (e.g., lists and dictionaries) to hold raw extracted data, maintaining all fields as strings per the PRD.
- [ ] Write unit tests in `tests/unit/test_extract.py` to verify:
  - Correct parsing of header and data lines.
  - Proper handling of missing values (e.g., "-999").
  - Accurate extraction of storm and track point counts.

---

### Phase 3: Implement Transform Stage

**Objective**: Develop the data transformation logic using Pydantic for validation and type conversion, potentially structuring the flow using the **Chain of Responsibility** or **Pipes and Filters** pattern for modularity.

**Testing Integration**:
- Write unit tests for transformation functions and Pydantic models.
- Ensure correct conversion of dates to UTC datetime, coordinates to decimal degrees, and proper handling of missing values.

**Deliverable**: Transformed data models that are validated and ready for database insertion.

**Purpose**: Ensures data consistency and accuracy before loading into the database.

**Tasks**:
- [ ] Define Pydantic models for storm headers and track points in `src/models.py`, specifying fields like `datetime: datetime`, `latitude: float`, `longitude: float`, etc.
- [ ] Implement transformation functions in `src/etl/transform.py` to convert raw data into Pydantic model instances, including:
  - Combining date and time into UTC datetime (e.g., "20230101" and "1200" to `datetime` object).
  - Converting latitude/longitude to decimal degrees (e.g., "29.5N" to `29.5`, "80.0W" to `-80.0`).
  - Mapping missing values (e.g., "-999") to `None`.
- [ ] Write unit tests in `tests/unit/test_transform.py` to verify:
  - Accurate field conversions.
  - Validation errors for invalid data.
  - Proper handling of historical wind speed precision (e.g., nearest 5 kt post-1886).

---

### Phase 4: Implement Load Stage

**Objective**: Develop the functionality to load transformed data into DuckDB with spatial capabilities, using the **Repository** pattern for data access abstraction and the **Unit of Work** pattern for transaction management.

**Testing Integration**:
- Write unit tests to verify data insertion and spatial functionality using an in-memory DuckDB instance.
- Ensure the schema is correctly defined and migrations are managed with Alembic.

**Deliverable**: A working load mechanism that inserts data into DuckDB and sets up spatial columns.

**Purpose**: Enables storage and spatial analysis of hurricane data.

**Tasks**:
- [ ] Set up Alembic for schema management, initializing it with `alembic init`.
- [ ] Define the database schema using SQLAlchemy models in `src/models.py`, e.g.:
  - `Storm` table: `id`, `name`, `year`, etc.
  - `TrackPoint` table: `id`, `storm_id` (foreign key), `datetime`, `latitude`, `longitude`, `geom` (spatial POINT).
- [ ] Implement the load logic in `src/etl/load.py` to:
  - Connect to DuckDB and load the spatial extension (`INSTALL spatial; LOAD spatial;`).
  - Insert transformed data using SQLAlchemy sessions (managed by the Unit of Work).
  - Populate the `geom` column with `ST_Point(longitude, latitude)`.
- [ ] Write unit tests in `tests/unit/test_load.py` to verify:
  - Correct table creation and data insertion.
  - Spatial column population (e.g., query `geom` values).
  - Basic spatial query functionality (e.g., `ST_Distance`).

---

### Phase 5: Implement CLI Interface

**Objective**: Develop a user-friendly CLI for running the ETL pipeline with progress indicators and error logging, potentially using the **Command** pattern to encapsulate ETL stages.

**Testing Integration**:
- Write tests for CLI commands using Typer’s testing features or by mocking inputs.
- Ensure the CLI correctly handles file paths, executes ETL stages, and displays progress.

**Deliverable**: A functional CLI that allows users to execute the ETL pipeline and monitor its progress.

**Purpose**: Facilitates user interaction and provides transparency during pipeline execution.

**Tasks**:
- [ ] Implement the CLI entry point in `src/cli.py` using Typer, with commands to specify input CSV and output DuckDB paths (e.g., `run-etl --input file.csv --output db.duckdb`).
- [ ] Integrate the extract, transform, and load functions into the CLI workflow (potentially as Command objects).
- [ ] Add progress bars using Rich for each stage (e.g., parsing storms, transforming records, loading data).
- [ ] Set up structured logging to a file in `logs/` using Python’s `logging` module, capturing errors and stage completion details.
- [ ] Write tests in `tests/integration/test_cli.py` to verify:
  - Command execution with valid inputs.
  - Progress display and log file creation.
  - Error handling for invalid inputs.
- [ ] Implement summary report generation after loading, querying DuckDB for stats (e.g., number of storms, track points, date range) and displaying via Rich.

---

### Additional Notes
- **Error Handling**: Throughout the phases, especially in Transform and Load stages, implement graceful error handling (e.g., log validation failures and continue or exit based on severity), aligning with the PRD’s reliability requirement.
- **Performance**: While designed for single-user use, aim for efficient processing (e.g., <10 minutes for the full dataset), leveraging DuckDB’s capabilities.
- **Testing Coverage**: Incremental testing ensures the 80% coverage goal, with unit tests for individual components and integration tests for stage interactions.
- **Final Validation**: After Phase 5, perform an end-to-end run with the full HURDAT2 dataset to confirm error-free processing and spatial accuracy, as per success metrics.