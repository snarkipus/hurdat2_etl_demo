# HURDAT2 ETL Pipeline

[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Typed with BasedPyright](https://img.shields.io/badge/BasedPyright-standard-blue.svg)](https://docs.basedpyright.com/)

A modern ETL (Extract, Transform, Load) pipeline for processing HURDAT2 hurricane track data into a structured DuckDB database.

<img src="docs/assets/readme/progress_bars.png" alt="ETL Pipeline Demo" width="800">

## Overview

This project implements a complete ETL pipeline for the [HURDAT2 dataset](https://www.nhc.noaa.gov/data/#hurdat), the National Hurricane Center's Atlantic hurricane database. The pipeline:

1. **Extracts** raw data from HURDAT2 text files
2. **Transforms** it into structured, validated data models
3. **Loads** it into a DuckDB database for easy querying

The implementation follows modern software engineering practices, including clean architecture principles, domain-driven design, comprehensive test coverage, and strong type safety.

## Features

- **Clean Architecture**: Separation of concerns with distinct Extract, Transform, and Load stages
- **Rich Progress UI**: Dynamic progress tracking with color-coded status indicators
- **Statistical Analysis**: Comprehensive summary of processed hurricane data
- **Robust Error Handling**: Custom exception hierarchy and detailed error reporting
- **Comprehensive Tests**: 80%+ code coverage with unit and integration tests
- **Type Checking**: BasedPyright standard diagnostics for source and tests
- **DuckDB Integration**: Fast, efficient geospatial querying capabilities
- **Detailed Logging**: Structured logging throughout the pipeline

## Installation

### Prerequisites

- Python >=3.13,<4 (development pin: 3.13)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (validated with 0.12.10)

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/snarkipus/hurdat2_etl_demo.git
   cd hurdat2_etl_demo
   ```

2. Install the pinned Python and locked project/development dependencies:
   ```bash
   uv python install 3.13
   uv sync --locked
   ```

   `uv sync --locked` uses `.python-version` and refuses an out-of-date `uv.lock`
   instead of updating dependency selections. Generate dependency changes with
   `uv lock`; never hand-edit the lockfile. Poetry is not required.

## Usage

Run the ETL pipeline with:

```bash
uv run --locked etl-pipeline --input /path/to/hurdat2.txt --output /path/to/database.duckdb
```

This is a single-command app: do not add a `run` or `run-etl` subcommand.
The output parent directory must already exist. DuckDB Spatial installation
may need network access (or a compatible cached extension); unavailable Spatial
prevents publication, rather than silently disabling verification.

Options:
- `--input`, `-i`: Path to the input HURDAT2 text file (required)
- `--output`, `-o`: Path to the output DuckDB database file (required)
- `--replace`: Explicitly allow atomic replacement of existing output. The CLI
  builds and verifies a sibling candidate before publication; keep external
  writers stopped. Publication failure retains the candidate at the reported
  path for manual recovery, without changing old output. Supported local
  filesystems must provide atomic replace/hard-link operations; no copy fallback
  or automatic backup is provided.
- `--log-level`, `-l`: Logging level (defaults to INFO)

Diagnostics append structlog-backed JSON lines to `logs/pipeline.log` in the
working directory, separate from Rich progress and summaries. Events carry a
UTC timestamp, severity, run ID, and applicable operation/stage and error context.
Each CLI invocation owns and closes its log handler; library diagnostics use the
same bridge and run-level filter (library-specific thresholds still apply).
Stage construction alone does not create a log file or configure host logging.
Existing host handlers remain attached; run exit restores the caller's logging
level and context. This is local diagnostic output, not an audit/rotation or
concurrent-run logging service. Manage log retention externally. Events avoid
full source records and SQL bound-parameter dumps by default, but paths, IDs,
exception text and tracebacks can appear: review logs before sharing them.

<img src="docs/assets/readme/help.png" alt="ETL Pipeline Demo CLI Help" width="800">

### Output Acceptance and Operating Limits

Each run is a full refresh: the CLI applies packaged Alembic migrations to
`head` on a unique sibling candidate, loads and commits it, then independently
checks persisted counts against accepted loader inputs. It closes database
resources before publishing. Existing output is never migrated, appended to,
or used as an upsert target. Direct Unit-of-Work callers must initialize schema
explicitly; entering a UoW does not create or repair tables. Normal CLI execution
does not use the repository's `alembic.ini` or its `data/hurdat.duckdb` default.

Observation `geom` is WKT **text**, `POINT(longitude latitude)`, not a native
geometry column; spatial queries use `ST_GeomFromText(geom)`. Every stored
geometry must parse as a nonempty point with finite longitude in `[-180, 180]`
and latitude in `[-90, 90]`, inclusive. No clamping, wrapping, coordinate repair,
or dropping persisted rows makes verification pass: `270E` still normalizes to
`270.0` and fails output acceptance. Existing skipped-record and last-wins storm
deduplication rules remain; even zero accepted rows can pass. This gate is not
a source checksum or a complete meteorological data-quality audit.

To deliberately replace output after preserving any operator-required backup:

```bash
uv run --locked etl-pipeline --input /path/to/hurdat2.txt --output /path/to/database.duckdb --replace
```

Use one active run per destination on an ordinary local filesystem, with
external writers stopped for the entire run. Destination symlinks, non-regular
files, input/output aliases and detected destination `.wal` files are refused.
Never delete an old WAL to bypass refusal; resolve it through the owning
database's normal shutdown/recovery. WAL checks are not writer coordination.
Allow space for the old database, the complete candidate and transient WAL/temp
state (and any backup you make); input and transformed rows are also materialized
in memory. No automatic space reservation or backup is provided.

Creation uses an atomic hard link that refuses late collisions; explicit
replacement uses atomic replace. Unsupported/denied operations fail without
copy, forced access, or delete-first fallback. Network filesystems, hostile path
changes and concurrent ownership are outside the supported envelope. Atomic
visibility is not guaranteed power-loss durability: crashes/abrupt termination
can leave artifacts, with no automatic sweeping, retry or crash recovery.

### Failure and Manual Recovery

- Processing, verification or finalization failure exits non-zero, preserves old
  output and attempts to remove only run-owned incomplete artifacts. Cleanup
  warnings identify leftovers; these are **not** verified recovery candidates.
- Publication failure exits non-zero and reports the finalized **verified
  candidate retained** path; old output remains unchanged. Fix the reported
  permission/lock/filesystem problem before considering manual publication.
- After publication, redundant-name cleanup or optional summary failures warn
  without undoing output or changing success. A leftover candidate name after
  successful hard-link publication can refer to the same file as the output.

There is no recovery CLI subcommand. For a candidate explicitly reported as
verified and retained after publication failure, a conservative manual recovery
is to expose it under a **new, unused sibling name**, leaving the old destination
and retained candidate untouched. First stop writers, confirm the reported
candidate is a regular non-symlink standalone file with no `.wal`, and ensure
neither the chosen new name nor its `.wal` exists. From the checkout, substitute
the actual absolute paths in this command (ordinary local directory only):

```bash
uv run --locked python -c 'import os, sys; os.link(sys.argv[1], sys.argv[2])' /absolute/path/.database.duckdb.RUN.duckdb /absolute/path/recovered.duckdb
```

The hard-link operation refuses an existing destination atomically and leaves
the retained name in place. If it fails, stop; do not add a forced overwrite or
copy/delete fallback. Reopen the recovered database read-only and inspect its
contents before using it or removing the redundant retained name. Both names
refer to the same data, **not independent backups**. Do not use this procedure
for incomplete or unexplained crash leftovers. Replacing the original pathname
is a separate deliberate operator decision; alternatively rerun the ETL with
`--replace` after resolving the cause. A successful replacement keeps no old-file
backup.

## Project Structure

```
etl_pipeline/
├── data/                  # Default directory for storing output data (e.g., DuckDB file)
├── docs/                  # Workflow guide, README assets, and historical archive
├── openspec/              # Accepted specifications and proposed changes
├── ref/                   # Reference materials (e.g., data format specs, source data files)
├── src/                   # Main source code directory
│   └── etl_pipeline/      # Core package for the ETL pipeline
│       ├── extract/       # Modules for extracting data from the source (HURDAT2)
│       ├── load/          # Modules for loading transformed data into the target (DuckDB)
│       ├── migrations/    # Database schema migration scripts (Alembic)
│       ├── transform/     # Modules for transforming and validating extracted data
│       ├── utils/         # Shared utility functions and classes
│       ├── cli.py         # Command-line interface logic (using Typer)
│       ├── core.py        # Shared stage execution, logging, and Rich progress
│       └── exceptions.py  # Custom exception classes for the pipeline
└── tests/                 # Test suite for the project
    ├── integration/       # Integration tests (testing component interactions)
    └── unit/              # Unit tests (testing individual modules/functions)
```

## Architecture

The ETL pipeline follows a three-stage architecture:

1. **Extract Stage**:
   - Processes the raw HURDAT2 text file
   - Yields rows of raw string data

2. **Transform Stage**:
   - Parses raw data into structured objects
   - Validates data using Pydantic models
   - Retains existing normalization and skipped-record rules

3. **Load Stage**:
   - Sets up DuckDB with spatial extensions
   - Implements the Repository pattern
   - Uses Unit of Work pattern for database transactions

## Statistical Insights

The ETL process generates a comprehensive summary report, including:

- Total storms and observations processed
- Full date range of the dataset
- Peak recorded wind per storm across all observation statuses, in neutral bands
  (<34, 34-63, 64-82, 83-95, 96-112, 113-136, and >=137 knots); these bands do not
  assign tropical-cyclone status or classification from wind alone
- Decade-by-decade distribution of hurricane activity
- Longest-duration storms in the record

## Development

See the [documentation map](docs/README.md) and
[agentic workflow](docs/agentic-workflow.md) before starting a change.
OpenSpec owns specification and intent; Beads drives implementation, with
`tasks.md` reconciled continuously from verified outcomes. Historical plans and
diagrams are retained in the [documentation archive](docs/archive/README.md),
not treated as accepted requirements or an active backlog.

### Testing

Run tests with pytest:

```bash
uv run --locked pytest              # Full suite, branch coverage, 80% minimum
uv run --locked pytest --no-cov tests/unit/transform/test_parser.py  # Focused only
```

### Type Checking

Run BasedPyright with standard diagnostics for `src` (including migrations) and
`tests`, without blanket diagnostic suppressions:

```bash
uv run --locked basedpyright
```

### Linting

Run ruff linter:

```bash
uv run --locked ruff check src tests
uv run --locked ruff format --check src tests
```

Use `uv run --locked ruff format src tests` to apply formatting.

### Local Hooks

```bash
uv run --locked pre-commit install
uv run --locked pre-commit run --all-files
```

All four hooks run the full configured scope through `uv run --locked`: Ruff
lint with fixes, Ruff formatting, BasedPyright, then pytest with the same 80%
branch-measured coverage gate. Hooks can change files; inspect their diff and
rerun until clean. The non-fixing commands above are the verification gates.
No remote Ruff/type-checker environments or separately resolved dependencies
are used. Hooks also run for configuration-only changes.

BasedPyright 1.40.0 is the selected stable checker. Ruff 0.11.2 and pre-commit
4.2.0 remain locked: both support this Python 3.13 workflow, and hook parity
does not require upgrading them or the existing test/build tools. Dependency
updates must regenerate `uv.lock` with uv and pass these same checks.

### Builds and Installed-Package Smoke

Build the PEP 621/Hatchling sdist and wheel with `uv build` (outputs to `dist/`).
The wheel includes the CLI and Alembic resources. For the Linux acceptance smoke,
use an existing temporary directory **outside** the checkout:

```bash
uv run --locked python tests/installed_package_smoke.py --temp-root /tmp
```

The script builds the wheel from the sdist, installs locked runtime dependencies
with hashes into a fresh non-editable environment, runs `pip check`, then checks
installed CLI help and fixture ETL values/migrations independently. CLI output
and logs stay in its isolated temporary working directory, removed on exit.
It needs uv, Python 3.13 and package-index/Spatial access or caches; do not use
`-O`/`PYTHONOPTIMIZE`, which would disable its assertions.

### Continuous Integration

[GitHub Actions CI](.github/workflows/ci.yml) runs on PRs targeting `main` and
pushes to `main`, using uv 0.12.10 and the shared Python 3.13 pin:

- **Linux quality and wheel** (`ubuntu-24.04`): locked non-fixing Ruff/format,
  BasedPyright, full pytest with the 80% branch-measured gate, and the installed
  wheel smoke outside the checkout.
- **Windows runtime and publication** (`windows-2025`): ten targeted real
  DuckDB/new-and-replacement output and file-lifecycle cases, not the full Linux
  suite or coverage gate.

Both jobs explicitly install/load/query Spatial and fail on errors; they do not
skip unavailable Spatial. They require no private credentials for public-fork
PRs. Workflow checks do not by themselves configure required branch-protection
checks. Test runs write ignored coverage artifacts; fixtures isolate their
working directories so they do not modify the developer's pipeline log.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [HURDAT2 Dataset](https://www.nhc.noaa.gov/data/#hurdat) from the National Hurricane Center
- [DuckDB](https://duckdb.org/) for the embedded database
