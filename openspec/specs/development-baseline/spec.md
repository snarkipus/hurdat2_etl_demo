# development-baseline Specification

## Purpose

Provide a reproducible development and distribution baseline with consistent
local and automated checks, so changes to the reference pipeline are verifiable.
This is the target toolchain, not a prerequisite for baseline validation or
runtime safety fixes, which can use the current project tools until the switch.

## Requirements

### Requirement: Supported runtime and reproducible setup

The project SHALL require Python >=3.13,<4 and provide a committed uv lockfile
and documented setup commands that install the project and development tools
without Poetry. Locked setup SHALL fail on inconsistent project metadata rather
than silently rewrite dependency selections.

#### Scenario: Clean developer environment
- **WHEN** a developer runs `uv sync --locked` on the documented Python 3.13 baseline
- **THEN** the project and development tools install from the committed lockfile without requiring Poetry or modifying the lockfile

#### Scenario: Unsupported interpreter
- **WHEN** installation is attempted with Python 3.12
- **THEN** the package metadata rejects that interpreter rather than claiming support

### Requirement: Validated runtime-library refresh

After establishing the locked Python 3.13 baseline, the project SHALL separately
review and upgrade its existing runtime libraries and necessary runtime
transitives to compatible stable releases. Version selections and any deferred
upgrades SHALL be recorded with their reasons. Declared ranges SHALL be reviewed
for compatibility rather than blindly accepting breaking releases. The uv
lockfile SHALL be generated with uv, without unrelated dev/build-tool upgrades.
Necessary compatibility edits SHALL preserve the source-value, migration,
spatial verification, transaction, and safe-publication contracts.

#### Scenario: Validate refreshed runtime dependencies
- **WHEN** the runtime-library refresh is implemented on Python 3.13
- **THEN** clean locked setup leaves the lockfile unchanged, the full suite retains at least 80% branch coverage, and existing independent value, migration/schema, Spatial, transaction, and publication assertions pass without weakened expectations

#### Scenario: Downstream acceptance uses the refreshed stack
- **WHEN** installed-wheel acceptance, the final bounded cleanup review, CI, and final acceptance execute
- **THEN** they evaluate the refreshed runtime dependency set rather than treating pre-upgrade results as sufficient evidence

### Requirement: Usable distributable CLI

Built distributions SHALL include all runtime resources needed for a fresh
database run, including migration resources. The installed `etl-pipeline`
entrypoint SHALL retain the single-command `--input`, `--output`, and
`--log-level` interface and expose the new `--replace` option.

#### Scenario: Run outside the source checkout
- **WHEN** a wheel built with `uv build` is installed in a clean supported environment and invoked from a directory outside the repository
- **THEN** `etl-pipeline --help` works and a fixture ETL run can create a migrated and verified database without a repository-local Alembic configuration

### Requirement: Consistent quality gates

Documented local checks and CI SHALL use the same configured lint, formatting,
typing, and test policies. BasedPyright SHALL replace mypy as the enforced type
checker. The test gate SHALL retain branch measurement and at least 80% total
coverage; adopting new tools SHALL NOT disable the gate or broadly exclude
runtime code to obtain a pass.

#### Scenario: Developer quality checks
- **WHEN** `uv run ruff check src tests`, `uv run ruff format --check src tests`, `uv run basedpyright`, and `uv run pytest` execute against the completed change
- **THEN** all commands pass under documented configuration, and the checks leave tracked source and lockfiles unchanged

#### Scenario: Local hook parity
- **WHEN** the configured pre-commit hooks execute
- **THEN** they enforce the same policies and dependency selections as the documented checks, without a separate mismatched Ruff or mypy environment

### Requirement: Automated platform verification

GitHub Actions SHALL run on pull requests to `main` and pushes to `main`, with
Python 3.13 full-suite coverage on Linux and targeted integration coverage on
Windows for real DuckDB use, file publication, and resource release. Jobs SHALL provision
DuckDB Spatial explicitly, fail on quality or installation errors, and require
no private credentials to validate a public fork pull request.
The full failure matrix need not be repeated through the CLI on both platforms.

#### Scenario: Pull request gate failure
- **WHEN** lint, formatting, type checking, migration execution, publication tests, or coverage fails in its configured CI job
- **THEN** that job reports failure rather than suppressing the error or silently skipping spatial integration tests
