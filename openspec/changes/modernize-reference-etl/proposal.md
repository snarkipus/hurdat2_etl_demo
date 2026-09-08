## Why

The current HURDAT2 reference application has a working staged design, but its
value correctness and migration path need stronger validation, and its CLI
deletes existing output before processing. Validate that baseline, correct its
output lifecycle, and modernize its tools without adding a framework or an
exhaustive defensive layer around a small local application.

## What Changes

- Establish a representative, independently reopened database round trip on
  today's implementation before changing runtime behavior or tools. Reuse
  existing tests and add characterization only where meaningful coverage is missing.
- **BREAKING**: Raise the supported Python minimum to 3.13; replace Poetry
  project/lock workflows with uv and mypy with BasedPyright. Align Ruff,
  pre-commit, packaging, and GitHub Actions around reproducible quality gates.
- Replace formatted text logging with structlog-backed structured events while
  retaining Typer/Rich progress and operator reports.
- Retain existing stages and local HURDAT2 reading, parsing, normalization,
  units, missing-value handling, and deduplication semantics. Extract a helper
  only when a concrete implementation or testability need justifies it.
- Make Alembic migrations to `head` authoritative for schema creation on a
  fresh, uniquely named temporary DuckDB database beside the destination.
- **BREAKING**: Refuse an existing destination before processing unless
  `--replace` is supplied. Preserve it until a verified replacement can be
  published; refuse late destination collisions without `--replace` too.
- Verify persisted counts against accepted loader inputs and use a compact
  DuckDB Spatial check for point geometry and geographic bounds. Rely on tested
  schema constraints for required fields and relationships, not duplicate
  runtime audits. **BREAKING**: Out-of-bounds normalized coordinates prevent
  publication without clamping, wrapping, or otherwise repairing those values.
- Finalize database state and close all connections before same-directory
  publication. Remove incomplete temporary artifacts on processing failure;
  retain and report verified output on publication failure. Verification is
  fatal; an optional summary failure only warns.

Modernization remains in scope as separately verifiable increments, not a
prerequisite for runtime corrections. Tests cover distinct behaviors at the
lowest useful layer, with a small end-to-end suite rather than a cross-product
of stages, failures, and platforms.

The [target diagram](assets/ETL_diagram_v2.png) describes the proposed direction,
not implemented behavior. The contracts in the specs and design disambiguate
its labels, especially normalization, schema lifecycle, and verification.

### Non-goals

No incremental loads, upserts, automatic upgrades of an existing destination,
standalone `db upgrade` command, source/plugin framework, scheduler, remote
source ingestion, data-quality framework, new unit system, or native geometry
column migration. No retrospective import of historical plans. No changes to
agent workflow authority or automatic Git/Beads publication.

The operating envelope is one local full-refresh run on a supported filesystem,
with external writers quiesced by the operator. No hostile-directory defense,
power-loss recovery, concurrent-writer coordination, filesystem fallback matrix,
or exhaustive combinations of secondary cleanup/reporting failures.

## Capabilities

### New Capabilities

- `development-baseline`: Reproducible Python 3.13+/uv development, BasedPyright,
  aligned local checks, and CI gates.
- `pipeline-observability`: Structured lifecycle and failure events separated
  from Rich presentation and optional summaries.
- `hurdat2-source-contract`: Retained local ingestion and independently validated
  normalized values, without mandatory structural refactoring.
- `migration-backed-loading`: Alembic-owned schema creation and transactional
  persistence into an isolated full-refresh database.
- `persisted-dataset-verification`: Independent loader-boundary counts and compact
  point/coordinate acceptance checks, including explicit geographic bounds.
- `safe-output-publication`: Explicit replacement consent, isolated build,
  database finalization, race-safe publication, and failure recovery.

### Modified Capabilities

None. `openspec/specs/` is currently empty; these are the first formal capability
specs, including contracts for retained behavior as well as changes.

## Impact

Future implementation touches `pyproject.toml`, lock/build configuration,
pre-commit and new CI workflows; CLI orchestration, logging, extract/load
boundaries, Alembic integration and packaged migrations; tests and active
usage/development guidance. Existing HURDAT2 reference files and normalized data
representations remain unchanged. Shell workflows and existing-output behavior
change deliberately; deployments need Python 3.13+ and working DuckDB Spatial.
The work can be delivered in verifiable increments under one reviewed proposal;
the implementation dependency graph will be mapped to Beads after review.
