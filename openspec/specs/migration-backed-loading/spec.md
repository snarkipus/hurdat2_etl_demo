# migration-backed-loading Specification

## Purpose

Create a versioned schema and persist each accepted dataset in an isolated
full-refresh database without treating existing outputs as incremental stores.

## Requirements

### Requirement: Authoritative migration-created schema

Every ETL build SHALL apply the packaged Alembic migration chain to `head` on
its new temporary database before inserting application records. The database
SHALL contain the migration revision marker and the tables, columns, and
constraints required by persistence. Runtime creation SHALL NOT bypass
migrations by silently creating or repairing tables from ORM metadata.
Existing foreign-key and non-null constraints SHALL enforce relationships and
required values during loading; optional meteorological values remain nullable.
These constraints do not require a second post-load null/orphan audit.

#### Scenario: Fresh database schema
- **WHEN** an ETL run initializes its temporary database
- **THEN** the migration revision is at `head` before loading and the resulting schema supports the expected storms and observations records

#### Scenario: Schema integrity
- **WHEN** loading attempts an orphan observation or a null required value
- **THEN** the database rejects the invalid write, while optional null pressure or wind radii remain valid

#### Scenario: Migration cannot complete
- **WHEN** any schema migration fails
- **THEN** loading and publication do not proceed, the requested destination remains untouched, and incomplete temporary artifacts are cleaned up under the publication failure contract

### Requirement: Full-refresh isolation

An ETL run SHALL populate a newly built database only. Even with `--replace`,
the existing destination SHALL NOT be opened for migration, appended to, or
used as an upsert target. This change SHALL NOT add an operator command for
in-place upgrades of an existing output.

#### Scenario: Replace a previously generated output
- **WHEN** an existing output is supplied with `--replace`
- **THEN** migrations and inserts target the new temporary database and the old output is unchanged until successful publication

### Requirement: Committed data and released resources

Loading SHALL commit the accepted records before persisted-result verification.
Insertion or commit failures SHALL propagate as processing failures. Database
sessions and connections SHALL be released on success and failure; before
publication all migration, load, and verification connections SHALL be closed
and pending database state SHALL be finalized into a standalone artifact.

#### Scenario: Commit failure
- **WHEN** persisting the accepted dataset fails at transaction commit
- **THEN** the run fails, releases its database resources, and does not verify or publish a successful output

#### Scenario: Independent reopen
- **WHEN** a verified candidate is finalized for publication
- **THEN** it can be reopened with the committed records and migration state without relying on an open writer or an unpublished write-ahead-log sidecar
