# pipeline-observability Specification

## Purpose

Give operators machine-readable run diagnostics and clear human-facing status
without confusing optional presentation failures with persistence integrity.

## Requirements

### Requirement: Structured run and stage events

The application SHALL write parseable JSON-line events to `logs/pipeline.log`
with timestamp, severity, event name, and a run identifier. Stage-related events
SHALL identify the stage; failures SHALL identify the failing operation and
error details. One run's context SHALL NOT leak into subsequent runs.

#### Scenario: Successful lifecycle
- **WHEN** an ETL run completes extraction, transformation, migration, loading, verification, and publication
- **THEN** the log identifies their outcomes under the same run identifier and records publication success only after the output is published

#### Scenario: Repeated invocation
- **WHEN** two runs execute in the same process
- **THEN** their events have distinct run identifiers and handlers do not duplicate event output

### Requirement: Separate diagnostics and presentation

The CLI SHALL retain readable progress, stage completion, and summary output.
Rich styling and terminal control sequences SHALL NOT corrupt JSON log events.
The existing log-level option SHALL continue to control diagnostic verbosity.

#### Scenario: Debug run with console progress
- **WHEN** the CLI runs at DEBUG level with progress enabled
- **THEN** console output remains human-readable and each emitted log event remains independently parseable JSON

### Requirement: Trustworthy success reporting

The CLI SHALL report overall success only after verification, database
finalization, and publication succeed. An optional summary failure after
successful publication SHALL produce a warning without changing the successful
exit status or deleting the published database. A required processing,
verification, or publication failure SHALL remain non-zero even if reporting
that failure encounters another error.

#### Scenario: Optional summary query fails
- **WHEN** a verified database has been published but a summary query or rendering operation fails
- **THEN** the CLI warns, retains the database, and exits successfully

#### Scenario: Verification failure
- **WHEN** required verification fails
- **THEN** the CLI exits non-zero and does not print an overall successful-completion message
