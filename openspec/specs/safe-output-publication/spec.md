# safe-output-publication Specification

## Purpose

Publish only fully built, verified database artifacts while making replacement
explicit and preserving existing output or recoverable candidates on failure.

## Operating Limits

Support ordinary local filesystems with atomic same-directory publication and
one active run per destination. Operators quiesce external writers. Concurrent
ownership coordination, hostile path mutation, network-filesystem guarantees,
power-loss recovery, and fallback publication mechanisms are outside scope.
Unsupported or denied filesystem operations fail clearly without weakening
the no-overwrite guarantee. Recovery is reporting retained paths, not an
automated retry, repair, or sweeping service.

## Requirements

### Requirement: Explicit destination replacement consent

Without `--replace`, the CLI SHALL fail clearly before processing if the
requested destination exists. With `--replace`, it SHALL leave existing output
untouched throughout processing and verification. Input and output identifying
the same file SHALL be rejected rather than replacing the source input.
The parent directory SHALL exist; symlink or other non-regular destinations
SHALL be rejected rather than followed or replaced. A detected destination WAL
SHALL block processing at preflight or publication at the final check. The
application SHALL NOT delete or adopt an old destination's WAL.

#### Scenario: Existing destination without consent
- **WHEN** the destination exists and `--replace` is absent
- **THEN** the CLI exits non-zero before extraction, migration, or loading and explains how explicit replacement is requested

#### Scenario: Explicit replacement
- **WHEN** the destination exists and `--replace` is present
- **THEN** processing builds alongside the existing file without deleting, migrating, or changing that existing file

#### Scenario: Input and destination alias
- **WHEN** input and output refer to the same existing file, including a detectable filesystem alias
- **THEN** the CLI refuses the operation even with `--replace` and preserves the input

#### Scenario: Unsuitable destination
- **WHEN** the destination is a symlink, its parent is missing, or a destination WAL is detected
- **THEN** the application refuses the operation and preserves existing files; a candidate already finalized before refusal is retained under the publication failure rule

### Requirement: Isolated same-directory build

The pipeline SHALL allocate a uniquely named temporary DuckDB candidate in the
destination directory. All run-owned database migrations, writes, and required
verification SHALL target that candidate. The requested destination SHALL NOT
be used as a partially built database or placeholder file.

#### Scenario: Successful new output
- **WHEN** the destination is absent and required processing succeeds
- **THEN** the final destination first becomes visible as the completed, verified database at publication

### Requirement: Finalized non-destructive publication

Before publication, the application SHALL finalize database state and close all
candidate connections. Publication SHALL use a same-directory filesystem
operation with atomic destination visibility, not a cross-filesystem copy or
delete-then-rename sequence. Without `--replace`, publication SHALL refuse to
overwrite a destination that appears after preflight. With `--replace`, the
destination SHALL transition directly from the old file to the verified new
file if replacement succeeds.

#### Scenario: Late destination collision
- **WHEN** a destination appears after initial validation and `--replace` is absent
- **THEN** publication fails non-zero without overwriting it, and the verified candidate is retained with its path reported

#### Scenario: Successful replacement
- **WHEN** the candidate is verified and finalized and the replacement operation succeeds
- **THEN** the requested path contains the new standalone database, with no interval in which the application explicitly deleted the old destination first

### Requirement: Processing failure cleanup

Failures in extraction, transformation, migration, loading, verification, or
database finalization SHALL prevent publication and trigger cleanup of the
run-owned incomplete candidate and its transient database artifacts. Existing
destination content SHALL remain unchanged. Cleanup errors SHALL NOT mask the
primary failure and SHALL report any leftover candidate paths.

#### Scenario: Required processing fails
- **WHEN** a required processing step fails before publication readiness
- **THEN** the CLI exits non-zero, removes incomplete run-owned artifacts when cleanup succeeds, and leaves any existing destination unchanged

### Requirement: Recoverable publication failure

If final publication fails after successful verification and finalization,
the application SHALL retain the verified candidate, report its path and the
publication error, and exit non-zero. It SHALL NOT delete or modify the existing
destination as a fallback, and SHALL NOT silently copy across filesystems.

After successful publication, housekeeping or optional summary errors SHALL
warn without deleting the published output or changing its successful result;
warnings identify any leftover run-owned path. This is not a publication failure.

#### Scenario: Destination is locked or replacement is denied
- **WHEN** the filesystem refuses final replacement of an existing destination
- **THEN** the old destination is unchanged and the finalized verified candidate remains available at the reported recovery path
