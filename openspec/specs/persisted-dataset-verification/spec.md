# persisted-dataset-verification Specification

## Purpose

Check committed row counts and spatial usability before publication, relying
on tested schema constraints rather than duplicating database validation.

## Requirements

### Requirement: Loader-boundary expected counts

The pipeline SHALL capture expected storm and observation counts from the
accepted collections actually submitted at the loader boundary, after
transformation, validation decisions, and deduplication. It SHALL compare those
expectations with independent counts queried from committed storage; loader
return counters or fixed NOAA dataset totals SHALL NOT substitute for the
persisted counts.

#### Scenario: Faithful persistence
- **WHEN** accepted loader inputs contain S storms and O observations and committed storage contains exactly S and O rows
- **THEN** the count checks pass regardless of how many original source rows were skipped

#### Scenario: Partial or extra persistence
- **WHEN** either persisted count differs from its loader-boundary expectation
- **THEN** verification fails and reports the expected and actual counts

#### Scenario: Empty accepted collections
- **WHEN** an otherwise successful transformation submits zero storms and zero observations
- **THEN** zero persisted counts satisfy count verification; no new nonzero-row threshold is imposed, and schema and spatial availability checks still run

### Requirement: Explicit geographic bounds without repair

Every persisted observation SHALL have finite latitude and longitude satisfying
latitude `[-90, 90]` and longitude `[-180, 180]`, inclusive. Bounds apply to the
unchanged normalized coordinates represented by stored geometry. Verification
SHALL NOT repair values or silently exclude offending rows to obtain a pass.

#### Scenario: Boundary coordinates
- **WHEN** a stored observation has finite coordinates at latitude -90 or 90 and longitude -180 or 180
- **THEN** the geographic bounds check passes

#### Scenario: Invalid coordinate output
- **WHEN** a stored observation has longitude 270, an out-of-range latitude, or a non-finite coordinate
- **THEN** verification fails and prevents publication without rewriting the value

### Requirement: Spatial interpretation of all observations

Verification SHALL use DuckDB Spatial to establish that every stored geometry
is interpretable as a nonempty point with usable longitude/latitude ordinates.
This check SHALL also establish spatial availability; a separate readiness
query is needed only when no observations exist. Verification SHALL fail on unavailable
spatial support, malformed WKT, empty or wrong-type geometry, or spatial query
errors. Checking one sample row SHALL NOT substitute for evaluating all stored
observation geometries.

#### Scenario: Queryable point geometry
- **WHEN** stored geometry is `POINT(-75.0 25.0)`
- **THEN** spatial verification interprets longitude as -75.0 and latitude as 25.0

#### Scenario: Malformed or non-point geometry
- **WHEN** any observation has unparseable geometry, an empty point, or a line instead of a point
- **THEN** verification fails rather than skipping that observation or converting the geometry

#### Scenario: Spatial support is unavailable
- **WHEN** the required spatial query cannot execute, including for an empty accepted dataset
- **THEN** verification fails instead of treating absence of observations as proof of spatial readiness

### Requirement: Verification is an output acceptance gate

All required checks SHALL succeed before the output is publishable. A failed
check or inability to execute a check SHALL produce a non-zero CLI result and
identify the failing invariant or query; the candidate SHALL be treated as an
incomplete processing artifact, not a recoverable verified output.

#### Scenario: Verification failure during replacement
- **WHEN** a candidate fails verification while replacing an existing destination
- **THEN** the old destination remains unchanged, the incomplete candidate is removed when cleanup succeeds, and the run exits non-zero
