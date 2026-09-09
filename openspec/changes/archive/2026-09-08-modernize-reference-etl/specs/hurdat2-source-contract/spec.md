## Purpose

Keep local HURDAT2 ingestion and normalized data meaning stable while clarifying
the boundary between source access, transformation, and output acceptance.

## ADDED Requirements

### Requirement: Local HURDAT2 ingestion boundary

The application SHALL ingest the existing local UTF-8 HURDAT2 text format,
including heterogeneous header and observation records. The existing extract
stage satisfies the source boundary; no new adapter or interface is required.
Field whitespace trimming, record order, and existing parsing
behavior SHALL be preserved. Access, decoding, and CSV-reader errors SHALL fail
with a source diagnostic. Readable unrecognized or malformed records SHALL
remain subject to the existing transformation rules, not a new whole-file
format-rejection policy. This change SHALL NOT introduce remote sources, new
formats, or a source registration/plugin mechanism.

#### Scenario: Existing input fixture
- **WHEN** the current local HURDAT2 fixture is read through the source boundary
- **THEN** the ordered, trimmed source fields supplied to transformation match the baseline behavior

#### Scenario: Input read failure
- **WHEN** the input cannot be opened or reading fails during iteration
- **THEN** processing fails non-zero and no destination output is published

#### Scenario: Readable unrecognized records
- **WHEN** the source reader successfully yields records that the baseline transformation rules skip
- **THEN** those rules still determine accepted loader inputs, including potentially empty collections, rather than a new source-format detector failing the run

### Requirement: Preserve normalized data meaning

Structural and tooling changes SHALL preserve normalized values for existing
valid inputs: storm identity, timestamps, hemisphere conversion, WKT
longitude/latitude order, missing-value handling, wind speeds in knots and
derived mph, pressure in millibars, and wind radii in nautical miles. The
existing mph rounding, invalid-record handling, and last-wins storm
deduplication SHALL NOT be silently replaced by new rules.

Before modernization or runtime restructuring, a representative round-trip
test SHALL establish these values against independently reopened baseline
output using explicit expectations. The same value assertions SHALL remain
valid after migration-backed loading and safe publication are introduced.

#### Scenario: Stable representative values
- **WHEN** a valid record contains `25.0N`, `75.0W`, and wind speed 100 knots
- **THEN** normalization retains latitude 25.0, longitude -75.0, `POINT(-75.0 25.0)`, and derived wind speed 115.1 mph

#### Scenario: Independently reopened output
- **WHEN** the representative fixture is processed before and after implementation changes
- **THEN** an independent database read satisfies the same explicit timestamp, wind, pressure, distinct-radius, optional-null, and WKT expectations, not merely matching row counts

#### Scenario: Existing missing and time handling
- **WHEN** baseline parsing maps an optional missing marker to null or defaults a malformed time representation to midnight
- **THEN** this change retains that normalization behavior rather than introducing a new rejection or substitution policy

#### Scenario: Duplicates and rejected source records
- **WHEN** transformation skips invalid observations or deduplicates storm identifiers
- **THEN** the accepted loader inputs retain the baseline decisions and expected persistence counts exclude records not submitted to loading

### Requirement: Separate normalization from geographic acceptance

The pipeline SHALL NOT introduce clamping, wrapping, coordinate substitution,
or other repair to make output pass geographic verification. Values produced
by the existing normalization rules SHALL be assessed unchanged by the
persisted-dataset verification contract.

#### Scenario: Previously permitted longitude outside output bounds
- **WHEN** existing normalization produces longitude 270.0 from `270E`
- **THEN** that value is not rewritten to -90.0 and an output containing it fails geographic verification rather than being published
