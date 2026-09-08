This is the proposed implementation checklist, not the execution tracker. No
implementation Beads graph has been created during planning. After review, map
each item to real Beads IDs and record those IDs beside the item before execution;
reuse issues and allow many-to-many mappings. Assignments, dependency edges,
blockers, and live execution state belong in Beads. Reconcile these checkboxes
in the same session as verified outcomes, checking an item only when its whole
scope is satisfied. Revisions and cancelled/superseded issues require coverage
reconciliation, not automatic completion.

Baseline validation precedes implementation changes. Runtime safety work uses
the existing tools and does not depend on toolchain or logging modernization.
Migrations precede integrated verification/publication; low-level filesystem
tests do not need a database. Modernization is an independent increment after
baseline validation. Reuse existing tests and test distinct behavior at its
own boundary, not every combination through the CLI on both platforms.

## 1. Validate the Current Implementation

Contracts: [source semantics](specs/hurdat2-source-contract/spec.md),
[verification](specs/persisted-dataset-verification/spec.md).

- [ ] 1.1 Isolate test working directories/logs and close test-owned database resources; verify current fixture tests run repeatedly without touching the developer's log, using the existing toolchain.
- [ ] 1.2 Establish an independently reopened baseline round trip with explicit UTC timestamp, knots/rounded mph, pressure, distinct radii, optional-null, and WKT expectations; verify it passes on today's implementation before structural/tool changes, investigating rather than blessing unexpected values.
- [ ] 1.3 Reuse parser/extract tests and fill meaningful gaps for source read failures, skipped records, last-wins storm deduplication, empty loader inputs, and unchanged `270E -> 270.0`; verify accepted collections and known point ordinates without copying a spatial function's output as its expected value. No new source interface is required.

## 2. Migration-Owned Loading and Resource Safety

Contract: [migration-backed loading](specs/migration-backed-loading/spec.md).

- [ ] 2.1 Add packaged Alembic initialization with a supplied connection and no logging reset; verify fresh-to-head schema/constraint parity and one initial-to-head rename preserving representative values before replacing `create_all()`. Retain revision identities and test representative required-field/foreign-key enforcement and valid optional nulls.
- [ ] 2.2 Make migrations the explicit schema prerequisite, remove UoW schema creation, and fix exception-safe resource ownership using the existing transaction; verify migrated loading, missing-schema failure, and focused entry/body/commit/rollback cleanup paths without a new transaction manager.
- [ ] 2.3 Finalize and close candidate resources, checkpointing only as needed; verify with real DuckDB that an independently reopened standalone file retains committed values and migration state without an open writer or unpublished WAL.

## 3. Compact Persisted Verification

Contract: [persisted verification](specs/persisted-dataset-verification/spec.md).

- [ ] 3.1 Capture two loader-input count expectations and compare with independently queried committed counts; verify match, mismatch, and empty cases, demonstrating loader-return counters are not the oracle. Do not duplicate schema null/orphan enforcement with runtime audits.
- [ ] 3.2 Use compact DuckDB Spatial queries to check all geometries are nonempty points with finite, in-bounds longitude/latitude; verify representative valid/boundary and invalid geometry/coordinate cases, including 270E-normalized output, without repair. Reuse this query for spatial readiness, a constant point for empty inputs, and a focused query-failure test for fail-closed behavior; no separate readiness subsystem.

## 4. Safe Publication and Trustworthy Results

Contracts: [publication](specs/safe-output-publication/spec.md),
[success reporting](specs/pipeline-observability/spec.md).

- [ ] 4.1 Add `--replace` and concise preflight for destination consent, input aliases, unsuitable paths, and destination WAL; verify refusal preserves existing files before processing, using filesystem-level tests rather than full ETL runs for every path variation.
- [ ] 4.2 Route migrations/loading/verification to a uniquely owned sibling candidate and publish only after finalization; verify atomic explicit replacement and no-clobber creation, including a late collision and denied/unsupported operation, with closed-file helper tests and no destructive fallback.
- [ ] 4.3 Add straightforward incomplete-candidate cleanup and verified-candidate retention on publication failure; verify primary-error and leftover-path reporting at the owning boundary with representative injected failures, without stage-by-stage secondary-error combinations or automated recovery.
- [ ] 4.4 Mark publication success before redundant-name cleanup and optional summary; verify publication failure is non-zero while post-publication housekeeping/summary failure warns and preserves success. This lifecycle correction does not depend on adopting structlog.

## 5. Modern Development Tools

Contract: [development baseline](specs/development-baseline/spec.md).

- [ ] 5.1 Switch to Python >=3.13,<4 with a 3.13 pin, PEP 621/Hatchling, and a generated uv lockfile; declare SQLAlchemy directly and remove superseded Poetry configuration only after locked setup and the baseline value tests pass. Verify metadata rejects Python 3.12 and locked setup does not rewrite dependencies.
- [ ] 5.2 Include CLI and migration resources in wheel/sdist builds; verify `uv build` and install the wheel outside the checkout, reusing the representative fixture for one migrated, verified ETL smoke rather than a second acceptance matrix.
- [ ] 5.3 Replace mypy with BasedPyright standard diagnostics and narrow justified exceptions; verify source/tests pass without blanket ignores or unrelated runtime redesign.
- [ ] 5.4 Align Ruff and local hooks with locked project tools, correcting existing lint/format issues without value changes; verify non-fixing lint/format checks, a hook run, and the full suite's existing 80% branch-coverage gate.

## 6. Focused Continuous Integration

Contract: [automated checks](specs/development-baseline/spec.md).
CI can begin with current commands and switch with section 5; no dual-toolchain
support is required. Reuse the tests and installed-wheel smoke above.

- [ ] 6.1 Add PR/main-push Linux CI for locked non-fixing quality checks, the full coverage suite, and the installed-wheel smoke; verify workflow configuration and successful jobs with stable check names and no error suppression. A separate failing-CI demonstration harness is not required.
- [ ] 6.2 Add targeted Python 3.13 Windows coverage for real DuckDB creation/replacement and file-handle/publication behavior; verify green jobs with explicit Spatial setup and no skipped installation errors, not the full Linux failure matrix.

## 7. Structured Diagnostics

Contract: [observability](specs/pipeline-observability/spec.md).

- [ ] 7.1 Add structlog-backed JSON-line diagnostics configured once per run, retaining Rich presentation and bridging library logs; verify expected lifecycle/error fields, log-level filtering, parseable output, and repeated-run context/handler isolation with focused tests. Reuse section 4's success/failure behavior rather than creating a logging recovery framework.

## 8. Small End-to-End Acceptance and Handoff

Contracts: all six capability specs and the rollout in [design.md](design.md).

- [ ] 8.1 Reuse the baseline value assertions for published new/replacement output, plus CLI refusal, one processing failure preserving old output, and one publication failure retaining a reopenable candidate; verify exit status and relevant file outcomes. Low-level fault variants remain in boundary tests, not a full stage/platform matrix.
- [ ] 8.2 Update active usage/developer guidance for the implemented toolchain, logs, migration ownership, replacement, manual recovery, geographic acceptance, and local operating limits; verify commands/links and preserve historical material and the proposal diagram.
- [ ] 8.3 Run final locked quality/coverage gates and strict OpenSpec validation, and reconcile each checklist item's scope with verified Beads outcomes; verify green targeted platform jobs, unchanged tracked files after non-fixing checks, and no scope mismatch before separately authorized sync/archive/publication.
