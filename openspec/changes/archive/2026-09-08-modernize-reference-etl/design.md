## Context

See [proposal.md](proposal.md) for motivation and scope. The source baseline is
`904d8e7`; the supplied [diagram](assets/ETL_diagram_v2.png) is a target reference,
not evidence of implemented behavior. Its SHA-256 is
`1b497b52d350f29dceea8fae9d48fd51617214ffaafdfd852882fb63850e164b`.

Relevant observations from the existing implementation:

| Area | Observed behavior and design consequence |
| --- | --- |
| `pyproject.toml`, `.pre-commit-config.yaml` | Poetry/Python 3.12/mypy; Ruff versions differ between project and hooks. SQLAlchemy is used directly but arrives transitively. Preserve the src-layout distribution and declare direct runtime dependencies. |
| `cli.py:82-99,142-163,170-201` | Deletes output first, builds an engine on the final path, deduplicates storms and flattens observations, then reports success before the summary. Move lifecycle ownership to an explicit build/verify/publish sequence. |
| `core.py:17-51` | Text FileHandlers configured by stage construction. Configure structured logging once per run rather than multiplying handlers. |
| `extract/extract.py:57-73,108-142` | Local-file CSV iteration with trimmed fields. Retain this boundary unless a concrete need justifies a small helper. |
| `transform/parser.py:116-179`, `transform/models.py:43-93,142-147` | UTC parsing, existing missing/time defaults, hemisphere rules, and rounded mph. Characterize these before refactoring; do not conflate acceptance with normalization. |
| `load/unit_of_work.py:109-137` | Calls `create_all()` and closes the session only after successful commit/rollback. Remove implicit schema creation and guarantee closure even when commit fails. |
| `load/models.py:80-126`, `load/load.py:241-269` | IDs are assigned per full-refresh load; geometry is `POINT(longitude latitude)` WKT text. Keep this representation; no additional coordinate columns or native geometry migration are required. |
| `migrations/env.py`, `alembic.ini` | Migrations build their own engine from repository configuration and reset logging. Inject the candidate connection and avoid repository-relative defaults during a CLI run. |
| `migrations/versions/` | Two revisions create the schema then rename `location_wkt` to `geom`. Exercise the real chain, not just ORM metadata. |
| Integration tests | CLI fixture checks 2 storms/34 observations; load tests bypass Alembic. Spatial distance comments and expectations disagree about axis semantics. New integrity tests must use independently justified expectations. |

There are no existing main OpenSpec capability specs to amend. The new delta
specs establish reviewed contracts for the selected behavior, not a complete
retrospective specification of every parser edge case.

## Goals / Non-Goals

**Goals:**

- Make the candidate database the unit of output publication, independent of
  transaction boundaries within migrations or loading.
- Keep resource ownership and failure classification simple enough to teach.
- Use independent persisted checks against immutable loader-boundary counts.
- Keep CLI, source, normalization, persistence, and presentation boundaries
  explicit without introducing a service/container/plugin architecture.
- Deliver coherent, testable increments under the capability contracts.

Handle an edge case explicitly when it risks data loss/corruption, is plausible
in ordinary local use, or is necessary for the normal path to work. Otherwise
document the limit and fail clearly. A safety rule does not imply a new
abstraction or an end-to-end test for every variation.

**Non-Goals:**

- See proposal non-goals. In particular, existing destination databases are
  never operational migration inputs to an ETL run.
- No new geographic canonicalization, new CRS, geodesic analytics framework,
  schema representation change, or data repair in the verifier.
- No guaranteed power-loss durability, distributed/network-filesystem
  transaction protocol, hostile-directory defense, or multi-writer scheduler.
  The safety contract covers ordinary local filesystem operations, errors, and
  late destination collisions; filesystem support is checked by the operation.

## Decisions

### 1. Validate the working baseline before changing it

Use the current tools and schema path to establish a representative fixture
round trip before runtime or toolchain changes. Independently reopen output
and assert explicit UTC timestamp meaning, knots/rounded mph, pressure,
distinct radii, optional nulls, and longitude/latitude WKT. Keep these value
assertions through migration and publication changes; do not derive expected
values from the loader or spatial function being tested.

Reuse existing parser/model tests, adding only missing representative coverage
for normalization and the actual deduplicated loader inputs, including empty
inputs. Isolate test working directories/logs and close test-owned engines.
If baseline values fail independent expectations, investigate before treating
them as the reference; do not silently bless a defect or expand parser scope.
Waiting until final acceptance for this oracle is rejected because it makes
regressions across unrelated changes harder to locate.

### 2. Keep the orchestration explicit and the source boundary local

Retain stages, the local extract implementation, Pydantic models, repository/UoW
persistence, materialized lists, and Typer/Rich UI. No source adapter or interface
is required. Extract a function only if it simplifies actual code or a necessary
test; generalized readers, streaming, and parser redesign are not this change.

Capture two expected counts immediately before loading, after last-wins storm
deduplication and observation flattening. They are independent of loader returns
and raw source totals. Preserve skipped-record decisions and zero accepted
counts. Keep existing normalization, including `round(knots * 1.15078, 1)`;
geographic acceptance can reject `270E -> 270.0` without changing that parser rule.

### 3. Build with migrations, then commit and verify

The orchestrator owns the candidate engine lifecycle. Invoke Alembic
programmatically with its migration location resolved from installed package
resources and the run's candidate connection supplied through configuration
attributes. Adapt `env.py` to use that connection without resetting application
logging or opening `data/hurdat.duckdb`. The packaged path must work in an
installed wheel and outside the repository. `alembic.ini` can remain an explicit
developer entry point, but it is not required by normal CLI execution.

Exercise the existing two revisions before replacing runtime `create_all()`.
Check fresh-to-head schema/constraint parity and one initial-to-head rename
with representative stored values. Retain revision identities and correct
dialect issues only when execution proves necessary; no general upgrade service
or compatibility matrix is required for new-database full refreshes.

Remove `Base.metadata.create_all()` from normal UoW entry. Schema creation is
an explicit prerequisite, not a repository side effect. Migration integration
fixtures must use the same initialization path; existing unit tests can retain
appropriate session mocks. Establish spatial extension availability on the
connections that need it; install only if absent, load per connection, and fail
clearly if setup cannot complete.

Load and commit in the existing UoW transaction. Fix exception-safe cleanup,
including partial entry and commit failure, rather than adding a transaction
manager. Dispose candidate resources at the owning boundary and use the existing
`ETLError` family for useful failure diagnostics.

Alternative: use `create_all()` followed by an Alembic stamp. Rejected: it labels
an unexecuted migration path as valid and cannot prove migration-owned schema.

### 4. Verify persisted results, not loader counters

Use a fresh session/connection on the committed candidate to perform the slim
checks in [the verification spec](specs/persisted-dataset-verification/spec.md):

- Query actual counts and compare against the immutable loader-boundary values.
- Parse every WKT `geom` with DuckDB Spatial, require a nonempty point, and
  obtain X as longitude and Y as latitude. Reject missing/non-finite ordinates
  and require inclusive longitude [-180,180] and latitude [-90,90].
- Use that geometry query to establish spatial readiness, with a constant known
  point only for empty observations. Do not add a separate readiness subsystem
  or a geodesic-distance acceptance threshold.

Use compact aggregate SQL, not a per-record Python validation pipeline. Query
and parse failures prevent acceptance; diagnostics identify the check and
counts rather than raw records. Rely on tested migration constraints for nulls
and relationships. Duplicate runtime audits, invalid-table fixture machinery,
and an extensible invariant registry add little value to a fresh controlled
database. Round-trip tests protect value semantics; runtime verification is
not a full source checksum or a general data-quality framework.

### 5. Publish only finalized candidates

Use this state model in CLI orchestration:

```text
preflight --> candidate --> migrate --> extract/transform/load
                                            |
                                            v
summary <-- published <-- publish <-- finalize/close <-- verify

preflight failure: no processing
processing/verification/finalization failure: cleanup incomplete candidate
publication failure: retain finalized verified candidate
post-publication summary/cleanup failure: warn, keep successful output
```

Use a few functions and explicit ownership, not a lifecycle class hierarchy.
Preflight requires accessible input, an existing parent, replacement consent,
and distinct input/output identities. Reject symlink/non-regular destinations
without resolving them to replacement targets. Refuse destination WAL state at
preflight and again before publication; never adopt or remove an old WAL.
These are inexpensive protections against data loss, not writer coordination.

Create a uniquely owned sibling candidate. Close any reservation handle before
DuckDB opens it; a zero-byte reserved file is not a valid DuckDB database.
After committed-data verification, checkpoint only as needed and close/dispose
all candidate resources. A focused real-DuckDB test establishes that the result
is standalone without its WAL; no extra transaction layer is needed.

Use `os.replace` with explicit consent, or `os.link` followed by temporary-name
removal without consent. The latter refuses late collisions atomically; an
existence check followed by replace is not safe. Unsupported or denied operations
fail clearly, with no copy, forced access, or delete-first fallback.

Before readiness, clean only run-owned incomplete artifacts and preserve the
primary error if cleanup fails. After readiness, publication failure retains
the candidate and reports its path. Mark publication successful immediately
after replace/link succeeds; subsequent redundant-name cleanup or summary errors
only warn. Recovery is manual. Abrupt termination can leave files; automatic
sweeping, retries, and combinations of secondary-error defenses are out of scope.

### 6. Modernize tools as a separate increment

Retain the chosen target: Python `>=3.13,<4` with a 3.13 pin, PEP 621/Hatchling,
uv with a generated committed lockfile, and BasedPyright `standard` for source
and tests. Declare directly used SQLAlchemy; add structlog with logging work.
Replace Poetry/mypy only when their successors are verified. Use narrow justified
typing exceptions, not blanket ignores or unrelated runtime rewrites.

Keep the Python/uv switch separate from a subsequent runtime-library refresh so
compatibility regressions have a bounded cause. Review Pydantic, Rich, Typer,
Alembic, DuckDB, duckdb-engine, SQLAlchemy, and necessary runtime transitives for
current stable Python 3.13-compatible releases. Record before/after versions,
review declared ranges and major-version compatibility, and justify holds;
generate the lock with uv without unrelated dev/build-tool churn. Make only
necessary compatibility edits and retain independent value, migration/schema,
Spatial, transaction, and publication assertions. This is not a schema redesign
or an upgrade service for old output databases.

The refresh follows the locked Python/uv baseline and precedes installed-wheel
acceptance and the final bounded cleanup review. CI and final acceptance must
validate the upgraded dependency set rather than reuse pre-upgrade evidence.
It does not depend on typing/hook modernization or structlog adoption; run the
applicable current quality gates and distinguish existing lint debt from new
regressions. Preserve the full suite's 80% branch-coverage gate.

Align Ruff and local hooks with locked project tools; CI checks do not fix files.
Preserve 80% branch-measured coverage. Linux runs the full quality/test suite and
one installed-wheel ETL smoke outside the checkout; Windows runs targeted real
DuckDB and file-lifecycle coverage on Python 3.13. Provision Spatial explicitly
and let installation/check failures fail CI. No exhaustive platform/failure
matrix or separate CI fault-injection harness is needed.

The current toolchain can validate and deliver safety fixes. Modernization is
a deliberate usability choice, not a runtime prerequisite. Basic CI can use
current commands first; switch it when the toolchain changes rather than
maintaining two permanent toolchains.

### 7. Separate structured diagnostics from optional summary

Configure structlog once at CLI entry with a stdlib bridge for existing library
logs, level filtering, timestamp, severity, event, run ID, and bound stage or
operation context. Use JSON lines in the existing `logs/pipeline.log` location
and Rich for console presentation. Reset run context and avoid duplicate
handlers when tests invoke the CLI repeatedly. Do not log full source records
or credentials by default; log IDs/counts and exception context as needed.

Emit overall success only after publication; summary queries run afterward on
a separately closed read connection and failures warn without changing success.
Fix this lifecycle independently of JSON logging. Preserve the primary error
and use plain stderr if presentation fails, not a new reporting/recovery layer.
Summary-as-verification is rejected because it mixes optional and required work.

### 8. Test distinct behavior at the lowest useful layer

Keep parser characterization, real migration/schema tests, UoW failure tests,
compact verifier tests, and publication-helper tests separate. Use closed fixture
files for collision/permission/cleanup behavior and injected failures for CLI
wiring. Do not replay every low-level failure through a migrated full ETL.

The small end-to-end suite covers value-correct new/replacement output, existing
output refusal, one processing failure preserving old output, and one publication
failure retaining a reopenable candidate. Platform tests target actual filesystem
and handle differences, not a stage-by-platform cross-product. Existing tests
count as coverage; do not manufacture duplicate cases to match checklist wording.

### 9. Polish source without adding weight

After relevant tests establish behavior, use `code-simplifier` selectively on
touched source and directly related legacy implementation. Prefer clearer
control flow/names and removal of duplication, dead code, or unnecessary helpers.
The primary agent bounds the scope, reviews the diff for exact behavior
preservation and unnecessary abstraction/SLOC growth, and reruns the baseline
assertions and relevant quality checks. Do not impose a line-count target or
manufacture edits when review finds no worthwhile simplification.

Scoped passes can accompany implementation; a final bounded cleanup review
revisits the earlier implementation after runtime, typing and diagnostic changes.
Use one editing agent at a time. Unrelated improvements return to the primary
agent rather than expanding into a repository-wide rewrite; final acceptance
reruns the full gates after cleanup.

## Risks / Trade-offs

- [New Python/dependency compatibility] -> Resolve and lock on Python 3.13;
  validate Linux quality/wheel use and targeted Windows runtime behavior.
- [Untested Alembic chain and DuckDB DDL limits] -> Add focused fresh/upgrade
  integration tests before removing runtime `create_all`; use intended-schema
  parity as the check, not revision markers alone.
- [File locks and engine pooling] -> Explicit commit/checkpoint/closure,
  Windows tests, and a retained verified candidate on final replacement failure.
- [Space consumption] -> A replacement temporarily requires room for both
  databases and transient state. Disk-full failures preserve the old output;
  error reporting identifies any leftover files.
- [Filesystem atomicity/support] -> Same-directory operations on supported local
  filesystems; fail safely without a copy fallback. Atomic visibility is not a
  promise of power-loss durability or network-filesystem semantics.
- [Normalization edge cases] -> Preserve parsing values and test the explicit
  acceptance gate separately, including positive longitudes above 180 and NaN.
- [Broad type-check migration] -> Standard diagnostics with targeted exceptions;
  do not suppress the whole pipeline or repair runtime semantics opportunistically.
- [Test cost] -> Keep the full-suite coverage gate, isolate logs/output paths,
  and inject failures at the relevant boundary instead of duplicating full runs.

## Migration Plan

1. Establish the independently reopened value baseline and test isolation using
   current tools. Add missing focused characterization, not a parser rewrite.
2. Exercise packaged migrations before making them authoritative; then integrate
   resource cleanup, compact verification, and safe publication. Keep the baseline
   value assertions and add representative failure coverage.
3. Deliver tooling/CI and structured logging in isolated increments after baseline
   validation. They do not block step 2 and require no source-boundary refactor.
4. Run the layered suite and installed-wheel smoke; document replacement, manual
   recovery, geographic acceptance, space needs, and the local operating limits.
5. Reconcile verified Beads outcomes with the checklist, validate the change, and
   perform separately authorized sync/archive. Update current-state docs only
   for implemented increments; the target diagram remains here until reviewed.

The rollout does not mutate old output databases. Operators keep using them
until explicitly replacing them; no data-upgrade/backfill step is required.
Source rollback uses a reviewed revert or an earlier release, not rewritten
Git history. Reverting to the old CLI reintroduces its destructive behavior and
must be called out. Once `--replace` succeeds, this application does not retain
an automatic backup of the old destination; operators requiring rollback of
the data artifact must preserve a separate copy before replacement.

## Open Questions

- Exact compatible dependency/tool pins, CI action revisions, and stable check
  display names will be chosen and recorded during implementation. They do not
  change the Python/platform baseline, check policies, or runtime contracts.
- The PNG is the only supplied diagram artifact. An editable source can be
  added later without blocking specification review or implying a new design.
