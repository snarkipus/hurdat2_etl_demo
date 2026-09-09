# Maintainer releases

Release notes: [v0.2.0](v0.2.0.md).

This is a manual, repeatable procedure, not authorization to publish. Follow
[repository workflow](../agentic-workflow.md) and explicit approval for source
commits, pushes, PRs, tags, releases and separate Beads synchronization. Beads
owns execution state and evidence; this document is not a second release tracker.
Do not publish to PyPI or add a release framework as part of this procedure.

## Prepare and verify

On the release feature branch, update `project.version` in `pyproject.toml`, run
`uv lock` without upgrade flags, and inspect the lock diff: only the local project
version should change, not dependency selections, hashes or sources. Write
versioned notes and review the entire branch diff. Run:

```bash
uv sync --locked
uv run --locked ruff check src tests
uv run --locked ruff format --check src tests
uv run --locked basedpyright
uv run --locked pytest
uv run --locked python tests/installed_package_smoke.py --temp-root /tmp
git diff --check
```

Confirm installed metadata matches the intended version. The smoke builds a
wheel **from the sdist**, rejects members outside the explicit sdist allowlist
before building that wheel, verifies packaged migration resources, and tests an
isolated installed CLI against `tests/baseline.py`; its temporary artifacts are
deleted. It does **not** validate a separately built/uploaded release wheel.
Use Python 3.13 without `-O` or `PYTHONOPTIMIZE`; package/Spatial failures are
failures, not skips. Run manual CLI probes outside the checkout to isolate logs.

With authorization, commit, push and merge the reviewed preparation PR only
after both hosted jobs pass: **Linux quality and wheel** and **Windows runtime
and publication**. Fast-forward a clean local `main` to the merged revision.
Record its full SHA and successful hosted runs for that exact main revision
(not just the PR head); repeat the local gates on that revision. Stop if main
or tracked contents change; earlier tests do not cover a different revision.

## Tag and assemble exact-revision artifacts

Substitute the approved version below. Use a fresh empty artifact directory
outside the checkout (`$OUT`, an absolute path), and a separate scratch directory.
Build release artifacts from a fresh clone of the exact approved revision,
without initializing Beads or installing agent tooling/local state. A clean
`git status` alone does not exclude ignored private files from packaging.
Verify the release tag does not already exist locally or remotely and review all
existing tags before publication. Do not move or overwrite a published tag.
With tag authorization and the tested SHA recorded as `$SHA`:

```bash
git tag -a v0.2.0 "$SHA" -m "Release v0.2.0"
git rev-parse 'v0.2.0^{commit}'
git archive --format=tar.gz --prefix=etl-pipeline-0.2.0/ --output="$OUT/hurdat2_etl-v0.2.0-source.tar.gz" v0.2.0
git bundle create "$OUT/hurdat2_etl-v0.2.0.bundle" refs/heads/main --tags
git bundle verify "$OUT/hurdat2_etl-v0.2.0.bundle"
git bundle list-heads "$OUT/hurdat2_etl-v0.2.0.bundle"
uv build --out-dir "$OUT"
```

Require `HEAD`, `refs/heads/main`, remote main and the peeled annotated tag to
equal `$SHA`. `git archive` includes only tracked source at that tag, not the
working directory's logs, databases, virtual environment or build leftovers;
inspect its listing and any archive attributes before release. The bundle uses
full reachable history, without exclusions/prerequisites. Its advertised refs
must be exactly `refs/heads/main` and the reviewed complete `refs/tags/*` set:
never use `--all`, topic branches, remote-tracking refs or `refs/dolt/data`.
Historical commits reachable through main/tags remain intentionally included.
Clone the bundle into scratch with `git clone --branch main <bundle> <scratch>`;
check main/tag SHAs, all tag refs and `git fsck --full`. No existing repository
objects should be required. Inspect the source tarball against the tagged tree.

Default `uv build` builds the sdist then builds the wheel from it. Require exactly
`etl_pipeline-0.2.0.tar.gz` and `etl_pipeline-0.2.0-py3-none-any.whl` as the Python
artifacts, and inspect their PKG-INFO/METADATA versions, Python requirement,
entry point and packaged migrations. Do not rebuild after acceptance/hashing.

Review **every sdist member** against `pyproject.toml`'s `only-include` boundary:
`src/etl_pipeline/`, `tests/`, `README.md`, `LICENSE`, `pyproject.toml`, `uv.lock`,
and `.python-version`, plus Hatchling-generated `PKG-INFO` and its automatically
included root `.gitignore`, beneath the single versioned archive root. Hatchling
1.27 force-includes that ignore file even with `ignore-vcs = true`; no other
ignore files are permitted. Require local caches/bytecode to be absent. Run
`validate_sdist` from `tests/installed_package_smoke.py` against the **actual
release sdist**, without optimized Python, before accepting assets. Unexpected
paths, links, Beads state/backups/interactions, agent dependencies, or arbitrary
private directories block release; do not extract or inspect private contents.
Nested `.gitignore` files are not a packaging security boundary. The separate
Git source archive retains tracked documentation/reference material; the lean
sdist intentionally omits it. Repeat member validation on the downloaded sdist.

## Validate the actual wheel and hash assets

For the wheel that will actually be uploaded, repeat the existing smoke's
installation/probe sequence manually in fresh scratch, **without its build
step**. Use absolute paths, clear inherited Python/editable/coverage injection,
and keep scratch outside the source checkout:

1. Export from the exact release checkout with `uv export --locked --no-dev
   --no-emit-project --output-file <scratch>/runtime.txt` (retain hashes).
2. Create a fresh `uv venv --python 3.13 <scratch>/venv`; install dependencies
   with `uv pip sync --python <venv-python> --require-hashes <runtime.txt>`.
3. Install the exact release wheel using `uv pip install --python <venv-python>
   --no-deps <absolute-release-wheel>` and run `uv pip check` against that Python.
4. Run the existing `PROBE` from `tests/installed_package_smoke.py` with `-I`,
   passing the checkout path; additionally require
   `importlib.metadata.version('etl-pipeline') == '0.2.0'`. Verify imports resolve
   inside the fresh environment and not to the checkout or an editable install.
5. Copy only `tests/unit/data/test_data.txt` and `tests/baseline.py` into scratch.
   From scratch run the installed console script through the environment's
   Python `-I` for `--help` and fixture ETL (no repository Alembic config).
   Invoke `assert_baseline` from the copied oracle as in the smoke. Require
   independent values/migration state, no WAL or candidate leftovers, and the
   expected CLI options. Retain command results and the tested wheel's SHA-256.

Generate `SHA256SUMS` in `$OUT` over **exactly** the source archive, bundle, wheel,
sdist and separately approved Beads archive below, using their relative basenames
(for example `sha256sum <five explicit
basenames> > SHA256SUMS` from that directory). Do not hash the manifest itself,
logs, scratch files or unrelated builds. Run `sha256sum -c SHA256SUMS`; compare
the wheel hash with the actual-wheel acceptance record. Record SHA, tool versions,
tests and hashes in the release Bead, never private logs or credentials.

## Beads history for rehosting

Publishing Beads history requires explicit approval because issues and historical
content may be sensitive. For this release it is approved as a separate public
asset, `hurdat2_etl-v0.2.0-beads.tar.gz`. Never package the active embedded database
or incidental local backups. Use the installed `bd` backup interface (validated
with Beads 1.2.2), with one writer and a fresh local output directory:

```bash
bd --sandbox backup status --json
bd --sandbox backup init /absolute/scratch/beads-backup
bd --sandbox backup sync
bd --sandbox backup status --json
```

Check the configured destination is the intended local path before syncing.
Initialization replaces the default backup registration; preserve an existing
registration deliberately rather than overwriting it unreviewed. Sync can make
a Beads-managed pre-backup commit. Wrap only the complete generated backup output
in a tarball, preserving hidden entries. This is an exported backup, not a source
Git bundle or JSONL snapshot. Include its hash in `SHA256SUMS`.

Verify restore in a disposable clone first. Before any Beads initialization,
remove the cloned public `sync.remote` setting and set `dolt.local-only: true`;
keep auto-push, backup publication and export staging disabled. Point the clone's
Git origin at a local/offline location during verification, not public GitHub.
`--sandbox` disables auto-push but is not a network isolation guarantee.

```bash
bd --sandbox init --prefix etl --non-interactive --role maintainer --skip-hooks --skip-agents
bd --sandbox backup restore /absolute/scratch/unpacked/beads-backup --force
bd --readonly --sandbox dolt remote list --json
```

Use `--force` only against that disposable initialized database. Initialization
may create a local Git commit and rewrite local sync configuration; inspect it
again after restore. Compare issue details, dependencies, comments and per-issue
`bd history --limit 0` with the source via `bd`, never raw database access. This
verifies the history exposed by Beads, not an independent database-wide audit.
Configure the internal Git and Beads remotes explicitly before normal use;
do not blindly run bootstrap/pull against inherited public settings. Remote refs
must be reviewed before pushing `main` or enabling cross-machine writes.

Restore may register its input directory as a backup destination; do not sync
into the only downloaded copy. On the source, use `bd backup remove` to unregister
the temporary destination afterward if none was configured before the export;
this leaves the generated backup intact. Record the snapshot cut: final release
closure necessarily occurs after the snapshot and is synchronized separately.

## Draft, download, publish

With approval, push only the new tag (`git push origin refs/tags/v0.2.0`), verify
its remote object and peeled SHA, then use `gh release create v0.2.0 --verify-tag
--draft --title "v0.2.0" --notes-file docs/releases/v0.2.0.md` with the six
explicit asset paths, including the Beads archive and manifest.
Do not upload globs that can include unrelated files.
Confirm the draft targets the tested tag and has exactly the intended assets.

Download draft assets as an authenticated maintainer with `gh release download
v0.2.0 --dir <fresh-download-directory>`. Check downloaded `SHA256SUMS` against
the trusted local manifest, then run `sha256sum -c SHA256SUMS` there. Verify the
downloaded bundle can clone, source archive contents match, and repeat the
actual-wheel acceptance above using the **downloaded** wheel. Do not substitute
a rebuild or editable install. Resolve any discrepancy before publication.

Publish with `gh release edit v0.2.0 --draft=false` only after all checks pass.
Download again into another empty directory and verify the public asset set,
manifest and every hash. Record the release URL and exact evidence in Beads.
Only then close the release Bead and perform separately authorized `bd dolt
push`/handoff verification. Beads history is in its separate approved backup, not
the source archive or source bundle; never include active embedded database files
or transport refs in those artifacts.
Finish on clean `main`, reporting any pending remote synchronization rather than
claiming that source publication backed up Beads.
