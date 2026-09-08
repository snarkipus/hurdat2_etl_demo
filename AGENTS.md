# Repository Instructions

## Workflow Authority

- Root `AGENTS.md` is canonical. `CLAUDE.md` is only a compatibility pointer. Executable configuration/code determines actual tool behavior.
- OpenSpec (`openspec/`, schema `spec-driven`) owns specification and intent; Beads owns execution state. Keep status, blockers, and claims in Beads rather than maintaining a second execution tracker in planning artifacts.
- Work sequentially in one checkout on feature branches. `main` is protected; do not implement directly on it or use parallel implementation agents/worktrees.
- Use the conservative Beads profile below: no commits, pushes, or Dolt remote synchronization without explicit authorization. A request limited to named files also excludes Beads writes and other incidental file changes.
- Read `docs/README.md` for documentation locations and authority, and `docs/agentic-workflow.md` for initialization, execution, branch handoff, and cross-machine recovery. Material in `docs/archive/` is historical evidence, not accepted OpenSpec requirements or an active backlog. Target-state diagrams belong with proposed OpenSpec changes until their scope is reviewed and implemented.
- Implementation is Beads-led: review `/opsx-propose` artifacts, then map `tasks.md` into a dependency-aware Beads graph, not necessarily one issue per checklist item. Do not use `/opsx-apply` or its skill as the normal implementation entry point; preserve the generated integration for tool updates.
- OpenSpec `tasks.md` is the sole exception to the managed block's Markdown-task prohibition: keep a spec-linked checklist with Beads IDs, not independent assignments, blockers, or live status. Reconcile it in the same session as verified Beads outcomes. Check an item only when its entire scope is satisfied; cancelled/superseded issues do not imply completion. Reconcile affected issues and checklist items after scope revisions, and perform a final consistency check before sync/archive.
- Repository policy overrides generated defaults: `dolt.auto-commit` is explicitly `off`, not per-write as the managed block below claims. Local writes persist in the working set. Explicit approval is required for `bd dolt commit` as well as Git commits and all remote synchronization. Do not enable auto-push, auto-backup publication, or automatic staging.
- Beads uses embedded mode and one active writer across machines. Its GitHub-backed remote is public: never store secrets or private logs in issues. Dolt history is separate from source branches; protecting `main` does not protect `refs/dolt/data`.
- Use the installed `bd` CLI for normal Beads operations, including inspection, issue/dependency changes, history, and authorized `bd dolt ...` commits/synchronization. Do not use raw `dolt` commands, direct SQL, database clients, or manual embedded-database file operations, even for read-only inspection. If `bd` cannot provide a necessary diagnostic or recovery operation, stop, explain the gap and proposed lower-level access, and obtain explicit approval for that specific operation. General permission to work on issues, commit, or sync does not authorize direct database access; this boundary also applies to delegated agents.
- This repository uses the `etl` issue prefix (for example, `etl-ej0`). The managed block's `bd-42` / `bd-123` IDs are generic placeholders; substitute actual `etl-...` IDs from `bd ready --json` or `bd list --json` when using those examples.
- OpenCode uses `@snarkipus/opencode-beads@0.10.0`, pinned in `.opencode/opencode.json`, for full `bd prime` context injection, `/beads:*` commands, and bounded `beads-task-agent` delegation. The installed `bd` CLI remains authoritative; this is not a Beads MCP server or a replacement for the managed integration below. If context injection is absent, run `bd prime` explicitly rather than reinitializing this workspace.
- Keep planning and orchestration in the primary thread. Delegate read-only Beads graph analysis or one existing Bead at a time to `beads-task-agent`; do not ask it to drain the backlog or start discovered work in the same invocation. Verify its result and reconcile the affected OpenSpec checklist before selecting the next issue. Embedded single-writer and explicit commit/sync/push approval rules still apply.
- Keep generated Beads markers and OpenSpec commands/skills intact; put repository overrides here and in `openspec/config.yaml`. Recheck integration after tool updates. The managed block's generic README/QUICKSTART links are upstream guidance; use our workflow guide locally.

<!-- BEGIN BEADS INTEGRATION v:1 profile:full hash:19cc25d9 -->
## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd?

- Dependency-aware: Track blockers and relationships between issues
- Git-friendly: Dolt-powered version control with native sync
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Quick Start

**Check for ready work:**

```bash
bd ready --json
```

**Create new issues:**

```bash
bd create "Issue title" --description="Detailed context" -t bug|feature|task -p 0-4 --json
bd create "Issue title" --description="What this issue is about" -p 1 --deps discovered-from:bd-123 --json
```

**Claim and update:**

```bash
bd update <id> --claim --json
bd update bd-42 --priority 1 --json
```

**Complete work:**

```bash
bd close bd-42 --reason "Completed" --json
```

### Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance (dependencies, tooling)

### Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (default, nice-to-have)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

### Workflow for AI Agents

1. **Check ready work**: `bd ready` shows unblocked issues
2. **Claim your task atomically**: `bd update <id> --claim`
3. **Work on it**: Implement, test, document
4. **Discover new work?** Create linked issue:
   - `bd create "Found bug" --description="Details about what was found" -p 1 --deps discovered-from:<parent-id>`
5. **Complete**: `bd close <id> --reason "Done"`

### Quality
- Use `--acceptance` and `--design` fields when creating issues
- Use `--validate` to check description completeness

### Lifecycle
- `bd defer <id>` / `bd supersede <id>` for issue management
- `bd stale` / `bd orphans` / `bd lint` for hygiene
- `bd human <id>` to flag for human decisions
- `bd formula list` / `bd mol pour <name>` for structured workflows

### Sync

bd stores issue history in Dolt:

- Each write auto-commits to Dolt history
- Use `bd dolt push`/`bd dolt pull` for remote sync
- Do not treat `.beads/issues.jsonl` as the sync protocol

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.

### Important Rules

- ✅ Use bd for ALL task tracking
- ✅ Always use `--json` flag for programmatic use
- ✅ Link discovered work with `discovered-from` dependencies
- ✅ Check `bd ready` before asking "what should I work on?"
- ❌ Do NOT create markdown TODO lists
- ❌ Do NOT use external issue trackers
- ❌ Do NOT duplicate tracking systems

For more details, see README.md and docs/QUICKSTART.md.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   bd dolt push
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.

<!-- END BEADS INTEGRATION -->


## Commands

Run from the repository root. Commands are verified against `pyproject.toml`, CLI code, and tests, not a claim that the current checkout passes all checks.

- Setup: Python `^3.12` (>=3.12,<4), then `poetry install` (includes dev tools). Preserve `poetry.lock`; it is generated, not hand-edited.
- CLI: `poetry run etl-pipeline --input /path/to/hurdat2.txt --output /path/to/new.duckdb`. This is a single-command Typer app: no `run` or `run-etl` subcommand.
- Lint: `poetry run ruff check src tests`; formatting check: `poetry run ruff format --check src tests`; apply formatting: `poetry run ruff format src tests`.
- Types: `poetry run mypy src tests` (`mypy_path = "src"`; tests have relaxed overrides).
- Full suite: `poetry run pytest`. Default options enforce 80% coverage with branch measurement and write `.coverage`, `htmlcov/`, and `lcov.info`.
- Focused test: `poetry run pytest --no-cov tests/unit/transform/test_parser.py::test_parse_cyclone_id`. Use `--no-cov` for focused runs to avoid the whole-project coverage gate; this is not a substitute for the full suite.
- Integration suite: `poetry run pytest --no-cov tests/integration`. DuckDB is embedded, but load/CLI tests execute `INSTALL spatial` and `LOAD spatial`; extension installation may require network access.
- Configured pre-commit order is Ruff fix/format, mypy, pytest. Hooks modify files; hook Ruff is pinned to `v0.9.7`, unlike Poetry's `^0.11.2`, so results can differ.

## Architecture & Gotchas

- `src/etl_pipeline/cli.py:app` is the entrypoint and orchestration layer, not `core.py`. It materializes extracted rows, transforms them, deduplicates storms by `storm_id` (last wins), and passes separate storm/observation lists to the loader.
- `core.py` supplies `ETLStage.execute()` -> `_process()`, shared logging, and Rich progress. Keep stage logic in extract/transform/load; use the `ETLError` hierarchy in `exceptions.py` for pipeline errors.
- `transform/models.py` contains Pydantic validation models; `load/models.py` contains distinct SQLAlchemy persistence models. `LoadStage` converts between them and receives an injectable Unit-of-Work factory; repositories share its session, committing on clean exit and rolling back on exceptions.
- Observation `geom` is a WKT **string**, not a native geometry column: `POINT(longitude latitude)`. Spatial queries convert it with `ST_GeomFromText`. Observation IDs are assigned by the loader, not database autoincrement.
- **The CLI deletes an existing output database before extraction.** Use a disposable/new output path, never an existing database that must be preserved.
- The CLI initializes schema with packaged Alembic migrations (`initialize_database(connection)`) before loading and owns disposal of both load and summary engines. UoW entry does not create or repair tables; callers must initialize schema explicitly. `alembic.ini` and the default UoW target `data/hurdat.duckdb`, while the CLI injects its requested output database.
- Stage construction writes `logs/pipeline.log` relative to the working directory. Tests isolate their working directories and close test-created file handlers so normal test runs do not delete the developer's log.
- Load integration tests use an in-memory DuckDB initialized by packaged migrations. CLI tests use `tests/unit/data/test_data.txt` and a temporary output DB, independently checking committed values and migration state after runtime disposal. Migration tests also verify the revision chain and ORM metadata parity.

## Local Conventions

- Follow `pyproject.toml` over prose: Ruff enables `E,F,B,S,I,UP`; migrations and `ref/` are excluded, and tests allow assertions. Mypy ignores errors in migration versions and is not blanket strict mode.
- Existing code mixes absolute and relative imports. Preserve the surrounding style rather than performing unrelated import rewrites.
