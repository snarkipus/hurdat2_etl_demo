# Agentic Development Workflow

## Scope and Authority

`main` is the protected canonical DuckDB implementation, initially at `9af5d7a`.
Root `AGENTS.md` governs repository behavior; `CLAUDE.md` only imports it.
OpenSpec owns specification and intent. Beads owns task execution state.
Existing plans and reviews in `docs/` are historical reference, not an active
backlog or automatically accepted requirements. Adopt specs incrementally for
reviewed changes instead of inventing a retrospective specification.

This bootstrap does not change application code, Python tooling, dependencies,
logging, migrations, CI, or pre-commit hooks. The previous baseline verification
passed 111 tests (83.57% coverage) and mypy; five Ruff E501 violations in
`src/etl_pipeline/transform/transform.py` remain separate follow-up work.

## Tooling

Bootstrap versions: OpenCode 1.18.29, OpenSpec 1.12.0, Beads 1.2.2, Dolt 2.3.2,
and Node.js 24.20.0. These are external tools, not Poetry dependencies.
Use compatible versions on other machines; review schema migrations before
upgrading Beads. Do not silently upgrade global tools during project work.

The original initialization commands were:

```bash
openspec init --tools opencode --profile core
bd init --prefix etl --non-interactive --role maintainer --skip-hooks --skip-agents
bd setup opencode
```

OpenCode's `/init` was then invoked using `opencode run --command init`, scoped
to `AGENTS.md`. Its generated guidance was reconciled with Beads and OpenSpec.
No OpenCode plugin, MCP server, or `opencode.json` is required for this setup.
OpenSpec supplies six commands and six skills under `.opencode/`.
Restart OpenCode after initialization or updates so new guidance is loaded.

Do not repeat `bd init` on another machine to recreate existing issue history.
Use the fresh-clone procedure below instead. Do not reinstall Beads Git hooks:
this repository already has pre-commit hooks, which remain unchanged.

## Branch Workflow

Use one checkout and one active implementation session. Parallel worktrees and
concurrent embedded-database writers are deferred, including across machines.
Read-only research can be delegated without creating competing implementations.

Before starting a new task, inspect `git status --short --branch`. Preserve any
existing edits and finish or hand off the current branch before switching.
From a clean checkout:

```bash
git fetch origin
git switch main
git merge --ff-only origin/main
git switch --no-track -c feature/descriptive-task origin/main
bd prime
bd ready --json
bd update <issue-id> --claim --json
```

Use `fix/` or `chore/` when more descriptive. Include the Beads ID and OpenSpec
change path in the eventual PR. Never implement directly on `main`. Do not
force-push or rewrite history. GitHub requires PRs, blocks force pushes and
deletion, and applies protection to admins; mandatory approvals are zero for
solo-maintainer use. Required CI checks remain a separate follow-up.

Before handoff, run relevant non-fixing verification, review the full diff and
new files, and update Beads with results and blockers. A local implementation
being complete does not mean its PR has merged. Do not stage, commit, publish,
open a PR, or synchronize Dolt unless the current user request authorizes it.
After an approved PR merges, return to clean `main` and fast-forward before
starting another branch. Beads state is shared across branches, not isolated
by `git switch`.

## Specification and Execution

Use `/opsx-explore` for investigation and `/opsx-propose` for a scoped change.
The core profile also provides `/opsx-apply`, `/opsx-update`, `/opsx-sync`, and
`/opsx-archive`. These are OpenCode commands, not shell executables.
OpenSpec sync reconciles specifications; it is not Beads remote synchronization.

Link Beads issues to `openspec/changes/<change>/` or the relevant spec using
`bd create --spec-id <path>` and describe acceptance and design. Reference the
resulting issue IDs in OpenSpec's implementation checklist. Claims, dependency
edges, blockers, and live progress belong only in Beads. The OpenSpec checklist
is a derived acceptance/implementation record, not a second task tracker.
Reconcile it from verified outcomes before archiving. This narrow checklist
exception overrides the generated Beads ban on Markdown task lists.
After revising requirements, reopen affected Beads work and refresh the derived
checklist before apply checks for completion. After archiving, update affected
Beads spec references to the actual archive or canonical spec paths; the original
change directory no longer exists.

Preserve the managed Beads block in `AGENTS.md` and generated OpenSpec command
and skill bodies. Put overrides outside that block and in `openspec/config.yaml`.
After `bd setup opencode` or `openspec update`, inspect regenerated output for
policy drift, especially commit/push instructions and task ownership.

## Storage and Review Gates

Beads uses embedded Dolt in `.beads/embeddeddolt/`, with issue prefix `etl`.
Track only shareable `.beads` configuration, metadata, ignore rules, and README.
Database files, credentials, interaction traces, optional JSONL exports, and
backups stay local and ignored. A source Git commit does not back up issues.

`dolt.auto-commit` is explicitly `off`. Writes persist in the Dolt working set
but are not ready for publication until explicitly committed. This overrides
the generated integration's statement that every write auto-commits.
Auto-push, automatic backup, and export staging are disabled. Agent handoff
policy is conservative; do not override it with environment settings.
Beads 1.2.2 stores `agent.profile` in its database, not this repository's YAML;
the initial database setting is `conservative`, with the same policy explicitly
enforced by `AGENTS.md` on every clone.

Source Git commits and Dolt database commits are separate approval-controlled
actions. Initializing the database itself creates internal Dolt history.
The bootstrap review checkpoint allows local initialization and task writes,
but not source commits, explicit Dolt commits, or remote publication.

## Cross-Machine Synchronization

The configured Dolt remote is:

```text
origin = git+https://github.com/snarkipus/hurdat2_etl_demo.git
```

Issue history uses `refs/dolt/data`, separate from source branches. GitHub's
`main` protection does not protect this ref. The repository and synchronized
tasks are public; do not put secrets or private operational data in issues.
Use each machine's credential helper/authentication, never credentials in
tracked configuration. Source-remote access alone does not prove Dolt transport
works; verify the complete round trip after approval.

Check local wiring without publication:

```bash
bd where --json
bd dolt status --json
bd dolt remote list --json
bd config get dolt.auto-commit
git ls-remote origin refs/dolt/data
```

Once explicitly authorized, the initial publisher commits reviewed task state
and publishes it:

```bash
bd dolt commit -m "Initialize reviewed Beads task history"
bd dolt push
```

Do not run this publication sequence before the bootstrap diff is approved.
After publication and source configuration availability, on a fresh clone:

```bash
bd bootstrap --dry-run --json
bd bootstrap --yes
bd where --json
bd dolt status --json
bd dolt remote list --json
bd dolt pull
bd list --json
```

Verify embedded mode, the remote URL, and the expected issue IDs and content.
Use the bootstrap commands only on a fresh clone or for deliberate recovery:
the installed version's dry-run can report `has_existing: false` even when an
embedded database exists. Do not treat that field as permission to overwrite
local state; check `bd where` and `bd dolt status` first on an existing checkout.
If bootstrap cannot obtain published history, stop; do not initialize an empty
replacement or force synchronization. These are the same checks to perform in
an isolated second clone before claiming cross-machine sync is operational.

For normal handoff after initial publication, the outgoing machine explicitly
commits approved local task changes, then runs `bd dolt push`. The incoming
machine runs `bd dolt pull` before claiming or editing work. Do not overlap
writers. If the incoming machine has local changes, reconcile them deliberately
before pulling; never discard them or use `--force` to bypass a conflict.

If sync fails, report the exact command and error and preserve the local
database. Do not use `bd init --force`, reinitialize history, import JSONL as a
substitute for pull, or delete local storage. A missing database can be restored
with `bd bootstrap` from verified published history; unpublished working-set
changes are not protected by that remote.

## Validation

```bash
bd setup opencode --check
bd dolt status --json
bd ready --json
openspec list --json
openspec validate --all --strict --no-interactive
git diff --check
```

An empty OpenSpec installation has no specs or changes to validate; that is not
evidence of application correctness. Read `AGENTS.md` for actual Python checks
and their side effects. Do not run fixing hooks merely to validate this tooling
bootstrap. Review new files too: ordinary `git diff` omits untracked files.
