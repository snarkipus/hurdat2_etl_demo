# Agentic Development Workflow

## Scope and Authority

`main` is the protected canonical DuckDB implementation, initially at `9af5d7a`.
Root `AGENTS.md` governs repository behavior; `CLAUDE.md` only imports it.
OpenSpec owns specification and intent. Beads owns task execution state.
Existing plans, reviews, and diagrams in `docs/archive/` are historical reference,
not an active backlog or automatically accepted requirements. Adopt specs
incrementally for reviewed changes instead of inventing a retrospective specification.
See the [documentation map](README.md) for assets, external references, and the
boundary between current behavior and proposed target architecture. A small
documentation-only cleanup can be tracked directly in Beads without inventing
application requirements or an OpenSpec change.

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
The initial bootstrap used the managed `AGENTS.md` integration alone; the
current setup also uses the OpenCode plugin described below. OpenSpec supplies
six commands and six skills under `.opencode/`.
Restart OpenCode after initialization or updates so new guidance is loaded.

Do not repeat `bd init` on another machine to recreate existing issue history.
Use the fresh-clone procedure below instead. Do not reinstall Beads Git hooks:
this repository already has pre-commit hooks, which remain unchanged.

### OpenCode Beads Plugin

The project config at [`.opencode/opencode.json`](../.opencode/opencode.json)
pins `@snarkipus/opencode-beads@0.10.0`. OpenCode loads this package as a plugin;
it is not a Python dependency or a Beads MCP server. Keep the version explicit
and review upgrades rather than silently adopting a floating version. See the
[versioned plugin documentation](https://github.com/snarkipus/opencode-beads/blob/v0.10.0/README.md).

The plugin and `bd setup opencode` have distinct roles:

- `bd setup opencode` maintains the generated Beads section in `AGENTS.md`.
- The plugin injects full `bd prime` context on the first eligible session
  message as system context, and after compaction as a synthetic `noReply`
  prompt. Primary sessions and both bounded Beads task agents are eligible.
  Recognized ordinary subagents such as `explore` and `general` are excluded;
  missing or failed agent lookup falls back to allowing injection.
- `/beads:ready`, `/beads:create`, and `/beads:show` are OpenCode workflows,
  not shell executables. They do not cover every CLI subcommand; use `bd --help`
  and the installed CLI for the complete command surface.
- `beads-task-agent` supports explicitly read-only status/graph analysis or
  completion of exactly one selected/ready Bead per invocation. Newly discovered
  work is recorded and returned to the primary agent, not immediately executed.

The primary thread retains the proposal, decisions, and orchestration context.
After proposal review, it maps the checklist to Beads, selects an existing issue,
and delegates that single issue with its scope and acceptance criteria. It
verifies the returned result and reconciles `tasks.md` before selecting another
issue. Do not run parallel implementation workers against the embedded database
or ask a worker to autonomously drain ready work. Read-only graph requests must
explicitly prohibit Beads mutations. The default worker inherits its caller's
model. This project also opts into `beads-task-agent-luna`, which uses the
plugin's `openai/gpt-5.6-luna` model and `max` variant defaults with the same
bounded one-Bead workflow. Select that agent explicitly when delegating;
enabling it does not reroute default delegation or change the primary model.
The provider must expose that model/variant; the plugin does not configure
authentication or silently fall back to another model.

The installed `bd` CLI still owns initialization, issue state, migrations,
backups, and Dolt synchronization. The plugin does not install `bd`, initialize
the database, or install/repair Beads skills. Bootstrap's `--skip-hooks` skips
Git hooks; `--skip-agents` skips generated agent instructions and automatic
Claude/Codex integration, including the Codex skill installation path. Neither
disables OpenCode plugin runtime hooks. The later `bd setup opencode` maintains
the managed block, not those skipped integrations. Do not rerun initialization
to imitate another installation's skill layout. Plugin workflows and injected
defaults do not grant permission to commit, synchronize, push, or override
repository policy.

Restart OpenCode after installing or changing the plugin configuration. Verify
the `/beads:*` commands and `beads-task-agent` are available in the new session.
`bd setup opencode --check` checks the managed block, not plugin loading. If
injection is missing, run `bd prime` and check CLI availability/workspace
resolution. Failed or empty initial prime output can be retried on later
messages; failed post-compaction injection does not necessarily retry on the
next message, so use the manual fallback. Do not add a duplicate injection hook
or reinitialize an existing database as a fix.
If a command or worker definition is unexpected, inspect effective OpenCode
configuration: explicit command definitions override plugin commands, and
explicit agent fields override the corresponding plugin defaults. Agent merges
are shallow: nested `permission`/`tools` values are replaced, not deep-merged,
and overriding `prompt` can replace the bounded workflow. Default-worker
overrides do not propagate to Luna. The one-Bead and read-only boundaries are
prompt guidance, not a plugin-enforced mutation sandbox; repository policy
and primary-thread verification remain necessary.

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

Use `fix/`, `chore/`, or `docs/` when more descriptive. Include the Beads ID and,
when applicable, the OpenSpec change path in the eventual PR. Never implement
directly on `main`. Do not force-push or rewrite history. GitHub requires PRs, blocks force pushes and
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

Implementation is driven by Beads, not `/opsx-apply`. OpenSpec commands are
invoked in OpenCode, not the shell. The normal flow is:

1. Investigate with `/opsx-explore`, then use `/opsx-propose` to produce the
   proposal, design, delta specs, and `tasks.md` for a scoped change.
2. Review those artifacts before building a Beads dependency graph from the
   approved implementation checklist. Do not infer migration approval from a
   diagram or a generated task.
3. Execute through Beads: inspect ready work, claim an issue, implement its
   scope, verify acceptance, and record its outcome. Beads owns assignments,
   dependency edges, blockers, and live execution status.
4. Reconcile `tasks.md` in the same session as each verified Beads outcome.
   This is continuous agent-maintained reconciliation, not background automation.
5. Perform a final coverage and consistency check, then complete the remaining
   OpenSpec flow with `/opsx-sync` and `/opsx-archive`. OpenSpec sync reconciles
   specifications; it is not Beads remote synchronization.

The mapping is not necessarily one-to-one. One checklist item can require
several Beads issues, and one issue can satisfy several checklist items. Record
the associated issue IDs alongside each mapped item. Link issues back with
`bd create --spec-id <path>` and identify the applicable item numbers or scope
in the issue description, together with acceptance criteria and design.
Reuse existing issues when appropriate; do not fabricate IDs during proposal
generation or mechanically create duplicate tasks.

Every implementation item must have traceable Beads coverage before execution.
Check it off only when its entire scope has verified coverage, including all
required supporting issues. A closed issue alone is not proof of full coverage;
cancelled or superseded work requires a verified replacement or explicit scope
revision. The OpenSpec checklist is a derived completion record, not a second
execution tracker. This narrow exception overrides the generated Beads ban on
Markdown task lists.

Use `/opsx-update` when requirements or design change. Revise the affected Beads
graph and reopen affected checklist items as necessary before continuing work.
Before sync/archive, confirm that the checklist, verified issue outcomes, and
approved scope agree. After archiving, update affected Beads spec references
to the actual archive or canonical spec paths; the original change directory
no longer exists.

The core profile still installs `/opsx-apply` and the `openspec-apply-change`
skill. Keep them intact for tool-update compatibility, but do not use them as
the normal implementation entry point. Their generated handoffs do not override
this repository's Beads-led workflow.

Preserve the managed Beads block in `AGENTS.md` and generated OpenSpec command
and skill bodies. Put overrides outside that block and in `openspec/config.yaml`.
After `bd setup opencode` or `openspec update`, inspect regenerated output for
policy drift, especially commit/push instructions and task ownership.

## Storage and Review Gates

Beads uses embedded Dolt in `.beads/embeddeddolt/`, with issue prefix `etl`.
Track only shareable `.beads` configuration, metadata, ignore rules, and README.
Database files, credentials, interaction traces, optional JSONL exports, and
backups stay local and ignored. A source Git commit does not back up issues.

### Beads Access Boundary

Use `bd` as the operational interface to Beads. This includes routine status,
history/diff inspection, issue and dependency changes, and authorized commits
and synchronization. Discover supported operations with `bd --help` and
`bd <command> --help`; do not substitute raw database commands when a familiar
command or flag is unavailable. In this guide, "Dolt commit/sync" means the
Beads-managed `bd dolt ...` commands, not invoking `dolt` directly.

Do not inspect or manipulate the embedded database through raw `dolt` commands
(including `status`, `diff`, or `sql`), direct SQL through any interface, another
database client, or manual database-file operations. Read-only intent does not
waive this boundary. Routine review must stay within Beads rather than bypassing
its lifecycle, locking, or schema assumptions.

If a necessary diagnostic or recovery step is unavailable through supported
Beads operations, stop and explain the limitation. Propose the exact lower-level
operation, whether it reads or writes, and its risks; obtain explicit approval
before running it. For approved writes, agree on backup/recovery precautions
and ensure other writers are stopped. Approval covers only that operation, not
an ongoing exception. General authorization to implement, commit, push, or sync
does not grant direct database access. Apply the same restrictions to subagents;
do not delegate a bypass or turn a failed Beads command into automatic raw-Dolt
troubleshooting.

`dolt.auto-commit` is explicitly `on`. Ordinary `bd` writes create local Dolt
history transparently; no separate approval or routine `bd dolt commit` step
is required. This matches the generated integration's per-write commit guidance.
Auto-push, automatic backup, and export staging are disabled. Agent handoff
policy is conservative; do not override it with environment settings.
Beads 1.2.2 stores `agent.profile` in its database, not this repository's YAML;
the initial database setting is `conservative`, with the same policy explicitly
enforced by `AGENTS.md` on every clone.

Local Beads history is bookkeeping, not remote publication. Source Git commits
and remote synchronization still require explicit authorization. The original
bootstrap disabled local auto-commit; that additional manual step is no longer
repository policy. If writes unexpectedly remain uncommitted, inspect the
effective setting and CLI diagnostics rather than routinely forcing a commit
or accessing the database directly.

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

Once explicitly authorized, publish the local history already managed by Beads:

```bash
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

For authorized handoff, the outgoing machine runs `bd dolt push`; no separate
local commit step is needed. The incoming machine runs an authorized
`bd dolt pull` before claiming or editing work. Do not overlap
writers. If the incoming machine has local changes, reconcile them deliberately
before pulling; never discard them or use `--force` to bypass a conflict.

If sync fails, report the exact command and error and preserve the local
database. Do not use `bd init --force`, reinitialize history, import JSONL as a
substitute for pull, or delete local storage. A missing database can be restored
with `bd bootstrap` from verified published history; local history that has
not been pushed is not protected by that remote.

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
