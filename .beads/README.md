# Beads Workspace

This repository uses embedded Beads with issue prefix `etl`. One writer works
sequentially in one checkout; switching Git branches does not isolate issue state.

Read [AGENTS.md](../AGENTS.md) for authority and
[the workflow guide](../docs/agentic-workflow.md) for setup, synchronization,
review gates, and recovery. Use `bd prime` for installed CLI guidance.

Use `bd` for routine inspection and changes, including authorized `bd dolt ...`
commits/synchronization. Do not access the embedded database with raw `dolt`,
direct SQL, other database clients, or manual database-file operations, even
read-only. A diagnostic or recovery gap requires explicit approval for the
specific lower-level operation, not merely general commit/sync permission.
See the [access boundary](../docs/agentic-workflow.md#beads-access-boundary).

OpenCode's project configuration pins `@snarkipus/opencode-beads@0.10.0` for
prime-context injection, `/beads:*` workflows, and one-Bead task-agent delegation.
It complements the managed `AGENTS.md` integration; it does not replace the
installed `bd` CLI, initialize the workspace, or authorize automatic publication.
See the [plugin usage guide](../docs/agentic-workflow.md#opencode-beads-plugin).

Track `.gitignore`, `config.yaml`, `metadata.json`, and this README in source Git.
Do not track the embedded database, interaction traces, exports, or credentials.
Issue history is synchronized separately through the Dolt remote, not by a
source-code commit. Local auto-commit is on: normal `bd` writes manage history
without a routine manual commit step. Auto-push remains off; remote pull/push
and source Git commits still require explicit authorization.
