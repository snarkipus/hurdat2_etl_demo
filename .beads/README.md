# Beads Workspace

This repository uses embedded Beads with issue prefix `etl`. One writer works
sequentially in one checkout; switching Git branches does not isolate issue state.

Read [AGENTS.md](../AGENTS.md) for authority and
[the workflow guide](../docs/agentic-workflow.md) for setup, synchronization,
review gates, and recovery. Use `bd prime` for installed CLI guidance.

Track `.gitignore`, `config.yaml`, `metadata.json`, and this README in source Git.
Do not track the embedded database, interaction traces, exports, or credentials.
Issue history is synchronized separately through the Dolt remote, not by a
source-code commit. Local writes persist, but auto-commit and auto-push are off.
