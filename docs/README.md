# Documentation Map

Root [AGENTS.md](../AGENTS.md) is the canonical repository instruction entry
point. Documentation location and status distinguish historical evidence,
current behavior, and proposed direction; adding a file does not approve it.

| Location | Purpose and authority |
| --- | --- |
| [Project README](../README.md) | Current setup, CLI usage, output acceptance/recovery, and development checks; executable code/configuration determines behavior. |
| [Agentic workflow](agentic-workflow.md) | Current development, verification, and cross-machine handoff conventions. |
| [Releases](releases/README.md) | Maintainer release procedure and versioned release notes; publication evidence is recorded in Beads. |
| [OpenSpec specs](../openspec/specs/) | Accepted requirements captured through OpenSpec; not a claim of complete retrospective coverage. |
| [OpenSpec changes](../openspec/changes/) | Proposed requirements, designs, and derived implementation checklists. Beads owns execution state. |
| [README assets](assets/readme/) | Presentation images for the root GitHub README, not specifications. |
| [Historical archive](archive/README.md) | Original plans, reviews, and diagrams; non-authoritative evidence, not a live backlog. |
| [Reference material](../ref/) | External HURDAT2 format documentation and versioned source data, not retired project plans. |

## Reference Material

- [HURDAT2 format PDF](../ref/hurdat2-format.pdf): external format reference.
- [Format transcription](../ref/schema_def.md): Markdown convenience copy of
  HURDAT2 format documentation, not the application's database schema.
- [Atlantic input snapshot](../ref/hurdat2-1851-2023-051124.txt): versioned data;
  historical observations are not obsolete documentation.

## Target Architecture

The [modernization diagram](../openspec/changes/archive/2026-09-08-modernize-reference-etl/assets/ETL_diagram_v2.png)
is a proposal reference, not a current-state authority. Its reviewed meaning and
hash remain in the change's [design](../openspec/changes/archive/2026-09-08-modernize-reference-etl/design.md).
For future scoped changes, place their diagrams and any editable source
under `openspec/changes/<change>/assets/` and reference them from `design.md`.
Label retained components, proposed changes, and unresolved decisions explicitly.
Do not create a placeholder change or assume its requirements are approved just
to store an image. Current operating guidance lives in the project README and
`AGENTS.md`; this reconciliation adds no architecture documentation or diagram.

The diagram supports explicit requirements; it does not authorize toolchain or
runtime migrations on its own. Review the scope, map the resulting `tasks.md` to
a Beads dependency graph, and implement through Beads. Keep the checklist in
step with verified outcomes throughout execution, then finish sync/archive.

Add maintained current-state architecture documentation under `docs/architecture/`
when implemented behavior has been verified. Keep proposal artifacts with their
OpenSpec history. `docs/archive/` is separate from `openspec/changes/archive/`,
which preserves completed OpenSpec changes.
