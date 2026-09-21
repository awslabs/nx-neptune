# Vendored Neptune skill references — provenance

The Markdown files in this directory are a **snapshot copy** of selected
reference files from the AWS `amazon-neptune` agent skill. They are vendored
(not fetched at runtime, not a git submodule) so they ship inside the package
wheel and load via `importlib.resources`.

- **Upstream repo:** https://github.com/aws/agent-toolkit-for-aws
- **Upstream path:** `skills/specialized-skills/database-skills/amazon-neptune/references/`
- **Pinned commit:** `764fd353c7f221f5e59712ae9c0d40f7786135b0`
- **Snapshot taken:** 2026-09-21
- **License:** Apache-2.0 (see the repository `LICENSE`; attribution recorded in `NOTICE`)

## Files copied

| File | Purpose here |
|---|---|
| `querying.md` | openCypher syntax + Neptune-subset gotchas (Query Planner deep dive) |
| `use-cases.md` | Concrete graph use-case patterns (capability catalog source) |
| `graphrag.md` | GraphRAG pipeline (capability catalog source) |
| `data-modeling.md` | Graph modeling rules (SQL Mapping guidance source) |

The remaining upstream references (`connectivity`, `migration`, `security`,
`decision-guide`, `analytics-vs-database`, `action-safety`, etc.) are
intentionally not copied — our path is fixed to Athena → Neptune Analytics and
the agents are suggest-only.

## Refreshing this snapshot

This is a manual copy. To update, re-copy the files above from the upstream path
at a newer commit and update the **Pinned commit** / **Snapshot taken** fields.
Automating this refresh is a future enhancement (see the spec §7).
