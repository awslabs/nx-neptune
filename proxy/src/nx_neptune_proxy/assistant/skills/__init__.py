# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Neptune capability skill (spec §9.13).

A knowledge layer that lets the assistant reason about *what moving relational
data into a graph unlocks*, not just how to write the load SQL. Two tiers:

1. **Always-on catalog** — distilled string constants compiled into this module
   and injected into agent system prompts every turn:
   - :data:`CAPABILITY_CATALOG` (Query Planner) — graph use-cases + the
     Neptune-Analytics / nx-neptune analytics the planner may propose,
     reconciled against ``neptune-algorithms-cross-reference.md`` so it only
     ever cites procedures the backend actually exposes.
   - :data:`CAPABILITY_HINTS` (Supervisor) — a one-paragraph routing hint.
   - :data:`DATA_MODELING_GUIDANCE` (SQL Mapping) — distilled graph-modeling
     rules (labels, edge direction, supernode avoidance).
2. **On-demand loader** — :func:`load_neptune_skill` returns the full text of
   one vendored reference (``references/{topic}.md``) for deep dives.

The vendored references under ``references/`` are a **snapshot copy** of the AWS
``amazon-neptune`` agent skill (Apache-2.0) — see ``references/PROVENANCE.md``.
This module never imports ``strands`` so the package stays importable without
``strands-agents`` installed.
"""

from functools import lru_cache
from importlib import resources
from typing import Literal, get_args

# --- On-demand reference loader -------------------------------------------

SkillTopic = Literal["querying", "use-cases", "graphrag", "data-modeling"]

_TOPICS: frozenset[str] = frozenset(get_args(SkillTopic))


@lru_cache(maxsize=None)
def _read_reference(topic: str) -> str:
    """Read a vendored reference file (cached; the files are read-only)."""
    return (
        resources.files(__package__)
        .joinpath("references", f"{topic}.md")
        .read_text(encoding="utf-8")
    )


def load_neptune_skill(topic: str) -> str:
    """Return the full text of one vendored Neptune reference for a deep dive.

    Fails soft (spec §9.13 graceful degradation): an unknown topic or a missing
    reference file returns a short "reference unavailable" note rather than
    raising, so a bad snapshot can never break a turn. Reads are cached, so a
    repeated topic within a process costs nothing after the first load.
    """
    if topic not in _TOPICS:
        allowed = ", ".join(sorted(_TOPICS))
        return f"Neptune skill reference '{topic}' unavailable (known topics: {allowed})."
    try:
        return _read_reference(topic)
    except (FileNotFoundError, OSError):
        return f"Neptune skill reference '{topic}' unavailable (file missing)."


# --- Always-on capability catalog (Query Planner) -------------------------

CAPABILITY_CATALOG = """\
# What a Neptune graph unlocks (capability catalog)

Once the relational data is loaded as a graph, propose openCypher that answers
questions relational JOINs handle poorly — multi-hop traversal, path finding,
and whole-graph analytics. Common patterns (from the Neptune use-case skill):

- **Identity resolution / Customer 360** — connect entities that share an
  identifier (email/phone/device) to merge duplicates or resolve identity.
- **Fraud rings** — accounts linked through shared identifiers or transaction
  chains; find connected suspicious clusters.
- **Dependency / impact analysis** — "blast radius": what (recursively) depends
  on a node via `-[:DEPENDS_ON*1..n]->`.
- **Access-control / permission tracing** — inheritance paths through groups
  and roles; "why does this user have this permission?".
- **Recommendations** — collaborative filtering over PURCHASED/RATED/FOLLOWS
  edges ("users who bought X also bought…").
- **Supply-chain traceability** — upstream/downstream provenance and recall
  impact along the chain.
- **Knowledge graph / GraphRAG** — entity-linked chunks for richer retrieval
  (deep dive: `load_neptune_skill("graphrag")`).

## Analytics you may propose (Neptune Analytics via nx-neptune)

Only cite procedures the backend actually exposes. nx-neptune runs these on
Neptune Analytics; each is reached through its NetworkX name, and a mutating
run writes the score onto each node (`write_property`, i.e. the `.mutate`
variant) instead of returning rows:

- **PageRank** — `neptune.algo.pageRank` — influence / importance ranking.
- **Degree centrality** — `neptune.algo.degree` (in / out / total) — connectedness.
- **Closeness centrality** — `neptune.algo.closenessCentrality` — how central a
  node is by shortest-path distance.
- **Louvain** — `neptune.algo.louvain` — community detection.
- **Label propagation** — `neptune.algo.labelPropagation` — fast community
  detection.
- **BFS** — `neptune.algo.bfs` — reachability / layers from source node(s).

NOT available (do not propose as if the backend runs them): single-source
shortest path (bellmanFord / deltaStepping / topksssp), weakly/strongly
connected components (wcc / scc), similarity (jaccard / common-neighbors), and
vector search. If a request needs one, say so plainly rather than inventing a
call.

## Neptune openCypher gotchas (Neptune Analytics is a subset of Neo4j Cypher)

- No `shortestPath()` — use a variable-length path and `min(length(path))` to
  approximate shortest distance.
- Label predicates inside `CASE`/`WHEN` are silently dropped and every branch
  yields null — use `labels(n)[0]` (the first label as a string) instead.
- No `apoc` procedures; `CALL { }` read-only subqueries are supported.
- Always parameterize (`$param`), never string-interpolate user values.
- Add `LIMIT`, and `min(length(path))` / `DISTINCT` on multi-hop matches.
"""

# --- Trimmed routing hints (Supervisor) -----------------------------------

CAPABILITY_HINTS = """\
Moving relational data into a Neptune graph unlocks multi-hop and whole-graph
work that JOINs handle poorly: identity resolution / Customer 360, fraud-ring
and dependency/blast-radius analysis, permission tracing, recommendations,
supply-chain traceability, GraphRAG, and analytics such as PageRank, degree and
closeness centrality, community detection (Louvain / label propagation), and
BFS reachability. Route a request to `generate_import` when the user wants to
build or explore such a graph; treat pure single-table lookups, SQL aggregates,
or full-text search as signs a graph is *not* warranted.
"""

# --- Data-modeling guidance (SQL Mapping) ---------------------------------

DATA_MODELING_GUIDANCE = """\
# Graph-modeling guidance (model for traversal, not normalization)

The node/edge SELECTs you emit define the graph model, so shape it for the
traversals the user will run — do not just mirror the relational tables:

- **Naming:** PascalCase node labels ("~label"), camelCase properties. Keep
  spelling/case consistent — downstream openCypher must match exactly.
- **Shared identifiers become nodes, not properties.** For identity/fraud
  patterns, model email/phone/device as their own nodes that multiple entities
  point at via edges, so traversal can find shared connections.
- **Relationships are edges, never list-valued properties.** A "friends" list
  column cannot be traversed; emit an edge query instead.
- **Put attributes of a relationship on the edge** (e.g. a purchase date/amount
  on a PURCHASED edge) rather than inventing an intermediate node.
- **Avoid supernodes.** Don't turn a low-cardinality column (country, status)
  into a hub node every row links to — keep it a property and filter on it.
- **Skip intermediate nodes** unless the intermediate entity has its own
  properties or relationships you'll query — every hop adds latency.

Deep dive: `load_neptune_skill("data-modeling")`.
"""
