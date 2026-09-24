# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Neptune capability skill (spec §9.13).

A knowledge layer that lets the assistant reason about *what moving relational
data into a graph unlocks*, not just how to write the load SQL. Two tiers:

1. **Always-on catalog** — distilled string constants compiled into this module
   and injected into agent system prompts every turn:
   - :data:`CAPABILITY_CATALOG` (Query Planner) — graph use-cases + a full
     reference for every Neptune Analytics procedure the planner may propose
     (call form, config-map params, and YIELD output fields for each variant).
     Proposed openCypher runs directly against Neptune Analytics, so every
     listed procedure is runnable — not only the ones nx-neptune wraps as
     Python functions.
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

## Analytics catalog (Neptune Analytics procedures)

Proposed openCypher runs **directly against Neptune Analytics**, so every
procedure below is runnable — not just the ones nx-neptune wraps as Python
functions. Match the call form and config-map key names exactly.

Conventions:
- Config keys are **camelCase**; pass them in the `{ }` map, e.g.
  `CALL neptune.algo.pageRank(n, {dampingFactor: 0.85, maxIterations: 20})`.
- Enum values are lowercase strings: `traversalDirection` ∈
  `"outbound"` (default) | `"inbound"` | `"both"` (BFS/centrality/community
  only — SSSP does **not** allow `"both"`). `edgeWeightType` /
  `vertexWeightType` ∈ `"int"` | `"long"` | `"float"` | `"double"`.
- A **`.mutate`** variant writes the result onto each node via
  `writeProperty: "<name>"` and yields only `success` (boolean) instead of
  rows: `CALL neptune.algo.pageRank.mutate({writeProperty: "rank"}) YIELD success RETURN success`.
- Shared optional config accepted by most procedures: `vertexLabel: string`,
  `edgeLabels: string[]`, `concurrency: 0|1` (`0` = all threads).
- Always add a `LIMIT` on read queries that can return the whole graph.

### Path-finding / traversal
`sourceNode` is bound by a preceding `MATCH` and passed positionally. SSSP
requires **positive** `edgeWeightProperty` + `edgeWeightType`, does not support
`traversalDirection: "both"`, and has no `.mutate` variant.

| Procedure | Call form | Params | YIELD (type) |
|---|---|---|---|
| `neptune.algo.bfs.parents` | `MATCH (n) WHERE n.id=$0 CALL neptune.algo.bfs.parents(n, {maxDepth:3}) YIELD parent, node RETURN parent, node` | `maxDepth: int`, `traversalDirection`, `edgeLabels`, `vertexLabel`, `concurrency` | `node` (node), `parent` (node) |
| `neptune.algo.bfs.levels` | `MATCH (n) WHERE n.id=$0 CALL neptune.algo.bfs.levels(n, {maxDepth:2}) YIELD node, level RETURN node, level` | same as `bfs.parents` | `node` (node), `level` (long) |
| `neptune.algo.sssp.bellmanFord` | `MATCH (n) WHERE n.id=$0 CALL neptune.algo.sssp.bellmanFord(n, {edgeWeightProperty:"cost", edgeWeightType:"double"}) YIELD source, node, distance RETURN node, distance` | `edgeWeightProperty: string` (req), `edgeWeightType` (req), `traversalDirection` (not `both`), `edgeLabels`, `vertexLabel`, `concurrency` | `source` (node), `node` (node), `distance` (double) |
| `neptune.algo.sssp.bellmanFord.parents` | `…CALL neptune.algo.sssp.bellmanFord.parents(n, {edgeWeightProperty:"cost", edgeWeightType:"double"}) YIELD source, node, distance, parent RETURN node, parent, distance` | same as `sssp.bellmanFord` | `source` (node), `node` (node), `distance` (double), `parent` (node) |
| `neptune.algo.sssp.bellmanFord.path` | `MATCH (s), (t) WHERE … CALL neptune.algo.sssp.bellmanFord.path(s, t, {edgeWeightProperty:"cost", edgeWeightType:"double"}) YIELD source, target, distance, path RETURN path, distance` | source **and** target node(s) + same config | `source` (node), `target` (node), `distance` (double), `vertexPath` (list), `allDistances` (list), `path` (path) |
| `neptune.algo.sssp.deltaStepping` | as `sssp.bellmanFord` (parallel; same result) | same as `sssp.bellmanFord` | `source` (node), `node` (node), `distance` (double) |
| `neptune.algo.sssp.deltaStepping.parents` | as `sssp.bellmanFord.parents` | same | `source`, `node`, `distance` (double), `parent` (node) |
| `neptune.algo.sssp.deltaStepping.path` | as `sssp.bellmanFord.path` | same (source + target) | `source`, `target`, `distance`, `vertexPath`, `allDistances`, `path` |
| `neptune.algo.topksssp` | `MATCH (n) WHERE n.id=$0 CALL neptune.algo.topksssp(n, {maxDepth:4}) YIELD … RETURN …` | `maxDepth: int`, optional `edgeWeightProperty`/`edgeWeightType`, `edgeLabels`, `concurrency` | top-K hop-limited paths sorted by cost (see Neptune docs) |

### Centrality
| Procedure | Call form | Params | YIELD (type) |
|---|---|---|---|
| `neptune.algo.pageRank` | `MATCH (n) CALL neptune.algo.pageRank(n, {dampingFactor:0.85, maxIterations:20}) YIELD rank RETURN n, rank` | `dampingFactor: float` (0.85), `maxIterations: int` (20), `tolerance: float`, `edgeWeightProperty`/`edgeWeightType`, `sourceNodes: node[]`, `sourceWeights: number[]`, `traversalDirection`, `edgeLabels`, `vertexLabel`, `concurrency` | `rank` (double) |
| `neptune.algo.pageRank.mutate` | `CALL neptune.algo.pageRank.mutate({writeProperty:"rank", dampingFactor:0.85}) YIELD success RETURN success` | `writeProperty: string` (req) + all pageRank params | `success` (bool) |
| `neptune.algo.degree` | `MATCH (n) CALL neptune.algo.degree(n) YIELD degree RETURN n.id, degree` | `traversalDirection` (`outbound`=out-degree, `inbound`=in-degree, `both`=total), `edgeLabels`, `vertexLabel`, `concurrency` | `degree` (long) |
| `neptune.algo.degree.mutate` | `CALL neptune.algo.degree.mutate({writeProperty:"degree"}) YIELD success RETURN success` | `writeProperty` (req) + degree params | `success` (bool) |
| `neptune.algo.closenessCentrality` | `CALL neptune.algo.closenessCentrality(n, {numSources:1024}) YIELD node, score RETURN id(node), score` | `numSources: int` (exact if omitted), `traversalDirection`, `edgeLabels`, `vertexLabel`, `concurrency` | `node` (node), `score` (double) |
| `neptune.algo.closenessCentrality.mutate` | `CALL neptune.algo.closenessCentrality.mutate({writeProperty:"closeness"}) YIELD success RETURN success` | `writeProperty` (req) + closeness params | `success` (bool) |

### Community detection
| Procedure | Call form | Params | YIELD (type) |
|---|---|---|---|
| `neptune.algo.louvain` | `MATCH (n) CALL neptune.algo.louvain(n, {iterationTolerance:1e-07}) YIELD node, community RETURN id(node), community` | `maxLevels: int`, `maxIterations: int`, `levelTolerance: float`, `iterationTolerance: float`, `edgeWeightProperty`/`edgeWeightType`, `edgeLabels`, `concurrency` | `node` (node), `community` (long) |
| `neptune.algo.louvain.mutate` | `CALL neptune.algo.louvain.mutate({writeProperty:"community"}) YIELD success RETURN success` | `writeProperty` (req) + louvain params | `success` (bool) |
| `neptune.algo.labelPropagation` | `MATCH (n) CALL neptune.algo.labelPropagation(n, {maxIterations:10}) YIELD node, community RETURN id(node), community` | `maxIterations: int`, `vertexWeightProperty`/`vertexWeightType`, `edgeWeightProperty`/`edgeWeightType`, `traversalDirection`, `edgeLabels`, `vertexLabel`, `concurrency` | `node` (node), `community` (long) |
| `neptune.algo.labelPropagation.mutate` | `CALL neptune.algo.labelPropagation.mutate({writeProperty:"community"}) YIELD success RETURN success` | `writeProperty` (req) + labelPropagation params | `success` (bool) |
| `neptune.algo.wcc` | `MATCH (n) CALL neptune.algo.wcc(n, {edgeLabels:["route"]}) YIELD node, component RETURN id(node), component` | `edgeLabels`, `vertexLabel`, `concurrency` | `node` (node), `component` (long) |
| `neptune.algo.wcc.mutate` | `CALL neptune.algo.wcc.mutate({writeProperty:"wccId"}) YIELD success RETURN success` | `writeProperty` (req) + wcc params | `success` (bool) |
| `neptune.algo.scc` | `MATCH (n) CALL neptune.algo.scc(n) YIELD node, component RETURN id(node), component` | `edgeLabels`, `vertexLabel`, `concurrency` | `node` (node), `component` (long) |
| `neptune.algo.scc.mutate` | `CALL neptune.algo.scc.mutate({writeProperty:"sccId"}) YIELD success RETURN success` | `writeProperty` (req) + scc params | `success` (bool) |

### Similarity (pairwise; bind two nodes with `MATCH`)
| Procedure | Call form | Params | YIELD (type) |
|---|---|---|---|
| `neptune.algo.jaccardSimilarity` | `MATCH (a),(b) WHERE a.id=$0 AND b.id=$1 CALL neptune.algo.jaccardSimilarity(a, b) YIELD score RETURN score` | two nodes; optional `edgeLabels`, `vertexLabel`, `concurrency` | `score` (double, 0–1) |
| `neptune.algo.overlapSimilarity` | as jaccard, `…CALL neptune.algo.overlapSimilarity(a, b) YIELD score RETURN score` | same | `score` (double) |
| `neptune.algo.neighbors.common` | `MATCH (a),(b) WHERE … CALL neptune.algo.neighbors.common(a, b) YIELD node RETURN node` | two nodes; optional `edgeLabels`, `concurrency` | `node` (node) — shared neighbors |
| `neptune.algo.neighbors.total` | `…CALL neptune.algo.neighbors.total(a, b) YIELD count RETURN count` | same | `count` (long) — size of combined neighborhood |

### Vector similarity search (require a vector index on the graph)
Only propose these if the graph was created with vector embeddings; otherwise
say a vector index is needed. Prefer the non-deprecated `.byNode` / `.byEmbedding`
forms: `vectors.distance.byNode`, `vectors.distance.byEmbedding`,
`vectors.topK.byNode`, `vectors.topK.byEmbedding`, `vectors.get`,
`vectors.upsert`, `vectors.remove`. (Deprecated aliases: `vectors.distance`,
`vectors.distanceByEmbedding`, `vectors.topKByEmbedding`, `vectors.topKByNode`.)
`topK.*` yields `(node, score)`; `distance.*` yields a `distance`; `get` yields
the stored `embedding`.

### Miscellaneous graph procedures
| Procedure | Purpose | YIELD |
|---|---|---|
| `neptune.graph.pg_schema` | Property-graph schema (labels, edge types, property keys) | schema document |
| `neptune.graph.pg_info` | Graph metadata / statistics | info document |
| `neptune.algo.degreeDistribution` | Degree histogram across the graph | degree → count distribution |

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
