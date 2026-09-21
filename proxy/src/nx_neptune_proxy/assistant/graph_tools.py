# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Neptune Analytics graph-inspection helpers for the assistant (spec §9).

Reads the *live* schema of an imported graph so the Query Planner can ground on
the real graph model (the labels, edge types, and properties that actually
loaded) instead of the predicted node/edge SQL. Read-only, under the scoped
agent role via ``agent_neptune_client()`` — no credential parameters, matching
the Athena tool layer (§9.11). Kept free of any Strands import.
"""

from nx_neptune_proxy.assistant.agent_aws import agent_neptune_client
from nx_neptune_proxy.assistant.schemas import GraphSchema


def _distinct_properties(structures: list, key: str) -> list[str]:
    """Union the property names across a summary's node/edge structures.

    ``get_graph_summary`` groups nodes/edges into *structures* (one per distinct
    shape); each carries its own property list. We flatten to a single distinct,
    order-preserving vocabulary — enough for the planner to reference exact
    property names without over-modeling per-structure detail."""
    seen: dict[str, None] = {}
    for struct in structures or []:
        for prop in struct.get(key, []) or []:
            # A property entry may be a bare name or a {"name": ...} dict.
            name = prop.get("name") if isinstance(prop, dict) else prop
            if name and name not in seen:
                seen[name] = None
    return list(seen)


def fetch_graph_schema(graph_id: str) -> GraphSchema:
    """Return the live schema of an imported Neptune Analytics graph.

    Calls ``get_graph_summary(mode="DETAILED")`` and parses it into a
    :class:`GraphSchema`: the node/edge labels plus the distinct property
    vocabulary across structures. Parsing is defensive — missing keys collapse
    to empty lists — so a sparse or evolving summary shape never raises."""
    client = agent_neptune_client()
    resp = client.get_graph_summary(graphIdentifier=graph_id, mode="DETAILED")
    summary = resp.get("graphSummary", {}) or {}
    return GraphSchema(
        node_labels=summary.get("nodeLabels", []) or [],
        edge_labels=summary.get("edgeLabels", []) or [],
        node_properties=_distinct_properties(
            summary.get("nodeStructures", []), "nodeProperties"
        ),
        edge_properties=_distinct_properties(
            summary.get("edgeStructures", []), "edgeProperties"
        ),
    )
