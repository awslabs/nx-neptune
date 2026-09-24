# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Neptune Analytics graph-inspection helpers for the assistant (spec §9).

Reads the *live* schema of an imported graph so the Query Planner can ground on
the real graph model (the labels, edge types, and properties that actually
loaded) instead of the predicted node/edge SQL. Read-only, under the scoped
agent role via ``agent_neptune_client()`` — no credential parameters, matching
the Athena tool layer (§9.11). Kept free of any Strands import.
"""

from typing import Optional

from botocore.exceptions import ClientError

from nx_neptune_proxy.assistant.agent_aws import agent_neptune_client
from nx_neptune_proxy.assistant.schemas import GraphSchema
from nx_neptune_proxy.utils.sanitize import sanitize_error_message


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


def validate_opencypher(graph_id: str, cypher: str) -> tuple[bool, Optional[str]]:
    """Validate one openCypher query against a live graph via ``EXPLAIN``.

    ``EXPLAIN`` runs on the real engine but is read-only and cheap: it plans the
    query without executing it, catching syntax errors, unknown procedures, and
    bad algorithm parameters authoritatively. Returns ``(valid, error)`` — a
    ``ClientError`` from the engine is the "invalid" signal (its message,
    sanitized), and any success means the query planned cleanly.

    Requires a live graph: the Neptune Analytics ``ExecuteQuery`` API always
    targets a ``graphIdentifier``, so there is no graph-less validation path.
    Read-only, under the scoped agent role via :func:`agent_neptune_client`.
    """
    stmt = cypher.strip()
    client = agent_neptune_client()
    try:
        # Neptune Analytics requests an EXPLAIN via the explainMode parameter —
        # NOT by prefixing the literal "EXPLAIN" keyword into queryString (that
        # gets parsed as query text and fails at column 1 with "Invalid input
        # 'E'"). STATIC plans the query WITHOUT executing it (DETAILS would run
        # it), so validation stays read-only and cheap while still catching
        # syntax/procedure/param errors authoritatively.
        client.execute_query(
            graphIdentifier=graph_id,
            queryString=stmt,
            language="OPEN_CYPHER",
            parameters={},
            explainMode="STATIC",
        )
        return True, None
    except ClientError as e:
        return False, sanitize_error_message(str(e))
