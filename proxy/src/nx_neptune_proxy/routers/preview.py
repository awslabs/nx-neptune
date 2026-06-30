# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Preview endpoints for Graph Explorer integration.

Serves preview graph data in the format expected by GE's preview Explorer.
"""

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api/v0/preview/{session_id}", tags=["preview"])

# Dummy data — replace with actual projection preview data
_DUMMY_VERTICES = [
    {"id": "v1", "type": "Person", "types": ["Person"], "attributes": {"name": "Alice", "age": 30}},
    {"id": "v2", "type": "Person", "types": ["Person"], "attributes": {"name": "Bob", "age": 25}},
    {"id": "v3", "type": "Company", "types": ["Company"], "attributes": {"name": "Acme", "industry": "Tech"}},
]

_DUMMY_EDGES = [
    {"id": "e1", "type": "KNOWS", "sourceId": "v1", "targetId": "v2", "attributes": {"since": 2020}},
    {"id": "e2", "type": "WORKS_AT", "sourceId": "v1", "targetId": "v3", "attributes": {"role": "Engineer"}},
    {"id": "e3", "type": "WORKS_AT", "sourceId": "v2", "targetId": "v3", "attributes": {"role": "Manager"}},
]


@router.get("/schema")
def schema(session_id: str):
    """Return schema inferred from preview data."""
    vertex_types: dict[str, dict] = {}
    for v in _DUMMY_VERTICES:
        vtype = v["type"]
        if vtype not in vertex_types:
            vertex_types[vtype] = {
                "type": vtype,
                "attributes": [{"name": k, "dataType": "String"} for k in v["attributes"]],
                "total": 0,
            }
        vertex_types[vtype]["total"] += 1

    edge_types: dict[str, dict] = {}
    for e in _DUMMY_EDGES:
        etype = e["type"]
        if etype not in edge_types:
            edge_types[etype] = {
                "type": etype,
                "attributes": [{"name": k, "dataType": "String"} for k in e["attributes"]],
                "total": 0,
            }
        edge_types[etype]["total"] += 1

    return {
        "totalVertices": len(_DUMMY_VERTICES),
        "vertices": list(vertex_types.values()),
        "totalEdges": len(_DUMMY_EDGES),
        "edges": list(edge_types.values()),
    }


@router.get("/search")
def search(
    session_id: str,
    term: str = "",
    vertexTypes: str = Query(default=""),
    limit: int = 10,
    offset: int = 0,
):
    """Keyword search over preview vertices."""
    type_filter = set(vertexTypes.split(",")) if vertexTypes else None
    results = []
    for v in _DUMMY_VERTICES:
        if type_filter and v["type"] not in type_filter:
            continue
        if term and not any(term.lower() in str(val).lower() for val in v["attributes"].values()):
            continue
        results.append(v)
    return {"vertices": results[offset : offset + limit]}


@router.get("/neighbors")
def neighbors(
    session_id: str,
    vertexId: str = Query(...),
    limit: int = 10,
    filterByVertexTypes: str = Query(default=""),
):
    """Get neighbors of a vertex."""
    type_filter = set(filterByVertexTypes.split(",")) if filterByVertexTypes else None
    vertex_map = {v["id"]: v for v in _DUMMY_VERTICES}
    result_vertices = []
    result_edges = []

    for e in _DUMMY_EDGES:
        neighbor_id = None
        if e["sourceId"] == vertexId:
            neighbor_id = e["targetId"]
        elif e["targetId"] == vertexId:
            neighbor_id = e["sourceId"]
        if neighbor_id is None:
            continue
        neighbor = vertex_map.get(neighbor_id)
        if not neighbor:
            continue
        if type_filter and neighbor["type"] not in type_filter:
            continue
        result_vertices.append(neighbor)
        result_edges.append(e)
        if len(result_vertices) >= limit:
            break

    return {"vertices": result_vertices, "edges": result_edges}


@router.get("/neighbor-counts")
def neighbor_counts(session_id: str, vertexIds: str = Query(...)):
    """Get neighbor counts for vertices."""
    ids = vertexIds.split(",")
    counts = []
    for vid in ids:
        type_counts: dict[str, int] = {}
        vertex_map = {v["id"]: v for v in _DUMMY_VERTICES}
        total = 0
        for e in _DUMMY_EDGES:
            neighbor_id = None
            if e["sourceId"] == vid:
                neighbor_id = e["targetId"]
            elif e["targetId"] == vid:
                neighbor_id = e["sourceId"]
            if neighbor_id is None:
                continue
            neighbor = vertex_map.get(neighbor_id)
            if not neighbor:
                continue
            total += 1
            type_counts[neighbor["type"]] = type_counts.get(neighbor["type"], 0) + 1
        counts.append({"vertexId": vid, "totalCount": total, "counts": type_counts})
    return {"counts": counts}
