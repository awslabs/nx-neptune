# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Session manager fixtures — read ops, S3 export/import, snapshots, lifecycle.

Env vars:
  NETWORKX_GRAPH_ID              - required for all tests
  NETWORKX_S3_EXPORT_BUCKET_PATH - required for S3 and import tests
"""

import logging
import os

import boto3
import networkx as nx
import pytest

from nx_neptune import NeptuneGraph, NETWORKX_GRAPH_ID, SessionManager, CleanupTask, Node, Edge
from nx_neptune.clients.iam_client import IamClient

logging.basicConfig(level=logging.INFO)
logging.getLogger("nx_neptune").setLevel(logging.INFO)

S3_EXPORT_BUCKET = os.environ.get("NETWORKX_S3_EXPORT_BUCKET_PATH")


@pytest.fixture(scope="module")
def session_manager():
    """SessionManager instance for read-only operations."""
    return SessionManager()


@pytest.fixture(scope="module")
def neptune_graph():
    """NeptuneGraph instance with test data for export."""
    g = nx.Graph()
    na_graph = NeptuneGraph.from_config(graph=g)
    na_graph.clear_graph()
    yield na_graph
    na_graph.clear_graph()


@pytest.fixture(scope="module")
def s3_client():
    return boto3.client("s3")


@pytest.fixture(scope="module")
def iam_client():
    """IAM client for permission checks."""
    sts_arn = boto3.client("sts").get_caller_identity()["Arn"]
    return IamClient(role_arn=sts_arn, client=boto3.client("iam"))


@pytest.fixture(scope="module")
def seeded_graph(neptune_graph):
    """Graph with nodes and edges for export testing."""
    nodes = [
        Node(id="s1", labels=["Person"], properties={"name": "Alice"}),
        Node(id="s2", labels=["Person"], properties={"name": "Bob"}),
        Node(id="s3", labels=["Person"], properties={"name": "Carol"}),
    ]
    neptune_graph.add_nodes(nodes)
    neptune_graph.add_edges([
        Edge(node_src=nodes[0], node_dest=nodes[1], label="KNOWS"),
        Edge(node_src=nodes[1], node_dest=nodes[2], label="KNOWS"),
    ])
    return neptune_graph
