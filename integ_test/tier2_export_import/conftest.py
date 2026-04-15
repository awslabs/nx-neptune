# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tier 2 fixtures — requires NETWORKX_GRAPH_ID + S3 bucket."""

import logging
import os
import uuid

import boto3
import networkx as nx
import pytest
from botocore.exceptions import ClientError

from nx_neptune import NeptuneGraph, NETWORKX_GRAPH_ID, Node, Edge
from nx_neptune.clients.iam_client import IamClient

logging.basicConfig(level=logging.INFO)
logging.getLogger("nx_neptune").setLevel(logging.INFO)

S3_IMPORT_BUCKET = os.environ.get("NETWORKX_S3_IMPORT_BUCKET_PATH")
S3_EXPORT_BUCKET = os.environ.get("NETWORKX_S3_EXPORT_BUCKET_PATH")


@pytest.fixture(scope="module", autouse=True)
def _require_s3_config():
    if not NETWORKX_GRAPH_ID:
        pytest.skip("NETWORKX_GRAPH_ID not set")
    if not S3_EXPORT_BUCKET:
        pytest.skip("NETWORKX_S3_EXPORT_BUCKET_PATH not set")


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
    sts_arn = boto3.client("sts").get_caller_identity()["Arn"]
    return IamClient(role_arn=sts_arn, client=boto3.client("iam"))


@pytest.fixture(scope="module")
def seeded_graph(neptune_graph):
    """Graph with a few nodes and edges for export testing."""
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
