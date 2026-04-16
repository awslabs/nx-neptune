# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Graph operations fixtures — requires only NETWORKX_GRAPH_ID."""

import pytest
import networkx as nx

from nx_neptune import NeptuneGraph, NETWORKX_GRAPH_ID, SessionManager


@pytest.fixture(scope="module", autouse=True)
def _require_graph_id():
    if not NETWORKX_GRAPH_ID:
        pytest.skip("NETWORKX_GRAPH_ID not set")


@pytest.fixture(scope="module")
def neptune_graph():
    """NeptuneGraph instance backed by the test graph."""
    g = nx.Graph()
    na_graph = NeptuneGraph.from_config(graph=g)
    na_graph.clear_graph()
    yield na_graph
    na_graph.clear_graph()


@pytest.fixture(scope="module")
def session_manager():
    """SessionManager instance for read-only operations."""
    return SessionManager()


def pytest_runtest_setup(item):
    """Clear the graph before any test whose name contains 'empty_graph'."""
    if "empty_graph" in item.name:
        g = nx.Graph()
        NeptuneGraph.from_config(graph=g).clear_graph()
