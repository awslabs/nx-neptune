# Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""
Integration test proving that the skip_graph_reset flag controls whether
_sync_data_to_neptune clears the graph before syncing data.

- skip_graph_reset=True  → pre-existing data is preserved
- skip_graph_reset=False → pre-existing data is wiped before sync

Usage:
    export NETWORKX_GRAPH_ID=g-your-graph-id
    pytest integ_test/test_security_graph_reset.py -v
"""
import pytest
import networkx as nx

from nx_plugin.config import NeptuneConfig
from nx_neptune import NeptuneGraph, NETWORKX_GRAPH_ID
from nx_neptune.clients import Node
from nx_neptune.utils.decorators import _sync_data_to_neptune

SEED_NODE_ID = "seed_reset_test"
SYNC_NODE_ID = "sync_reset_test"


@pytest.fixture(scope="module")
def na_graph():
    if not NETWORKX_GRAPH_ID:
        pytest.skip('Environment Variable "NETWORKX_GRAPH_ID" is not defined')

    g = nx.Graph()
    graph = NeptuneGraph.from_config(graph=g)
    yield graph
    graph.clear_graph()


def _get_node_ids(na_graph):
    return {n["~id"] for n in na_graph.get_all_nodes()}


def _seed_graph(na_graph):
    """Clear and insert a single seed node."""
    na_graph.clear_graph()
    na_graph.add_node(Node(id=SEED_NODE_ID, labels=["ResetTest"], properties={}))


def _build_nx_graph():
    """Build a small NetworkX graph to sync."""
    g = nx.Graph()
    g.add_node(SYNC_NODE_ID, label="ResetTest")
    return g


class TestSkipGraphResetTrue:
    """When skip_graph_reset=True, pre-existing data must survive sync."""

    def test_seed_node_preserved_after_sync(self, na_graph):
        _seed_graph(na_graph)
        assert SEED_NODE_ID in _get_node_ids(na_graph)

        config = NeptuneConfig(graph_id=NETWORKX_GRAPH_ID, skip_graph_reset=True)
        _sync_data_to_neptune(_build_nx_graph(), na_graph, config)

        node_ids = _get_node_ids(na_graph)
        assert SEED_NODE_ID in node_ids, "Seed node was wiped despite skip_graph_reset=True"
        assert SYNC_NODE_ID in node_ids, "Synced node missing"


class TestSkipGraphResetFalse:
    """When skip_graph_reset=False, pre-existing data must be cleared before sync."""

    def test_seed_node_wiped_after_sync(self, na_graph):
        _seed_graph(na_graph)
        assert SEED_NODE_ID in _get_node_ids(na_graph)

        config = NeptuneConfig(graph_id=NETWORKX_GRAPH_ID, skip_graph_reset=False)
        _sync_data_to_neptune(_build_nx_graph(), na_graph, config)

        node_ids = _get_node_ids(na_graph)
        assert SEED_NODE_ID not in node_ids, "Seed node survived despite skip_graph_reset=False"
        assert SYNC_NODE_ID in node_ids, "Synced node missing"
