# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Integration tests for SessionManager read-only operations."""

import pytest
from nx_neptune import NETWORKX_GRAPH_ID, SessionManager
from nx_neptune.clients import NeptuneAnalyticsClient


class TestListGraphs:

    def test_list_graphs_returns_list(self, session_manager):
        graphs = session_manager.list_graphs()
        assert isinstance(graphs, list)
        assert len(graphs) > 0

    def test_list_graphs_contains_test_graph(self, session_manager):
        graphs = session_manager.list_graphs()
        graph_ids = [g.graph_id for g in graphs]
        assert NETWORKX_GRAPH_ID in graph_ids

    def test_list_graphs_returns_client_objects(self, session_manager):
        graphs = session_manager.list_graphs()
        for g in graphs:
            assert isinstance(g, NeptuneAnalyticsClient)
            assert g.graph_id is not None


class TestGetGraph:

    def test_get_graph_by_id(self, session_manager):
        graph = session_manager.get_graph(NETWORKX_GRAPH_ID)
        assert isinstance(graph, NeptuneAnalyticsClient)
        assert graph.graph_id == NETWORKX_GRAPH_ID

    def test_get_graph_invalid_id_raises(self, session_manager):
        with pytest.raises(Exception, match="No graph instance"):
            session_manager.get_graph("g-nonexistent-000000")


class TestSessionNameFiltering:

    def test_session_name_filters_graphs(self):
        sm = SessionManager(session_name="zzz-no-match-prefix")
        graphs = sm.list_graphs()
        assert len(graphs) == 0


class TestValidatePermissions:

    def test_validate_permissions_returns_dict(self, session_manager):
        result = session_manager.validate_permissions()
        assert isinstance(result, dict)
        assert len(result) > 0
