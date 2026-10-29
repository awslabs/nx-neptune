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
import pytest
from unittest.mock import MagicMock, patch
import logging
import networkx as nx

from nx_neptune.clients import (
    PARAM_TRAVERSAL_DIRECTION_BOTH,
    PARAM_TRAVERSAL_DIRECTION_INBOUND,
    PARAM_TRAVERSAL_DIRECTION_OUTBOUND,
)
from nx_neptune.na_graph import NeptuneGraph
from nx_plugin import NeptuneConfig
from nx_neptune.clients import (
    NeptuneAnalyticsClient,
    insert_node,
    Node,
    Edge,
    clear_query,
    match_all_nodes,
    match_all_edges,
    update_node,
    delete_node,
    insert_edge,
    update_edge,
    delete_edge,
)

CONFIGURED_ROLE = "arn:aws:iam::123456789012:role/ConfiguredRole"
CALLER_ROLE = "arn:aws:iam::123456789012:role/CallerRole"


class TestNeptuneGraph:
    @pytest.fixture
    def mock_client(self):
        """Create a mock NeptuneAnalyticsClient"""
        mock = MagicMock(spec=NeptuneAnalyticsClient)
        mock.execute_generic_query.return_value = {"client": "response"}
        return mock

    @pytest.fixture
    def neptune_graph(self, mock_client):
        """Create a NeptuneGraph instance with a mock client"""
        return NeptuneGraph(
            na_client=mock_client, iam_client=mock_client, graph=nx.Graph()
        )

    @pytest.fixture
    def neptune_digraph(self, mock_client):
        """Create a NeptuneGraph instance with a mock client"""
        return NeptuneGraph(
            na_client=mock_client, iam_client=mock_client, graph=nx.DiGraph()
        )

    @patch("logging.getLogger")
    @patch("boto3.client")
    def test_init_default(self, boto_client, getLogger):
        """Test initialization with default parameters"""
        graph = MagicMock()
        test_na_graph = NeptuneGraph(boto_client, boto_client, graph)

        getLogger.is_called_once_with(__name__)
        assert test_na_graph.logger == getLogger.return_value

        assert test_na_graph.na_client == boto_client
        assert test_na_graph.iam_client == boto_client
        assert test_na_graph.graph == graph

    def test_init_custom(self, mock_client):
        """Test initialization with custom parameters"""
        custom_logger = logging.getLogger("custom")
        custom_graph = nx.Graph()
        custom_graph.add_node("testNode")

        test_na_graph = NeptuneGraph(
            na_client=mock_client,
            iam_client=mock_client,
            graph=custom_graph,
            logger=custom_logger,
        )

        assert test_na_graph.logger == custom_logger
        assert test_na_graph.na_client == mock_client
        assert list(test_na_graph.graph_object().nodes) == ["testNode"]

    def test_graph_object(self, neptune_graph):
        """Test graph_object() method returns the cached graph"""
        cache = neptune_graph.graph_object()
        assert cache == neptune_graph.graph
        assert isinstance(cache, nx.Graph)

    def test_traversal_direction(self, neptune_graph, neptune_digraph):
        assert neptune_graph.traversal_direction(True) == PARAM_TRAVERSAL_DIRECTION_BOTH
        assert (
            neptune_graph.traversal_direction(False) == PARAM_TRAVERSAL_DIRECTION_BOTH
        )

        assert (
            neptune_digraph.traversal_direction(True)
            == PARAM_TRAVERSAL_DIRECTION_INBOUND
        )
        assert (
            neptune_digraph.traversal_direction(False)
            == PARAM_TRAVERSAL_DIRECTION_OUTBOUND
        )

    def test_add_node(self, neptune_graph, mock_client):
        """Test add_node method"""
        node = Node(id=123, properties={"name": "TestNode", "prop": "value"})

        # execute
        result = neptune_graph.add_node(node)

        # Verify the correct query was built and executed
        expected_query, param_values = insert_node(node)
        mock_client.execute_generic_query.assert_called_once_with(
            expected_query, param_values
        )
        assert result == {"client": "response"}

    def test_update_nodes(self, neptune_graph, mock_client):
        """Test update_nodes method"""
        match_labels = "Person"
        ref_name = "n"
        nodes = [Node(id="John")]
        properties_set = {"n.age": 30}

        # execute
        result = neptune_graph.update_nodes(
            match_labels, ref_name, nodes, properties_set
        )

        # Verify the correct query was built and executed
        expected_query, param_values = update_node(
            match_labels, ref_name, ["John"], properties_set
        )
        mock_client.execute_generic_query.assert_called_once_with(
            expected_query, param_values
        )
        assert result == {"client": "response"}

    def test_delete_nodes(self, neptune_graph, mock_client):
        """Test delete_nodes method"""
        node = Node(id=123, properties={"name": "TestNode", "prop": "value"})

        # execute
        result = neptune_graph.delete_nodes(node)

        # Verify the correct query was built and executed
        expected_query, param_values = delete_node(node)
        mock_client.execute_generic_query.assert_called_once_with(
            expected_query, param_values
        )
        assert result == {"client": "response"}

    def test_clear_graph(self, neptune_graph, mock_client):
        """Test clear_graph method"""
        result = neptune_graph.clear_graph()

        # Verify the correct query was built and executed
        expected_query = clear_query()
        mock_client.execute_generic_query.assert_called_once_with(expected_query)
        assert result == {"client": "response"}

    def test_add_edge(self, neptune_graph, mock_client):
        """Test add_edge method"""
        src_node = Node("Alice")
        dst_node = Node("Bob")
        edge = Edge(src_node, dst_node, label="KNOWS")

        result = neptune_graph.add_edge(edge)

        # Verify the correct query was built and executed
        expected_query, param_values = insert_edge(edge)
        mock_client.execute_generic_query.assert_called_once_with(
            expected_query, param_values
        )
        assert result == {"client": "response"}

    def test_update_edges(self, neptune_graph, mock_client):
        """Test update_edges method"""
        src_node = Node("Tarzan", labels=["Person"], properties={"name": "Tarzan"})
        dst_node = Node("Jane", labels=["Person"], properties={"name": "Jane"})
        edge = Edge(src_node, dst_node, label="KNOWS")

        ref_name_src = "a"
        ref_name_edge = "e"
        ref_name_des = "b"
        where_filters = {"a.name": "Tarzan", "b.name": "Jane"}
        properties_set = {"e.since": 2020}

        result = neptune_graph.update_edges(
            ref_name_src,
            ref_name_edge,
            ref_name_des,
            edge,
            where_filters,
            properties_set,
        )

        # Verify the correct query was built and executed
        expected_query, param_values = update_edge(
            ref_name_src,
            ref_name_edge,
            edge,
            ref_name_des,
            where_filters,
            properties_set,
        )
        mock_client.execute_generic_query.assert_called_once_with(
            expected_query, param_values
        )
        assert result == {"client": "response"}

    def test_delete_edges(self, neptune_graph, mock_client):
        """Test delete_edges method"""
        src_node = Node("Tarzan", labels=["Person"], properties={"name": "Tarzan"})
        dst_node = Node("Jane", labels=["Person"], properties={"name": "Jane"})
        edge = Edge(src_node, dst_node, label="KNOWS", properties={"since": 2020})

        # exercise
        result = neptune_graph.delete_edges(edge)

        # Verify the correct query was built and executed
        expected_query, param_values = delete_edge(edge)
        mock_client.execute_generic_query.assert_called_once_with(
            expected_query, param_values
        )
        assert result == {"client": "response"}

    def test_get_all_nodes(self, neptune_graph, mock_client):
        mock_client.execute_generic_query.return_value = [
            {"n": "node1"},
            {"n": "node2"},
        ]

        """Test get_all_nodes method"""
        result = neptune_graph.get_all_nodes()

        # Verify the correct query was built and executed
        expected_query = match_all_nodes()
        mock_client.execute_generic_query.assert_called_once_with(expected_query)
        assert result == ["node1", "node2"]

    def test_get_all_edges(self, neptune_graph, mock_client):
        mock_client.execute_generic_query.return_value = [
            {"r": "relationship1"},
            {"r": "relationship1"},
        ]

        """Test get_all_edges method"""
        result = neptune_graph.get_all_edges()

        # Verify the correct query was built and executed
        expected_query = match_all_edges()
        mock_client.execute_generic_query.assert_called_once_with(expected_query)
        assert result == ["relationship1", "relationship1"]

    @patch("nx_neptune.na_graph.ClientFactory")
    @patch("nx_neptune.na_graph.NeptuneAnalyticsClient")
    @patch("nx_neptune.na_graph.get_config")
    def test_from_graph_id_uses_configured_role(
        self, mock_get_config, mock_na_cls, mock_factory
    ):
        """from_graph_id binds the configured s3_iam_role without calling STS."""
        mock_get_config.return_value = NeptuneConfig(s3_iam_role=CONFIGURED_ROLE)

        result = NeptuneGraph.from_graph_id("g-abc123")

        assert mock_na_cls.call_args.args[0] == "g-abc123"
        assert result.iam_client.role_arn == CONFIGURED_ROLE
        mock_factory.return_value.sts.assert_not_called()
        assert isinstance(result.graph, nx.Graph)
        assert result.graph.number_of_nodes() == 0

    @patch("nx_neptune.na_graph.ClientFactory")
    @patch("nx_neptune.na_graph.NeptuneAnalyticsClient")
    @patch("nx_neptune.na_graph.get_config")
    def test_from_graph_id_falls_back_to_caller_identity(
        self, mock_get_config, mock_na_cls, mock_factory
    ):
        """from_graph_id uses the caller identity when s3_iam_role is unset."""
        mock_get_config.return_value = NeptuneConfig(s3_iam_role=None)
        mock_factory.return_value.sts.return_value.get_caller_identity.return_value = {
            "Arn": CALLER_ROLE
        }

        result = NeptuneGraph.from_graph_id("g-abc123")

        assert mock_na_cls.call_args.args[0] == "g-abc123"
        assert result.iam_client.role_arn == CALLER_ROLE

    @patch("nx_neptune.na_graph.ClientFactory")
    @patch("nx_neptune.na_graph.NeptuneAnalyticsClient")
    def test_from_config_graph_id_overrides_config(self, mock_na_cls, mock_factory):
        """An explicit graph_id wins over config.graph_id."""
        config = NeptuneConfig(graph_id="g-from-config", s3_iam_role=CONFIGURED_ROLE)

        NeptuneGraph.from_config(config, graph_id="g-override")

        assert mock_na_cls.call_args.args[0] == "g-override"
        assert config.graph_id == "g-from-config"
