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

from nx_neptune.session_manager import (
    _get_graph_id,
    SessionGraph,
    SessionManager,
)


class TestSessionGraph:
    """Tests for SessionGraph class."""

    def test_session_graph_init(self):
        """Test SessionGraph initialization."""
        graph = SessionGraph(
            id="g-123",
            name="test-graph",
            status="AVAILABLE",
            details={"provisionedMemory": 128},
        )
        assert graph.id == "g-123"
        assert graph.name == "test-graph"
        assert graph.status == "AVAILABLE"

    def test_session_graph_from_response(self):
        """Test SessionGraph.from_response class method."""
        response = {
            "id": "g-123",
            "name": "test-graph",
            "status": "AVAILABLE",
            "endpoint": "test-endpoint",
        }
        graph = SessionGraph.from_response(response)
        assert graph.name == "test-graph"
        assert graph.id == "g-123"
        assert graph.status == "AVAILABLE"
        assert graph.details == response


class TestGetGraphId:
    """Tests for _get_graph_id function."""

    def test_get_graph_id_from_string(self):
        """Test extracting graph ID from string."""
        result = _get_graph_id("g-123")
        assert result == "g-123"

    def test_get_graph_id_from_obj(self):
        """Test extracting graph ID from dictionary."""
        graph_obj = SessionGraph.from_response(
            {"id": "g-456", "name": "test-graph", "status": "TEST"}
        )
        result = _get_graph_id(graph_obj)
        assert result == "g-456"

    def test_get_graph_id_invalid_input(self):
        """Test exception when graph ID cannot be extracted."""
        with pytest.raises(KeyError):
            _get_graph_id(SessionGraph.from_response({"name": "test"}))

    def test_get_graph_id_empty_dict(self):
        """Test exception when graph is empty dict."""
        with pytest.raises(KeyError):
            _get_graph_id(SessionGraph.from_response({}))

    def test_get_graph_id_empty_id_value(self):
        """Test that empty id value is returned as-is."""
        result = _get_graph_id(
            SessionGraph(id="", name="test", status="AVAILABLE", details={})
        )
        assert result == ""


class TestSessionManager:
    """Tests for SessionManager class."""

    @patch("boto3.client")
    def test_session_manager_init(self, mock_boto3_client):
        """Test SessionManager initialization."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}

        sm = SessionManager(session_name="test-session")
        assert sm.session_name == "test-session"

    @patch("boto3.client")
    def test_session_classmethod(self, mock_boto3_client):
        """Test SessionManager.session() class method."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}

        sm = SessionManager.session("test-session")
        assert isinstance(sm, SessionManager)
        assert sm.session_name == "test-session"

    @patch("boto3.client")
    def test_list_graphs_no_filter(self, mock_boto3_client):
        """Test listing graphs without session name filter."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {"name": "graph-1", "id": "g-1", "status": "AVAILABLE"},
                {"name": "graph-2", "id": "g-2", "status": "CREATING"},
            ]
        }

        sm = SessionManager()
        graphs = sm.list_graphs()
        assert len(graphs) == 2

    @patch("boto3.client")
    def test_list_graphs_with_filter(self, mock_boto3_client):
        """Test listing graphs with session name filter."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {"name": "test-graph-1", "id": "g-1", "status": "AVAILABLE"},
                {"name": "other-graph", "id": "g-2", "status": "AVAILABLE"},
                {"name": "test-graph-2", "id": "g-3", "status": "CREATING"},
            ]
        }

        sm = SessionManager(session_name="test-graph")
        graphs = sm.list_graphs()
        assert len(graphs) == 2
        assert all(g.name.startswith("test-graph") for g in graphs)

    @patch("boto3.client")
    def test_get_existing_graph_no_filter(self, mock_boto3_client):
        """Test getting existing graph without status filter."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {"name": "graph-1", "id": "g-1", "status": "AVAILABLE"},
                {"name": "graph-2", "id": "g-2", "status": "CREATING"},
            ]
        }

        sm = SessionManager()
        graph = sm._get_existing_graph()
        assert graph is not None
        assert graph.id == "g-1"

    @patch("boto3.client")
    def test_get_existing_graph_with_status_filter(self, mock_boto3_client):
        """Test getting existing graph with status filter."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {"name": "graph-1", "id": "g-1", "status": "CREATING"},
                {"name": "graph-2", "id": "g-2", "status": "AVAILABLE"},
            ]
        }

        sm = SessionManager()
        graph = sm._get_existing_graph(filter_status=["AVAILABLE"])
        assert graph is not None
        assert graph.id == "g-2"
        assert graph.status == "AVAILABLE"

    @patch("boto3.client")
    def test_get_existing_graph_case_insensitive(self, mock_boto3_client):
        """Test status filter is case-insensitive."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {"name": "graph-1", "id": "g-1", "status": "AVAILABLE"},
            ]
        }

        sm = SessionManager()
        graph = sm._get_existing_graph(filter_status=["available"])
        assert graph is not None
        assert graph.status == "AVAILABLE"

    @patch("boto3.client")
    def test_get_existing_graph_no_match(self, mock_boto3_client):
        """Test getting existing graph when no status matches."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {"name": "graph-1", "id": "g-1", "status": "CREATING"},
            ]
        }

        sm = SessionManager()
        graph = sm._get_existing_graph(filter_status=["AVAILABLE"])
        assert graph is None

    @patch("boto3.client")
    def test_get_existing_graph_empty_list(self, mock_boto3_client):
        """Test getting existing graph when no graphs exist."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {"graphs": []}

        sm = SessionManager()
        graph = sm._get_existing_graph()
        assert graph is None

    @patch("boto3.client")
    def test_validate_permissions(self, mock_boto3_client):
        """Test validate_permissions method."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}

        with patch(
            "nx_neptune.session_manager.instance_management.validate_permissions"
        ) as mock_validate:
            mock_validate.return_value = True
            sm = SessionManager()
            result = sm.validate_permissions()
            assert result is True
            mock_validate.assert_called_once()

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    async def test_get_or_create_graph_creates_new(
        self, mock_create, mock_boto3_client
    ):
        """Test get_or_create_graph when no graphs exist."""

        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}

        # First call returns empty list, second call returns the created graph
        mock_client.list_graphs.side_effect = [
            {"graphs": []},  # First call - no graphs exist
            {
                "graphs": [
                    {
                        "name": "test-session-graph",
                        "id": "test-session-graph-id",
                        "status": "AVAILABLE",
                    }
                ]
            },  # Second call - graph exists
        ]

        # Mock the async function to return a string
        mock_create.return_value = "test-session-graph-id"

        sm = SessionManager(session_name="test-session")
        result = await sm.get_or_create_graph()

        # Should call create_na_instance
        mock_create.assert_called_once()
        # Should return SessionGraph object
        assert isinstance(result, SessionGraph)
        assert result.id == "test-session-graph-id"

    @patch("boto3.client")
    def test_list_graphs_with_details(self, mock_boto3_client):
        """Test listing graphs with full details."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {
                    "name": "graph-1",
                    "id": "g-1",
                    "status": "AVAILABLE",
                    "endpoint": "test-endpoint",
                    "provisionedMemory": 16,
                }
            ]
        }

        sm = SessionManager()
        graphs = sm.list_graphs()
        assert len(graphs) == 1
        assert isinstance(graphs[0], SessionGraph)
        assert graphs[0].details is not None
        assert "endpoint" in graphs[0].details
        assert "provisionedMemory" in graphs[0].details

    @patch("boto3.client")
    def test_get_graph_success(self, mock_boto3_client):
        """Test getting a graph by ID successfully."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {
                    "name": "g-1",
                    "id": "graph-1",
                    "status": "AVAILABLE",
                    "endpoint": "test-endpoint",
                    "provisionedMemory": 16,
                },
            ]
        }

        sm = SessionManager()
        graph = sm.get_graph("graph-1")

        assert isinstance(graph, SessionGraph)
        assert graph.id == "graph-1"

    @patch("boto3.client")
    def test_get_graph_failed(self, mock_boto3_client):
        """Test listing graphs with full details."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {
                    "name": "graph-1",
                    "id": "g-1",
                    "status": "AVAILABLE",
                    "endpoint": "test-endpoint",
                    "provisionedMemory": 16,
                },
            ]
        }

        sm = SessionManager()
        with pytest.raises(Exception):
            sm.get_graph("g-2")

    @patch("boto3.client")
    def test_get_existing_graph_multiple_status_filters(self, mock_boto3_client):
        """Test getting existing graph with multiple status filters."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {"name": "graph-1", "id": "g-1", "status": "CREATING"},
                {"name": "graph-2", "id": "g-2", "status": "AVAILABLE"},
                {"name": "graph-3", "id": "g-3", "status": "STOPPED"},
            ]
        }

        sm = SessionManager()
        graph = sm._get_existing_graph(filter_status=["AVAILABLE", "STOPPED"])
        assert graph is not None
        assert graph.id == "g-2"  # Should return first match

    @pytest.mark.asyncio
    @patch("boto3.client")
    async def test_get_or_create_graph_returns_existing(self, mock_boto3_client):
        """Test get_or_create_graph when graph already exists."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [{"name": "test-graph", "id": "g-123", "status": "AVAILABLE"}]
        }

        sm = SessionManager(session_name="test")
        result = await sm.get_or_create_graph()

        # Should return existing graph without creating new one
        assert isinstance(result, SessionGraph)
        assert result.id == "g-123"
        assert result.status == "AVAILABLE"
        assert result.name == "test-graph"
