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
    _format_output_graph,
    _get_graph_id,
    SessionManager,
)


class TestFormatOutputGraph:
    """Tests for _format_output_graph function."""

    def test_format_output_graph_with_details(self):
        """Test formatting with full details."""
        graph_details = {
            "name": "test-graph",
            "id": "g-123",
            "status": "AVAILABLE",
            "endpoint": "test-endpoint",
            "provisionedMemory": 16,
        }
        result = _format_output_graph(graph_details, with_details=True)
        assert result == graph_details

    def test_format_output_graph_without_details(self):
        """Test formatting with minimal details."""
        graph_details = {
            "name": "test-graph",
            "id": "g-123",
            "status": "AVAILABLE",
            "endpoint": "test-endpoint",
            "provisionedMemory": 16,
        }
        result = _format_output_graph(graph_details, with_details=False)
        assert result == {
            "name": "test-graph",
            "id": "g-123",
            "status": "AVAILABLE",
        }


class TestGetGraphId:
    """Tests for _get_graph_id function."""

    def test_get_graph_id_from_string(self):
        """Test extracting graph ID from string."""
        result = _get_graph_id("g-123")
        assert result == "g-123"

    def test_get_graph_id_from_dict(self):
        """Test extracting graph ID from dictionary."""
        graph_dict = {"id": "g-456", "name": "test-graph"}
        result = _get_graph_id(graph_dict)
        assert result == "g-456"

    def test_get_graph_id_invalid_input(self):
        """Test exception when graph ID cannot be extracted."""
        with pytest.raises(KeyError):
            _get_graph_id({"name": "test"})


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
        assert all(g["name"].startswith("test-graph") for g in graphs)

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
        assert graph["id"] == "g-1"

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
        assert graph["id"] == "g-2"
        assert graph["status"] == "AVAILABLE"

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
        assert graph["status"] == "AVAILABLE"

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
