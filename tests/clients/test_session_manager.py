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
import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nx_neptune.session_manager import (
    CleanupTask,
    NeptuneAnalyticsClient,
    SessionManager,
    _belongs_to_session,
)

UUID_1 = "1b4e28ba-2fa1-41d2-883f-0016d3cca427"
UUID_2 = "6fa459ea-ee8a-4ca4-894e-db77e160355e"


class TestNeptuneAnalyticsClient:
    """Tests for NeptuneAnalyticsClient class."""

    def test_session_graph_init(self):
        """Test NeptuneAnalyticsClient initialization."""
        mock_client = MagicMock()
        graph = NeptuneAnalyticsClient(
            graph_id="g-123",
            client=mock_client,
            name="test-graph",
            status="AVAILABLE",
            details={"provisionedMemory": 128},
        )
        assert graph.graph_id == "g-123"
        assert graph.graph_id == "g-123"
        assert graph.name == "test-graph"
        assert graph.status == "AVAILABLE"

    def test_session_graph_from_response(self):
        """Test NeptuneAnalyticsClient.from_response class method."""
        mock_client = MagicMock()
        response = {
            "id": "g-123",
            "name": "test-graph",
            "status": "AVAILABLE",
            "endpoint": "test-endpoint",
        }
        graph = NeptuneAnalyticsClient.from_response(response, mock_client)
        assert graph.name == "test-graph"
        assert graph.graph_id == "g-123"
        assert graph.graph_id == "g-123"
        assert graph.status == "AVAILABLE"
        assert graph.details == response


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
                {"name": f"test-graph-{UUID_1}", "id": "g-1", "status": "AVAILABLE"},
                {"name": "other-graph", "id": "g-2", "status": "AVAILABLE"},
                {"name": f"test-graph-{UUID_2}", "id": "g-3", "status": "CREATING"},
                {"name": f"test-graph2-{UUID_1}", "id": "g-4", "status": "AVAILABLE"},
                {"name": f"test-graph-b-{UUID_1}", "id": "g-5", "status": "AVAILABLE"},
                {"name": "test-graph-backup", "id": "g-6", "status": "AVAILABLE"},
                {"name": "test-graph", "id": "g-7", "status": "AVAILABLE"},
            ]
        }

        sm = SessionManager(session_name="test-graph")
        graphs = sm.list_graphs()
        assert [g.graph_id for g in graphs] == ["g-1", "g-3", "g-7"]

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
        assert graph.graph_id == "g-1"

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
        assert graph.graph_id == "g-2"
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
                        "name": f"test-session-{UUID_1}",
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
        # Should return NeptuneAnalyticsClient object
        assert isinstance(result, NeptuneAnalyticsClient)
        assert result.graph_id == "test-session-graph-id"

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
        assert isinstance(graphs[0], NeptuneAnalyticsClient)
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

        assert isinstance(graph, NeptuneAnalyticsClient)
        assert graph.graph_id == "graph-1"

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
        assert graph.graph_id == "g-2"  # Should return first match

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    async def test_create_graph_ignores_existing(self, mock_create, mock_boto3_client):
        """create_graph always creates, even when a prefix-matching graph exists."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {"name": f"test-{UUID_1}", "id": "g-old", "status": "AVAILABLE"},
                {"name": f"test-{UUID_2}", "id": "g-new", "status": "AVAILABLE"},
            ]
        }
        mock_create.return_value = "g-new"

        sm = SessionManager(session_name="test")
        result = await sm.create_graph(config={"provisionedMemory": 32})

        mock_create.assert_called_once()
        assert mock_create.call_args.kwargs["config"] == {"provisionedMemory": 32}
        assert mock_create.call_args.kwargs["graph_name_prefix"] == "test"
        assert mock_create.call_args.kwargs["on_created"] is None
        assert result.graph_id == "g-new"

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    async def test_create_graph_passes_on_created(self, mock_create, mock_boto3_client):
        """create_graph forwards on_created to create_na_instance."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [{"name": f"test-{UUID_2}", "id": "g-new", "status": "AVAILABLE"}]
        }
        mock_create.return_value = "g-new"
        on_created = MagicMock()

        sm = SessionManager(session_name="test")
        await sm.create_graph(on_created=on_created)

        assert mock_create.call_args.kwargs["on_created"] is on_created

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    async def test_create_graph_failure_does_not_reuse_existing(
        self, mock_create, mock_boto3_client
    ):
        """create_graph raises on failure instead of falling back to a matching graph."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [{"name": f"test-{UUID_1}", "id": "g-old", "status": "AVAILABLE"}]
        }
        mock_create.side_effect = Exception("create failed")

        sm = SessionManager(session_name="test")
        with pytest.raises(Exception, match="create failed"):
            await sm.create_graph()

    @pytest.mark.asyncio
    @patch("boto3.client")
    async def test_get_or_create_graph_returns_existing(self, mock_boto3_client):
        """Test get_or_create_graph when graph already exists."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [{"name": f"test-{UUID_1}", "id": "g-123", "status": "AVAILABLE"}]
        }

        sm = SessionManager(session_name="test")
        result = await sm.get_or_create_graph()

        # Should return existing graph without creating new one
        assert isinstance(result, NeptuneAnalyticsClient)
        assert result.graph_id == "g-123"
        assert result.status == "AVAILABLE"
        assert result.name == f"test-{UUID_1}"

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    async def test_get_or_create_graph_uses_graph_named_as_session(
        self, mock_create, mock_boto3_client
    ):
        """A graph named exactly as the session, such as the CloudFormation graph, is reused."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {"name": "nx-neptune-graph", "id": "g-cfn", "status": "AVAILABLE"}
            ]
        }

        sm = SessionManager(session_name="nx-neptune-graph")
        result = await sm.get_or_create_graph()

        assert result.graph_id == "g-cfn"
        mock_create.assert_not_called()


class TestSessionManagerExitCleanup:
    """__exit__ honours the configured CleanupTask, not just DESTROY."""

    @patch("boto3.client")
    @patch(
        "nx_neptune.session_manager.instance_management.stop_na_instance",
        new_callable=AsyncMock,
    )
    def test_exit_with_stop_cleanup_task_stops_session_graphs(
        self, mock_stop, mock_boto3_client
    ):
        """Leaving a `with` block with CleanupTask.STOP stops the session's graphs."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [{"name": "test-graph", "id": "g-123", "status": "AVAILABLE"}]
        }

        with SessionManager(session_name="test", cleanup_task=CleanupTask.STOP):
            pass

        mock_stop.assert_awaited_once_with("g-123")

    @patch("boto3.client")
    def test_exit_with_no_cleanup_task_touches_nothing(self, mock_boto3_client):
        """The default session performs no cleanup on exit."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [{"name": "test-1", "id": "g-1", "status": "AVAILABLE"}]
        }

        with patch(
            "nx_neptune.session_manager.instance_management.stop_na_instance",
            new_callable=AsyncMock,
        ) as mock_stop:
            with SessionManager(session_name="test"):
                pass

        mock_stop.assert_not_awaited()
        mock_client.stop_graph.assert_not_called()

    @patch("boto3.client")
    @patch(
        "nx_neptune.session_manager.instance_management.stop_na_instance",
        new_callable=AsyncMock,
    )
    def test_exit_stop_skips_graphs_that_are_not_available(
        self, mock_stop, mock_boto3_client
    ):
        """A graph that is already stopped is left alone."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {"name": "test-1", "id": "g-1", "status": "AVAILABLE"},
                {"name": "test-2", "id": "g-2", "status": "STOPPED"},
            ]
        }

        with SessionManager(session_name="test", cleanup_task=CleanupTask.STOP):
            pass

        mock_stop.assert_awaited_once_with("g-1")

    @patch("boto3.client")
    @patch(
        "nx_neptune.session_manager.instance_management.stop_na_instance",
        new_callable=AsyncMock,
    )
    def test_exit_stops_even_when_the_body_raises(self, mock_stop, mock_boto3_client):
        """Cleanup still runs when the with-block body raises, and the error propagates."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [{"name": "test-1", "id": "g-1", "status": "AVAILABLE"}]
        }

        with pytest.raises(ValueError, match="boom"):
            with SessionManager(session_name="test", cleanup_task=CleanupTask.STOP):
                raise ValueError("boom")

        mock_stop.assert_awaited_once_with("g-1")


class TestSessionManagerExitRobustness:
    """Exit-time cleanup must survive the contexts the library is actually used in."""

    @staticmethod
    def _client(name="test-1", status="AVAILABLE"):
        client = MagicMock()
        client.get_caller_identity.return_value = {"Arn": "test-arn"}
        client.get_graph.return_value = {"id": "g-1", "name": name, "status": status}
        client.list_graphs.return_value = {
            "graphs": [{"name": name, "id": "g-1", "status": status}]
        }
        return client

    @pytest.mark.asyncio
    async def test_exit_works_inside_a_running_event_loop(self):
        """Every create method is async, so the with-block usually exits inside a loop."""
        with patch("boto3.client", return_value=self._client()):
            with patch(
                "nx_neptune.session_manager.instance_management.stop_na_instance",
                new_callable=AsyncMock,
            ) as mock_stop:
                with SessionManager(session_name="test", cleanup_task=CleanupTask.STOP):
                    pass

        mock_stop.assert_awaited_once_with("g-1")

    def test_stop_cleanup_task_requires_a_session_name(self):
        """STOP with no session_name would target every graph in the account."""
        with patch("boto3.client", return_value=self._client()):
            with pytest.raises(ValueError, match="session_name"):
                SessionManager(cleanup_task=CleanupTask.STOP)

    def test_exit_cleanup_failure_does_not_mask_the_body_exception(self):
        """The caller must see their own error, not a cleanup error raised on the way out."""
        with patch("boto3.client", return_value=self._client()):
            with patch(
                "nx_neptune.session_manager.instance_management.stop_na_instance",
                new_callable=AsyncMock,
                side_effect=RuntimeError("stop failed"),
            ):
                with pytest.raises(ValueError, match="THE REAL ERROR"):
                    with SessionManager(
                        session_name="test", cleanup_task=CleanupTask.STOP
                    ):
                        raise ValueError("THE REAL ERROR")

    def test_exit_cleanup_failure_raises_when_the_body_succeeded(self):
        """With no error to mask, a failed stop must not pass silently."""
        with patch("boto3.client", return_value=self._client()):
            with patch(
                "nx_neptune.session_manager.instance_management.stop_na_instance",
                new_callable=AsyncMock,
                side_effect=RuntimeError("stop failed"),
            ):
                with pytest.raises(RuntimeError, match="stop failed"):
                    with SessionManager(
                        session_name="test", cleanup_task=CleanupTask.STOP
                    ):
                        pass


class TestSessionMembership:
    """Which graphs a session owns, and so which ones cleanup may touch.

    ``__exit__`` -> ``_graph_bulk_operation`` -> ``list_graphs`` all narrow down
    to ``_belongs_to_session``, so a bug here either leaks a graph or stops one
    belonging to a different session.
    """

    @pytest.mark.parametrize(
        "graph_name,expected",
        [
            ("test-abc123", True),  # the generated '{prefix}-{uuid}' shape
            ("testing-abc123", False),  # belongs to session 'testing'
            ("testgraph", False),  # no separator: not this session's
            ("test", False),  # the bare prefix is not a session graph
            ("other-test-1", False),  # prefix must anchor at the start
            (None, False),  # list_graphs can omit the name
        ],
    )
    def test_membership_rule(self, graph_name, expected):
        assert _belongs_to_session(graph_name, "test") is expected

    @staticmethod
    def _client(name):
        client = MagicMock()
        client.get_caller_identity.return_value = {"Arn": "test-arn"}
        client.list_graphs.return_value = {
            "graphs": [{"name": name, "id": "g-1", "status": "AVAILABLE"}]
        }
        return client

    @pytest.mark.parametrize("graph_name", ["testgraph", "testing-1"])
    def test_exit_skips_a_sibling_prefix(self, graph_name):
        """Cleanup may not claim a graph whose name only shares the prefix."""
        client = self._client(graph_name)

        with patch("boto3.client", return_value=client):
            with patch(
                "nx_neptune.session_manager.instance_management.stop_na_instance",
                new_callable=AsyncMock,
            ) as mock_stop:
                with SessionManager(session_name="test", cleanup_task=CleanupTask.STOP):
                    pass

        mock_stop.assert_not_awaited()

    def test_exit_stops_a_session_graph(self):
        """Cleanup acts on a graph named '{session_name}-{uuid}'."""
        client = self._client("test-abc123")

        with patch("boto3.client", return_value=client):
            with patch(
                "nx_neptune.session_manager.instance_management.stop_na_instance",
                new_callable=AsyncMock,
            ) as mock_stop:
                with SessionManager(session_name="test", cleanup_task=CleanupTask.STOP):
                    pass

        mock_stop.assert_awaited_once_with("g-1")

    @patch("boto3.client")
    def test_list_graphs_ignores_a_sibling_prefix(self, mock_boto3_client):
        """list_graphs is the source of truth, so it applies the same rule."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {"name": "test-1", "id": "g-1", "status": "AVAILABLE"},
                {"name": "testing-1", "id": "g-2", "status": "AVAILABLE"},
            ]
        }

        graphs = SessionManager(session_name="test").list_graphs()

        assert [g.graph_id for g in graphs] == ["g-1"]


class TestCreateMultipleInstances:
    """create_multiple_instances must honour its list[str] return annotation."""

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch(
        "nx_neptune.session_manager.instance_management.create_na_instance",
        new_callable=AsyncMock,
    )
    async def test_returns_the_created_graph_ids(self, mock_create, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_create.side_effect = ["g-1", "g-2"]

        result = await SessionManager(session_name="test").create_multiple_instances(2)

        assert result == ["g-1", "g-2"]

    @pytest.mark.asyncio
    @patch("boto3.client")
    async def test_returns_an_empty_list_for_zero(self, mock_boto3_client):
        """count=0 asks for nothing, which is an empty list - not None."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}

        result = await SessionManager(session_name="test").create_multiple_instances(0)

        assert result == []


# (cleanup task, instance_management function it must run, aws CLI verb)
CLEANUP_TASKS = [
    pytest.param(CleanupTask.STOP, "stop_na_instance", "stop-graph", id="stop"),
    pytest.param(CleanupTask.RESET, "reset_graph", "reset-graph", id="reset"),
    pytest.param(
        CleanupTask.DESTROY, "delete_na_instance", "delete-graph", id="destroy"
    ),
]

OPERATION = "nx_neptune.session_manager.instance_management.{}"


def _client(graph_ids=("g-1",), status="AVAILABLE", name="test"):
    """A boto3 client double whose list_graphs returns session-owned graphs."""
    client = MagicMock()
    client.get_caller_identity.return_value = {"Arn": "test-arn"}
    client.list_graphs.return_value = {
        "graphs": [
            {"name": f"{name}-{gid}", "id": gid, "status": status} for gid in graph_ids
        ]
    }
    return client


def _recording_operation(fail_ids=()):
    """An async stand-in for an instance_management operation.

    Records which graphs were attempted and which ran to completion. A failing graph raises immediately, while the others
    need a few loop turns, so they only finish if the caller does not abort. Failure messages never mention the graph id,
    so a warning that names the id has to have got it from the cleanup itself.
    """
    attempted, completed = [], []

    async def operation(graph_id, *args, **kwargs):
        attempted.append(graph_id)
        if graph_id in fail_ids:
            raise RuntimeError("simulated AWS error")
        for _ in range(10):
            await asyncio.sleep(0)
        completed.append(graph_id)
        return graph_id

    return operation, attempted, completed


class TestExitCleanupResetAndDestroy:
    """AC1/AC2: RESET and DESTROY are awaited to completion on exit."""

    @pytest.mark.parametrize(
        "task,operation_name",
        [
            pytest.param(CleanupTask.RESET, "reset_graph", id="reset"),
            pytest.param(CleanupTask.DESTROY, "delete_na_instance", id="destroy"),
        ],
    )
    def test_exit_from_sync_code_awaits_the_operation(self, task, operation_name):
        """AC1/AC2: leaving the with-block runs the task to completion, no loop needed."""
        operation, attempted, completed = _recording_operation()

        with patch("boto3.client", return_value=_client()):
            with patch(OPERATION.format(operation_name), side_effect=operation):
                with SessionManager(session_name="test", cleanup_task=task):
                    pass

        assert completed == ["g-1"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "task,operation_name",
        [
            pytest.param(CleanupTask.RESET, "reset_graph", id="reset"),
            pytest.param(CleanupTask.DESTROY, "delete_na_instance", id="destroy"),
        ],
    )
    async def test_exit_inside_a_running_loop_awaits_the_operation(
        self, task, operation_name
    ):
        """AC1/AC2: an async caller (notebook cell) still gets cleanup completed."""
        operation, attempted, completed = _recording_operation()

        with patch("boto3.client", return_value=_client()):
            with patch(OPERATION.format(operation_name), side_effect=operation):
                with SessionManager(session_name="test", cleanup_task=task):
                    pass

        assert completed == ["g-1"]

    def test_exit_reset_skips_graphs_that_are_not_available(self):
        """AC1: RESET needs AVAILABLE (Neptune API), so a STOPPED graph is left alone."""
        with patch("boto3.client", return_value=_client(status="STOPPED")):
            with patch(OPERATION.format("reset_graph"), new_callable=AsyncMock) as op:
                with SessionManager(
                    session_name="test", cleanup_task=CleanupTask.RESET
                ):
                    pass

        op.assert_not_awaited()


def _mixed_client(statuses):
    """A boto3 client double with one session graph per ``{graph_id: status}``."""
    client = MagicMock()
    client.get_caller_identity.return_value = {"Arn": "test-arn"}
    client.list_graphs.return_value = {
        "graphs": [
            {"name": f"test-{gid}", "id": gid, "status": status}
            for gid, status in statuses.items()
        ]
    }
    return client


class TestExitCleanupDestroyStatusPolicy:
    """DESTROY deletes AVAILABLE graphs only; every other status is skipped."""

    def _exit(
        self, statuses, task=CleanupTask.DESTROY, operation_name="delete_na_instance"
    ):
        operation, attempted, completed = _recording_operation()
        with patch("boto3.client", return_value=_mixed_client(statuses)):
            with patch(OPERATION.format(operation_name), side_effect=operation):
                with SessionManager(session_name="test", cleanup_task=task):
                    pass
        return attempted, completed

    def test_destroy_leaves_a_stopped_graph_alone(self):
        """A STOPPED graph may be someone else's data under a matching prefix.

        Session membership is a name-prefix match, not a record of what this
        session created, and delete_na_instance clears deletion protection and
        skips the snapshot - so a graph stopped to preserve its data must not be
        deleted on the way out.
        """
        attempted, completed = self._exit({"g-1": "STOPPED"})

        assert attempted == []

    def test_destroy_skips_a_transitional_graph_and_warns(self, caplog):
        """A CREATING graph is not deletable here: no call, and the skip is logged."""
        with caplog.at_level(logging.WARNING):
            attempted, completed = self._exit({"g-1": "CREATING"})

        assert attempted == []
        assert any(
            "g-1" in r.getMessage() and "CREATING" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.parametrize("status", ["DELETING", "UPDATING"])
    def test_destroy_skips_other_transitional_states(self, status):
        attempted, completed = self._exit({"g-1": status})

        assert attempted == []

    def test_destroy_handles_a_mixed_session(self):
        """Only AVAILABLE is deleted; STOPPED and CREATING are left alone."""
        attempted, completed = self._exit(
            {"g-1": "AVAILABLE", "g-2": "STOPPED", "g-3": "CREATING"}
        )

        assert attempted == ["g-1"]
        assert completed == ["g-1"]

    @pytest.mark.parametrize(
        "task,operation_name",
        [
            pytest.param(CleanupTask.STOP, "stop_na_instance", id="stop"),
            pytest.param(CleanupTask.RESET, "reset_graph", id="reset"),
            pytest.param(CleanupTask.DESTROY, "delete_na_instance", id="destroy"),
        ],
    )
    def test_no_task_widens_to_stopped(self, task, operation_name):
        """Regression guard: every cleanup task stays AVAILABLE-only."""
        attempted, completed = self._exit({"g-1": "STOPPED"}, task, operation_name)

        assert attempted == []

    @pytest.mark.asyncio
    async def test_public_destroy_all_graphs_leaves_a_stopped_graph_alone(self):
        """The public API follows the same policy as the exit path."""
        operation, attempted, completed = _recording_operation()

        with patch("boto3.client", return_value=_mixed_client({"g-1": "STOPPED"})):
            with patch(OPERATION.format("delete_na_instance"), side_effect=operation):
                await SessionManager(session_name="test").destroy_all_graphs()

        assert attempted == []

    @pytest.mark.asyncio
    async def test_public_destroy_graph_leaves_a_stopped_graph_alone(self):
        """Named-graph destroy follows it too, not just the bulk sweep."""
        operation, attempted, completed = _recording_operation()

        with patch("boto3.client", return_value=_mixed_client({"g-1": "STOPPED"})):
            with patch(OPERATION.format("delete_na_instance"), side_effect=operation):
                await SessionManager(session_name="test").destroy_graph("test-g-1")

        assert attempted == []


class TestExitCleanupAttemptsEveryGraph:
    """AC3: one failing graph must not abandon the others."""

    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    def test_every_graph_is_attempted_when_the_middle_one_fails(
        self, task, operation_name, verb
    ):
        """AC3: g-1 and g-3 run to completion although g-2 fails."""
        operation, attempted, completed = _recording_operation(fail_ids={"g-2"})

        with patch("boto3.client", return_value=_client(("g-1", "g-2", "g-3"))):
            with patch(OPERATION.format(operation_name), side_effect=operation):
                with pytest.raises(ValueError, match="body"):
                    with SessionManager(session_name="test", cleanup_task=task):
                        raise ValueError("body")

        assert sorted(attempted) == ["g-1", "g-2", "g-3"]
        assert sorted(completed) == ["g-1", "g-3"]

    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    def test_the_failure_is_still_reported_after_attempting_all(
        self, task, operation_name, verb
    ):
        """AC3: finishing the others does not hide the failure from a clean body."""
        operation, attempted, completed = _recording_operation(fail_ids={"g-2"})

        with patch("boto3.client", return_value=_client(("g-1", "g-2", "g-3"))):
            with patch(OPERATION.format(operation_name), side_effect=operation):
                with pytest.raises(RuntimeError, match="simulated AWS error"):
                    with SessionManager(session_name="test", cleanup_task=task):
                        pass

        assert sorted(completed) == ["g-1", "g-3"]


class TestExitCleanupFailureReporting:
    """AC4: the STOP failure contract holds for every cleanup task."""

    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    def test_failure_does_not_mask_the_body_exception(self, task, operation_name, verb):
        """AC4: the caller sees their own error, not the cleanup's."""
        with patch("boto3.client", return_value=_client()):
            with patch(
                OPERATION.format(operation_name),
                new_callable=AsyncMock,
                side_effect=RuntimeError("cleanup failed"),
            ):
                with pytest.raises(ValueError, match="THE REAL ERROR"):
                    with SessionManager(session_name="test", cleanup_task=task):
                        raise ValueError("THE REAL ERROR")

    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    def test_failure_raises_when_the_body_succeeded(self, task, operation_name, verb):
        """AC4: with no body error to preserve, a failed cleanup must not pass silently."""
        with patch("boto3.client", return_value=_client()):
            with patch(
                OPERATION.format(operation_name),
                new_callable=AsyncMock,
                side_effect=RuntimeError("cleanup failed"),
            ):
                with pytest.raises(RuntimeError, match="cleanup failed"):
                    with SessionManager(session_name="test", cleanup_task=task):
                        pass

    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    def test_warning_names_the_failing_graph(self, task, operation_name, verb, caplog):
        """AC4: the WARNING identifies the graph that failed, and only that one."""
        operation, attempted, completed = _recording_operation(fail_ids={"g-2"})

        with caplog.at_level(logging.WARNING, logger="nx_neptune.session_manager"):
            with patch("boto3.client", return_value=_client(("g-1", "g-2"))):
                with patch(OPERATION.format(operation_name), side_effect=operation):
                    with pytest.raises(ValueError, match="body"):
                        with SessionManager(session_name="test", cleanup_task=task):
                            raise ValueError("body")

        warnings = [
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert any("g-2" in message for message in warnings)
        assert not any("g-1" in message for message in warnings)

    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    def test_warning_carries_the_remediation_command(
        self, task, operation_name, verb, caplog
    ):
        """AC4: the WARNING gives the task-appropriate manual aws command."""
        with caplog.at_level(logging.WARNING, logger="nx_neptune.session_manager"):
            with patch("boto3.client", return_value=_client()):
                with patch(
                    OPERATION.format(operation_name),
                    new_callable=AsyncMock,
                    side_effect=RuntimeError("cleanup failed"),
                ):
                    with pytest.raises(ValueError, match="body"):
                        with SessionManager(session_name="test", cleanup_task=task):
                            raise ValueError("body")

        warnings = [
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        ]
        expected = f"aws neptune-graph {verb} --graph-identifier"
        assert any(expected in message for message in warnings)


def _cancelled_operation():
    """A cleanup operation whose graph is cancelled mid-cleanup.

    The coroutine raising CancelledError is what makes asyncio.gather(...,
    return_exceptions=True) hand it back as a *returned value* (a BaseException
    that is not an Exception) rather than raising it from the gather.
    """
    attempted = []

    async def operation(graph_id, *args, **kwargs):
        attempted.append(graph_id)
        await asyncio.sleep(0)
        raise asyncio.CancelledError()

    return operation, attempted


class TestExitCleanupBaseExceptionFailure:
    """AC4: a non-Exception BaseException (CancelledError) from cleanup is a failure too."""

    @staticmethod
    def _assert_body_error_wins_and_is_reported(task, operation_name, verb, caplog):
        operation, attempted = _cancelled_operation()

        with caplog.at_level(logging.WARNING, logger="nx_neptune.session_manager"):
            with patch("boto3.client", return_value=_client()):
                with patch(OPERATION.format(operation_name), side_effect=operation):
                    with pytest.raises(ValueError, match="THE REAL ERROR"):
                        with SessionManager(session_name="test", cleanup_task=task):
                            raise ValueError("THE REAL ERROR")

        assert attempted == ["g-1"]
        warnings = [
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        ]
        expected = f"aws neptune-graph {verb} --graph-identifier"
        assert any(expected in message for message in warnings)

    @staticmethod
    def _assert_raises_when_body_succeeded(task, operation_name):
        operation, attempted = _cancelled_operation()

        with patch("boto3.client", return_value=_client()):
            with patch(OPERATION.format(operation_name), side_effect=operation):
                # CancelledError is not an Exception; the contract is only that
                # the failure is not swallowed.
                with pytest.raises(asyncio.CancelledError):
                    with SessionManager(session_name="test", cleanup_task=task):
                        pass

        assert attempted == ["g-1"]

    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    def test_cancelled_cleanup_does_not_mask_the_body_exception(
        self, task, operation_name, verb, caplog
    ):
        """AC4: sync exit; the body's ValueError wins and the warning is logged."""
        self._assert_body_error_wins_and_is_reported(task, operation_name, verb, caplog)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    async def test_cancelled_cleanup_does_not_mask_the_body_exception_in_a_loop(
        self, task, operation_name, verb, caplog
    ):
        """AC4: same contract when exit runs inside a running event loop."""
        self._assert_body_error_wins_and_is_reported(task, operation_name, verb, caplog)

    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    def test_cancelled_cleanup_raises_when_the_body_succeeded(
        self, task, operation_name, verb
    ):
        """AC4: sync exit; a cancelled cleanup with a clean body is not silent."""
        self._assert_raises_when_body_succeeded(task, operation_name)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    async def test_cancelled_cleanup_raises_when_the_body_succeeded_in_a_loop(
        self, task, operation_name, verb
    ):
        """AC4: same, inside a running event loop."""
        self._assert_raises_when_body_succeeded(task, operation_name)


class TestBulkCleanupTaskRequiresSessionName:
    """AC5: STOP, RESET and DESTROY all refuse to be built without a session_name."""

    @pytest.mark.parametrize(
        "task",
        [CleanupTask.STOP, CleanupTask.RESET, CleanupTask.DESTROY],
        ids=["stop", "reset", "destroy"],
    )
    def test_construction_without_a_session_name_raises(self, task):
        """AC5: an empty selection would match every graph in the account/region."""
        with patch("boto3.client", return_value=_client()):
            with pytest.raises(ValueError, match="session_name"):
                SessionManager(cleanup_task=task)

    def test_cleanup_task_none_does_not_require_a_session_name(self):
        """AC5 boundary: NONE targets nothing, so it needs no scope."""
        with patch("boto3.client", return_value=_client()):
            sm = SessionManager(cleanup_task=CleanupTask.NONE)

        assert sm.cleanup_task == CleanupTask.NONE


class TestPublicBulkMethodsStillRaise:
    """AC6: awaiting a public bulk method directly surfaces failures as exceptions."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method,operation_name,status",
        [
            pytest.param("stop_all_graphs", "stop_na_instance", "AVAILABLE", id="stop"),
            pytest.param(
                "start_all_graphs", "start_na_instance", "STOPPED", id="start"
            ),
            pytest.param("reset_all_graphs", "reset_graph", "AVAILABLE", id="reset"),
            pytest.param(
                "destroy_all_graphs", "delete_na_instance", "AVAILABLE", id="destroy"
            ),
        ],
    )
    async def test_a_failing_graph_raises_instead_of_being_returned(
        self, method, operation_name, status
    ):
        """AC6: the failure is raised, not tucked into the returned list."""
        operation, attempted, completed = _recording_operation(fail_ids={"g-2"})

        with patch("boto3.client", return_value=_client(("g-1", "g-2", "g-3"), status)):
            with patch(OPERATION.format(operation_name), side_effect=operation):
                sm = SessionManager(session_name="test")
                with pytest.raises(RuntimeError, match="simulated AWS error"):
                    await getattr(sm, method)()

    @pytest.mark.asyncio
    async def test_success_returns_one_result_per_graph(self):
        """AC6: the success contract (a result per graph) is unchanged."""
        operation, attempted, completed = _recording_operation()

        with patch("boto3.client", return_value=_client(("g-1", "g-2"))):
            with patch(OPERATION.format("stop_na_instance"), side_effect=operation):
                result = await SessionManager(session_name="test").stop_all_graphs()

        assert sorted(result) == ["g-1", "g-2"]


class TestExitCleanupInterruptsAlwaysPropagate:
    """KeyboardInterrupt / SystemExit are control flow, never "cleanup failures".

    Cleanup polls for minutes, so Ctrl-C during exit is an ordinary event. Even
    when the ``with`` body already raised, the interrupt must propagate; every
    other BaseException (e.g. CancelledError) still lets the body's error win.
    """

    INTERRUPTS = [
        pytest.param(KeyboardInterrupt, id="keyboard-interrupt"),
        pytest.param(lambda: SystemExit(3), id="system-exit"),
    ]

    @staticmethod
    def _interrupt_type(factory):
        return type(factory())

    @staticmethod
    def _warnings(caplog):
        return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]

    @pytest.mark.parametrize("factory", INTERRUPTS)
    def test_interrupt_in_cleanup_beats_the_body_exception(self, factory, caplog):
        """Cleanup interrupted while the body raised ValueError: interrupt wins."""
        with patch("boto3.client", return_value=_client()):
            sm = SessionManager(session_name="test", cleanup_task=CleanupTask.STOP)
        with caplog.at_level(logging.WARNING, logger="nx_neptune.session_manager"):
            with patch.object(sm, "list_graphs", side_effect=factory()):
                with pytest.raises(self._interrupt_type(factory)):
                    with sm:
                        raise ValueError("body error")

        expected = "aws neptune-graph stop-graph --graph-identifier"
        assert any(expected in message for message in self._warnings(caplog))

    @pytest.mark.parametrize("factory", INTERRUPTS)
    def test_interrupt_in_cleanup_propagates_when_the_body_succeeded(self, factory):
        """Regression guard (green already): clean body, interrupt still raised."""
        with patch("boto3.client", return_value=_client()):
            sm = SessionManager(session_name="test", cleanup_task=CleanupTask.STOP)
        with patch.object(sm, "list_graphs", side_effect=factory()):
            with pytest.raises(self._interrupt_type(factory)):
                with sm:
                    pass

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory", INTERRUPTS)
    async def test_interrupt_in_cleanup_beats_the_body_exception_in_a_loop(
        self, factory, caplog
    ):
        """Same contract when exit runs inside a running event loop."""
        with patch("boto3.client", return_value=_client()):
            sm = SessionManager(session_name="test", cleanup_task=CleanupTask.STOP)
        with caplog.at_level(logging.WARNING, logger="nx_neptune.session_manager"):
            with patch.object(sm, "list_graphs", side_effect=factory()):
                with pytest.raises(self._interrupt_type(factory)):
                    with sm:
                        raise ValueError("body error")

        expected = "aws neptune-graph stop-graph --graph-identifier"
        assert any(expected in message for message in self._warnings(caplog))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory", INTERRUPTS)
    async def test_interrupt_in_cleanup_propagates_when_the_body_succeeded_in_a_loop(
        self, factory
    ):
        """Regression guard (green already): clean body, running loop."""
        with patch("boto3.client", return_value=_client()):
            sm = SessionManager(session_name="test", cleanup_task=CleanupTask.STOP)
        with patch.object(sm, "list_graphs", side_effect=factory()):
            with pytest.raises(self._interrupt_type(factory)):
                with sm:
                    pass

    def test_cancelled_error_in_cleanup_still_lets_the_body_exception_win(self):
        """Contract guard (green already): only KeyboardInterrupt/SystemExit escape."""
        with patch("boto3.client", return_value=_client()):
            sm = SessionManager(session_name="test", cleanup_task=CleanupTask.STOP)
        with patch.object(sm, "list_graphs", side_effect=asyncio.CancelledError()):
            with pytest.raises(ValueError, match="body error"):
                with sm:
                    raise ValueError("body error")


class TestSelectGraphIdsStatusIsNotSubstringMatched:
    """A bare ``str`` status must be compared by equality, not substring.

    ``_select_graph_ids`` wraps a ``str`` into a tuple so ``graph.status in
    status_to_check`` is a membership test; without the wrap, ``in`` on a str is
    substring matching. This is regression protection, not a reachable defect:
    no real Neptune Analytics status is a proper substring of AVAILABLE or
    STOPPED.
    """

    @staticmethod
    def _select(status, wanted):
        with patch("boto3.client", return_value=_client(status=status)):
            sm = SessionManager(session_name="test", cleanup_task=CleanupTask.NONE)
            return sm._select_graph_ids(wanted, [])

    @pytest.mark.parametrize("status", ["A", "ABLE", "LAB", ""])
    def test_proper_substring_of_available_is_not_selected(self, status):
        assert self._select(status, "AVAILABLE") == []

    @pytest.mark.parametrize("status", ["S", "PPED", "STOP", ""])
    def test_proper_substring_of_stopped_is_not_selected(self, status):
        assert self._select(status, "STOPPED") == []

    def test_exact_status_is_still_selected(self):
        assert self._select("AVAILABLE", "AVAILABLE") == ["g-1"]

    @pytest.mark.parametrize("status", ["A", "ABLE", ""])
    def test_substring_status_graph_is_not_stopped_on_exit(self, status):
        with patch("boto3.client", return_value=_client(status=status)):
            with patch(
                OPERATION.format("stop_na_instance"), new_callable=AsyncMock
            ) as op:
                with SessionManager(session_name="test", cleanup_task=CleanupTask.STOP):
                    pass

        op.assert_not_awaited()


async def _ticker(counter):
    """Append to ``counter`` on every loop turn, to show the loop is still live."""
    while True:
        counter.append(None)
        await asyncio.sleep(0)


def _slow_operation():
    """An instance_management stand-in that yields to the loop while it works."""
    completed = []

    async def operation(graph_id, *args, **kwargs):
        for _ in range(5):
            await asyncio.sleep(0.01)
        completed.append(graph_id)
        return graph_id

    return operation, completed


class TestAsyncContextManagerCleanup:
    """``async with`` is the supported path in async code: it awaits cleanup."""

    @pytest.mark.asyncio
    async def test_aenter_returns_the_session(self):
        with patch("boto3.client", return_value=_client()):
            sm = SessionManager(session_name="test", cleanup_task=CleanupTask.NONE)
            async with sm as entered:
                assert entered is sm

    @pytest.mark.asyncio
    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    async def test_aexit_awaits_the_operation(self, task, operation_name, verb):
        """Every task runs to completion on the way out of an ``async with``."""
        operation, completed = _slow_operation()

        with patch("boto3.client", return_value=_client()):
            with patch(OPERATION.format(operation_name), side_effect=operation):
                async with SessionManager(session_name="test", cleanup_task=task):
                    pass

        assert completed == ["g-1"]

    @pytest.mark.asyncio
    async def test_aexit_with_no_cleanup_task_touches_nothing(self):
        with patch("boto3.client", return_value=_client()):
            with patch(
                OPERATION.format("stop_na_instance"), new_callable=AsyncMock
            ) as op:
                async with SessionManager(session_name="test"):
                    pass

        op.assert_not_awaited()


class TestAsyncExitDoesNotBlockTheCallersLoop:
    """The regression the sync hook causes in async code, and the async fix.

    ``__exit__`` is synchronous, so inside a running loop it has to bridge to a
    worker thread and block that loop until cleanup finishes - which is also why
    Ctrl-C is held for the duration. ``__aexit__`` awaits on the caller's loop
    instead, so other tasks keep running. Asserting on a concurrent task is what
    distinguishes the two; mocking the cleanup to raise cannot see it.
    """

    @pytest.mark.asyncio
    async def test_async_exit_leaves_the_loop_running(self):
        operation, completed = _slow_operation()
        ticks = []
        ticker = asyncio.create_task(_ticker(ticks))

        with patch("boto3.client", return_value=_client()):
            with patch(OPERATION.format("stop_na_instance"), side_effect=operation):
                async with SessionManager(
                    session_name="test", cleanup_task=CleanupTask.STOP
                ):
                    pass

        ticker.cancel()
        assert completed == ["g-1"]
        assert ticks, "the caller's loop made no progress during `async with` cleanup"

    @pytest.mark.asyncio
    async def test_sync_exit_inside_a_loop_blocks_it(self):
        """Documents why ``async with`` exists: the thread bridge stalls the loop."""
        operation, completed = _slow_operation()
        ticks = []
        ticker = asyncio.create_task(_ticker(ticks))
        await asyncio.sleep(0)
        ticks.clear()

        with patch("boto3.client", return_value=_client()):
            with patch(OPERATION.format("stop_na_instance"), side_effect=operation):
                with SessionManager(session_name="test", cleanup_task=CleanupTask.STOP):
                    pass

        ticker.cancel()
        assert completed == ["g-1"]
        assert ticks == [], "expected the sync exit to block the caller's loop"

    @pytest.mark.asyncio
    async def test_sync_exit_inside_a_loop_warns_and_points_at_async_with(self, caplog):
        with caplog.at_level(logging.WARNING):
            with patch("boto3.client", return_value=_client()):
                with patch(
                    OPERATION.format("stop_na_instance"), new_callable=AsyncMock
                ):
                    with SessionManager(
                        session_name="test", cleanup_task=CleanupTask.STOP
                    ):
                        pass

        assert any("async with" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_async_exit_does_not_warn(self):
        """The supported path must stay quiet, or the warning becomes noise."""
        with patch("boto3.client", return_value=_client()):
            with patch(OPERATION.format("stop_na_instance"), new_callable=AsyncMock):
                with patch("nx_neptune.session_manager.logger") as mock_logger:
                    async with SessionManager(
                        session_name="test", cleanup_task=CleanupTask.STOP
                    ):
                        pass

        mock_logger.warning.assert_not_called()

    def test_sync_exit_outside_a_loop_does_not_warn(self):
        """Synchronous callers are a supported path too, so no warning there."""
        with patch("boto3.client", return_value=_client()):
            with patch(OPERATION.format("stop_na_instance"), new_callable=AsyncMock):
                with patch("nx_neptune.session_manager.logger") as mock_logger:
                    with SessionManager(
                        session_name="test", cleanup_task=CleanupTask.STOP
                    ):
                        pass

        mock_logger.warning.assert_not_called()


class TestAsyncExitFailureHandling:
    """``__aexit__`` must share ``__exit__``'s failure contract, not re-derive it."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    async def test_failure_does_not_mask_the_body_exception(
        self, task, operation_name, verb
    ):
        with patch("boto3.client", return_value=_client()):
            with patch(
                OPERATION.format(operation_name),
                new_callable=AsyncMock,
                side_effect=RuntimeError("cleanup failed"),
            ):
                with pytest.raises(ValueError, match="THE REAL ERROR"):
                    async with SessionManager(session_name="test", cleanup_task=task):
                        raise ValueError("THE REAL ERROR")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    async def test_failure_raises_when_the_body_succeeded(
        self, task, operation_name, verb
    ):
        with patch("boto3.client", return_value=_client()):
            with patch(
                OPERATION.format(operation_name),
                new_callable=AsyncMock,
                side_effect=RuntimeError("cleanup failed"),
            ):
                with pytest.raises(RuntimeError, match="cleanup failed"):
                    async with SessionManager(session_name="test", cleanup_task=task):
                        pass

    @pytest.mark.asyncio
    @pytest.mark.parametrize("task,operation_name,verb", CLEANUP_TASKS)
    async def test_failure_warning_carries_the_remediation_command(
        self, task, operation_name, verb, caplog
    ):
        with caplog.at_level(logging.WARNING):
            with patch("boto3.client", return_value=_client()):
                with patch(
                    OPERATION.format(operation_name),
                    new_callable=AsyncMock,
                    side_effect=RuntimeError("cleanup failed"),
                ):
                    with pytest.raises(RuntimeError):
                        async with SessionManager(
                            session_name="test", cleanup_task=task
                        ):
                            pass

        assert any(
            f"aws neptune-graph {verb}" in r.getMessage() for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_interrupt_in_cleanup_beats_the_body_exception(self):
        """Ctrl-C is control flow, not a cleanup failure: it must still win."""
        with patch("boto3.client", return_value=_client()):
            sm = SessionManager(session_name="test", cleanup_task=CleanupTask.STOP)
        with patch.object(sm, "list_graphs", side_effect=KeyboardInterrupt()):
            with pytest.raises(KeyboardInterrupt):
                async with sm:
                    raise ValueError("body error")

    @pytest.mark.asyncio
    async def test_cancelled_cleanup_still_lets_the_body_exception_win(self):
        with patch("boto3.client", return_value=_client()):
            sm = SessionManager(session_name="test", cleanup_task=CleanupTask.STOP)
        with patch.object(sm, "list_graphs", side_effect=asyncio.CancelledError()):
            with pytest.raises(ValueError, match="body error"):
                async with sm:
                    raise ValueError("body error")
