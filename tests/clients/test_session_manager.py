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
import gc
import itertools
import os
import weakref
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from nx_neptune.session_manager import (
    CleanupTask,
    NeptuneAnalyticsClient,
    SessionManager,
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


class TestSessionManagerAutoStopOnDestroy:
    """A STOP session stops the graphs it created when it is garbage collected."""

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    async def test_gc_stops_graph_created_by_session(
        self, mock_create, mock_boto3_client
    ):
        """Dropping the last reference to a STOP session stops the graph it created."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_create.return_value = "g-new"
        mock_client.get_graph.return_value = {
            "id": "g-new",
            "name": "test-session-1",
            "status": "AVAILABLE",
        }
        # First call: nothing exists yet. Every later call: the created graph.
        mock_client.list_graphs.side_effect = itertools.chain(
            [{"graphs": []}],
            itertools.repeat(
                {
                    "graphs": [
                        {"name": "test-session-1", "id": "g-new", "status": "AVAILABLE"}
                    ]
                }
            ),
        )

        sm = SessionManager(session_name="test-session", cleanup_task=CleanupTask.STOP)
        await sm.get_or_create_graph()
        ref = weakref.ref(sm)
        del sm

        # Prove the object really died before judging the side effect, otherwise a
        # missing stop is ambiguous: broken finalizer, or test still holding a ref?
        gc.collect()
        assert ref() is None, "SessionManager still reachable; test cannot observe GC"
        mock_client.stop_graph.assert_called_once_with(graphIdentifier="g-new")

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    async def test_gc_does_not_stop_a_graph_it_did_not_create(
        self, mock_create, mock_boto3_client
    ):
        """Only graphs this session created are stopped, never ones it merely found."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_create.return_value = "g-mine"
        mock_client.get_graph.return_value = {
            "id": "g-mine",
            "name": "test-session-1",
            "status": "AVAILABLE",
        }
        mock_client.list_graphs.side_effect = itertools.chain(
            [{"graphs": []}],
            itertools.repeat(
                {
                    "graphs": [
                        {
                            "name": "test-session-1",
                            "id": "g-mine",
                            "status": "AVAILABLE",
                        },
                        # Same prefix, available, but somebody else's graph.
                        {
                            "name": "test-session-theirs",
                            "id": "g-theirs",
                            "status": "AVAILABLE",
                        },
                    ]
                }
            ),
        )

        sm = SessionManager(session_name="test-session", cleanup_task=CleanupTask.STOP)
        await sm.get_or_create_graph()
        ref = weakref.ref(sm)
        del sm

        gc.collect()
        assert ref() is None
        mock_client.stop_graph.assert_called_once_with(graphIdentifier="g-mine")

    @pytest.mark.parametrize("task", [CleanupTask.NONE, None])
    @patch("boto3.client")
    def test_gc_does_not_stop_unless_cleanup_task_is_stop(
        self, mock_boto3_client, task
    ):
        """Without CleanupTask.STOP, releasing the session touches nothing."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [{"name": "test-session-1", "id": "g-new", "status": "AVAILABLE"}]
        }

        sm = SessionManager(session_name="test-session", cleanup_task=task)
        ref = weakref.ref(sm)
        del sm

        gc.collect()
        assert ref() is None
        mock_client.stop_graph.assert_not_called()

    @patch("boto3.client")
    def test_gc_of_session_that_created_nothing_stops_nothing(self, mock_boto3_client):
        """A session that created no graph stops nothing, even inside its own prefix.

        Cleanup acts on the graphs this session created, not on everything a
        prefix scan happens to return, so a graph someone else made under the
        same prefix is left running.
        """
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.list_graphs.return_value = {
            "graphs": [
                {"name": "test-session-theirs", "id": "g-theirs", "status": "AVAILABLE"}
            ]
        }

        sm = SessionManager(session_name="test-session", cleanup_task=CleanupTask.STOP)
        ref = weakref.ref(sm)
        del sm

        gc.collect()
        assert ref() is None
        mock_client.stop_graph.assert_not_called()

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    async def test_gc_stop_failure_is_swallowed(self, mock_create, mock_boto3_client):
        """A failing stop during finalization must not raise.

        Exceptions in a finalizer never propagate; Python routes them to
        sys.unraisablehook. Asserting the hook stays unused is the only way to
        prove the failure was handled rather than merely ignored.
        """
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_create.return_value = "g-new"
        mock_client.get_graph.return_value = {
            "id": "g-new",
            "name": "test-session-1",
            "status": "AVAILABLE",
        }
        mock_client.list_graphs.side_effect = itertools.chain(
            [{"graphs": []}],
            itertools.repeat(
                {
                    "graphs": [
                        {"name": "test-session-1", "id": "g-new", "status": "AVAILABLE"}
                    ]
                }
            ),
        )
        mock_client.stop_graph.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "nope"}}, "StopGraph"
        )

        sm = SessionManager(session_name="test-session", cleanup_task=CleanupTask.STOP)
        await sm.get_or_create_graph()
        ref = weakref.ref(sm)
        del sm

        with patch("sys.unraisablehook") as unraisable:
            gc.collect()
            assert ref() is None
        mock_client.stop_graph.assert_called_once_with(graphIdentifier="g-new")
        unraisable.assert_not_called()

    @patch("boto3.client")
    @patch(
        "nx_neptune.session_manager.instance_management.stop_na_instance",
        new_callable=AsyncMock,
    )
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    def test_no_second_stop_when_exited_then_collected(
        self, mock_create, mock_stop, mock_boto3_client
    ):
        """An explicit context exit disarms the safety net; the graph is stopped once."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_create.return_value = "g-new"
        mock_client.get_graph.return_value = {
            "id": "g-new",
            "name": "test-session-1",
            "status": "AVAILABLE",
        }
        mock_client.list_graphs.side_effect = itertools.chain(
            [{"graphs": []}],
            itertools.repeat(
                {
                    "graphs": [
                        {"name": "test-session-1", "id": "g-new", "status": "AVAILABLE"}
                    ]
                }
            ),
        )

        sm = SessionManager(session_name="test-session", cleanup_task=CleanupTask.STOP)
        with sm:
            asyncio.run(sm.get_or_create_graph())
        assert mock_stop.await_count == 1  # stopped by __exit__
        ref = weakref.ref(sm)
        del sm

        gc.collect()
        assert ref() is None
        assert mock_stop.await_count == 1
        mock_client.stop_graph.assert_not_called()  # finalizer must not fire again


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


class TestSessionManagerOwnershipAndGuards:
    """Every create path arms the safety net, and the finalizer is guarded."""

    @staticmethod
    def _client(mock_boto3_client, name="test-session-1", status="AVAILABLE"):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
        mock_client.get_graph.return_value = {
            "id": "g-new",
            "name": name,
            "status": status,
        }
        mock_client.list_graphs.return_value = {
            "graphs": [{"name": name, "id": "g-new", "status": status}]
        }
        return mock_client

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch(
        "nx_neptune.session_manager.instance_management.create_na_instance_from_snapshot"
    )
    async def test_create_from_snapshot_is_owned(self, mock_create, mock_boto3_client):
        """A graph restored from a snapshot is stopped when the session is released."""
        mock_client = self._client(mock_boto3_client)
        mock_create.return_value = "g-new"

        sm = SessionManager(session_name="test-session", cleanup_task=CleanupTask.STOP)
        await sm.create_from_snapshot("snap-1")
        ref = weakref.ref(sm)
        del sm

        gc.collect()
        assert ref() is None
        mock_client.stop_graph.assert_called_once_with(graphIdentifier="g-new")

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch(
        "nx_neptune.session_manager.instance_management.create_na_instance_with_s3_import"
    )
    async def test_create_from_csv_is_owned(self, mock_create, mock_boto3_client):
        """A graph created with an S3 import is stopped when the session is released."""
        mock_client = self._client(mock_boto3_client)
        mock_create.return_value = ("g-new", "task-1")

        sm = SessionManager(session_name="test-session", cleanup_task=CleanupTask.STOP)
        await sm.create_from_csv("arn:aws:s3:::bucket/data")
        ref = weakref.ref(sm)
        del sm

        gc.collect()
        assert ref() is None
        mock_client.stop_graph.assert_called_once_with(graphIdentifier="g-new")

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    async def test_create_multiple_instances_are_owned(
        self, mock_create, mock_boto3_client
    ):
        """Every graph from a bulk create is stopped when the session is released."""
        mock_client = self._client(mock_boto3_client)
        mock_create.side_effect = ["g-a", "g-b"]
        mock_client.get_graph.side_effect = lambda graphIdentifier: {
            "id": graphIdentifier,
            "name": "test-session-1",
            "status": "AVAILABLE",
        }

        sm = SessionManager(session_name="test-session", cleanup_task=CleanupTask.STOP)
        await sm.create_multiple_instances(2)
        ref = weakref.ref(sm)
        del sm

        gc.collect()
        assert ref() is None
        assert sorted(
            c.kwargs["graphIdentifier"] for c in mock_client.stop_graph.call_args_list
        ) == ["g-a", "g-b"]

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    async def test_finalizer_skips_a_graph_that_is_not_available(
        self, mock_create, mock_boto3_client
    ):
        """A graph already stopped (or deleting) is left alone."""
        mock_client = self._client(mock_boto3_client, status="STOPPED")
        mock_create.return_value = "g-new"

        sm = SessionManager(session_name="test-session", cleanup_task=CleanupTask.STOP)
        sm._own("g-new")
        ref = weakref.ref(sm)
        del sm

        gc.collect()
        assert ref() is None
        mock_client.stop_graph.assert_not_called()

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    async def test_finalizer_skips_a_graph_outside_the_session_prefix(
        self, mock_create, mock_boto3_client
    ):
        """Defence in depth: never stop a graph whose name is not ours."""
        mock_client = self._client(mock_boto3_client, name="prod-analytics")
        mock_create.return_value = "g-new"

        sm = SessionManager(session_name="test-session", cleanup_task=CleanupTask.STOP)
        sm._own("g-new")
        ref = weakref.ref(sm)
        del sm

        gc.collect()
        assert ref() is None
        mock_client.stop_graph.assert_not_called()

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    async def test_finalizer_does_nothing_in_a_forked_child(
        self, mock_create, mock_boto3_client
    ):
        """A forked child inherits the finalizer; it must not stop the parent's graph."""
        mock_client = self._client(mock_boto3_client)
        mock_create.return_value = "g-new"

        sm = SessionManager(session_name="test-session", cleanup_task=CleanupTask.STOP)
        sm._own("g-new")
        ref = weakref.ref(sm)

        # Patch before dropping the reference: under refcounting the finalizer
        # runs on `del`, not on the later gc.collect().
        with patch("os.getpid", return_value=os.getpid() + 1):
            del sm
            gc.collect()
            assert ref() is None
        mock_client.stop_graph.assert_not_called()


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

    @pytest.mark.asyncio
    @patch("boto3.client")
    @patch("nx_neptune.session_manager.instance_management.create_na_instance")
    async def test_finalizer_requires_an_exact_session_prefix(
        self, mock_create, mock_boto3_client
    ):
        """Generated names are '<prefix>-<uuid>', so session 'test' must not own 'testing-1'."""
        client = self._client(name="testing-1")
        mock_boto3_client.return_value = client
        mock_create.return_value = "g-1"

        sm = SessionManager(session_name="test", cleanup_task=CleanupTask.STOP)
        sm._own("g-1")
        ref = weakref.ref(sm)
        del sm

        gc.collect()
        assert ref() is None
        client.stop_graph.assert_not_called()
