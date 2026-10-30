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
import os
import asyncio
from asyncio import Future
from unittest.mock import AsyncMock, MagicMock, patch, call, ANY

import pytest
import networkx as nx

from nx_neptune.utils.decorators import (
    configure_if_nx_active,
    _execute_setup_routines_on_graph,
    _execute_setup_new_graph,
    _sync_data_to_neptune,
    _execute_teardown_routines_on_graph,
)
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.clients import Edge, Node
from nx_plugin import NeptuneConfig


def _teardown_config(
    graph_id="test-graph-id",
    export_s3_bucket=None,
    save_snapshot=False,
    destroy_instance=False,
    stop_instance=False,
    reset_graph=False,
):
    """Mock NeptuneConfig with every teardown flag set explicitly."""
    cfg = MagicMock(spec=NeptuneConfig)
    cfg.graph_id = graph_id
    cfg.export_s3_bucket = export_s3_bucket
    cfg.save_snapshot = save_snapshot
    cfg.destroy_instance = destroy_instance
    cfg.stop_instance = stop_instance
    cfg.reset_graph = reset_graph
    return cfg


class TestConfigureIfNxActive:
    """Tests for the configure_if_nx_active decorator"""

    @patch("nx_neptune.utils.decorators.get_config")
    def test_pytest_environment(self, mock_get_config):
        """Test that the decorator returns the original function when in pytest environment"""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            # Create a test function
            def test_func(graph, arg1, arg2=None):
                return f"Original function called with {graph}, {arg1}, {arg2}"

            # Apply the decorator
            decorated_func = configure_if_nx_active()(test_func)

            # Call the decorated function
            graph = nx.Graph()
            result = decorated_func(graph, "test_arg", arg2="test_kwarg")

            # Verify the original function was called
            assert (
                result
                == "Original function called with " + f"{graph}, test_arg, test_kwarg"
            )

            # Verify get_config was not called
            mock_get_config.assert_not_called()

    @patch("nx_neptune.utils.decorators.get_config")
    @patch("nx_neptune.utils.decorators.NeptuneGraph")
    @patch("nx_neptune.utils.decorators.asyncio.run")
    @patch("nx_neptune.utils.decorators.asyncio.get_running_loop")
    # MagicMock (not the auto AsyncMock): asyncio.run is mocked, so the coroutine
    # would otherwise never be awaited.
    @patch(
        "nx_neptune.utils.decorators._execute_teardown_routines_on_graph",
        new_callable=MagicMock,
    )
    @patch(
        "nx_neptune.utils.decorators._execute_setup_routines_on_graph",
        new_callable=MagicMock,
    )
    @patch("nx_neptune.utils.decorators._sync_data_to_neptune")
    def test_with_existing_graph_id(
        self,
        mock_sync_data,
        mock_execute_setup_routines_on_graph,
        mock_execute_teardown_routines_on_graph,
        mock_get_running_loop,
        mock_asyncio_run,
        mock_neptune_graph,
        mock_get_config,
    ):
        """Test the decorator with an existing graph_id"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.graph_id = "test-graph-id"
        mock_config.create_new_instance = False
        mock_get_config.return_value = mock_config
        mock_config.validate_config.return_value = None

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)
        mock_neptune_graph.from_config.return_value = mock_na_graph
        g = nx.Graph()

        # Setup mock for event loop
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = False
        mock_get_running_loop.return_value = mock_loop

        # Setup mock for asyncio.run
        mock_asyncio_run.side_effect = [mock_config, None]  # For setup and teardown

        # Create a test function
        def test_func(graph, arg1, arg2=None):
            return f"Function called with {graph}, {arg1}, {arg2}"

        # Apply the decorator
        decorated_func = configure_if_nx_active()(test_func)

        # Call the decorated function
        result = decorated_func(g, "test_arg", arg2="test_kwarg")

        # Verify the function was called with the NeptuneGraph
        assert result == f"Function called with {mock_na_graph}, test_arg, test_kwarg"

        # Verify config was validated
        mock_config.validate_config.assert_called_once()

        # Verify NeptuneGraph was created
        mock_neptune_graph.from_config.assert_called_once_with(
            config=mock_config, graph=g, logger=ANY
        )

        # Verify setup and teardown routines were executed
        assert mock_asyncio_run.call_count == 2
        mock_execute_setup_routines_on_graph.assert_called_once_with(
            mock_na_graph, mock_config
        )
        # Verify sync data was called
        mock_sync_data.assert_called_once_with(g, mock_na_graph, mock_config)
        mock_execute_teardown_routines_on_graph.assert_called_once_with(
            mock_na_graph, mock_config
        )

    @patch("nx_neptune.utils.decorators.get_config")
    @patch("nx_neptune.utils.decorators.NeptuneGraph")
    @patch("nx_neptune.utils.decorators.asyncio.run")
    @patch("nx_neptune.utils.decorators.asyncio.get_running_loop")
    @patch(
        "nx_neptune.utils.decorators._execute_teardown_routines_on_graph",
        new_callable=MagicMock,
    )
    @patch(
        "nx_neptune.utils.decorators._execute_setup_new_graph",
        new_callable=MagicMock,
    )
    @patch("nx_neptune.utils.decorators._sync_data_to_neptune")
    def test_with_create_new_instance(
        self,
        mock_sync_data,
        mock_execute_setup_new_graph,
        mock_execute_teardown_routines_on_graph,
        mock_get_running_loop,
        mock_asyncio_run,
        mock_neptune_graph,
        mock_get_config,
    ):
        """Test the decorator with create_new_instance=True"""
        # Setup mock config
        mock_initial_config = MagicMock(spec=NeptuneConfig)
        mock_initial_config.graph_id = None
        mock_initial_config.create_new_instance = True
        mock_get_config.return_value = mock_initial_config
        mock_initial_config.validate_config.return_value = None

        mock_updated_config = MagicMock(spec=NeptuneConfig)
        mock_updated_config.graph_id = "new-graph-id"

        # Setup mock for event loop
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = False
        mock_get_running_loop.return_value = mock_loop

        # Setup mock for asyncio.run
        mock_asyncio_run.side_effect = [
            mock_updated_config,
            None,
        ]  # For setup and teardown

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)
        mock_neptune_graph.from_config.return_value = mock_na_graph

        # Create a test function
        def test_func(graph, arg1, arg2=None):
            return f"Function called with {graph}, {arg1}, {arg2}"

        # Apply the decorator
        decorated_func = configure_if_nx_active()(test_func)

        # Call the decorated function
        graph = nx.Graph()
        result = decorated_func(graph, "test_arg", arg2="test_kwarg")

        # Verify the function was called with the NeptuneGraph
        assert result == f"Function called with {mock_na_graph}, test_arg, test_kwarg"

        # Verify config was validated
        mock_initial_config.validate_config.assert_called_once()

        # Verify NeptuneGraph was created with updated config
        mock_neptune_graph.from_config.assert_called_once_with(
            config=mock_updated_config, graph=graph, logger=ANY
        )

        # Verify setup and teardown routines were executed
        assert mock_asyncio_run.call_count == 2
        mock_execute_setup_new_graph.assert_called_once_with(mock_initial_config, graph)
        # Verify sync data was called
        mock_sync_data.assert_called_once_with(
            graph, mock_na_graph, mock_updated_config
        )
        mock_execute_teardown_routines_on_graph.assert_called_once_with(
            mock_na_graph, mock_updated_config
        )

    @patch("nx_neptune.utils.decorators.get_config")
    @patch("nx_neptune.utils.decorators.NeptuneGraph")
    @patch("nx_neptune.utils.decorators.asyncio.get_running_loop")
    @patch("nx_neptune.utils.decorators.concurrent.futures.ThreadPoolExecutor")
    @patch(
        "nx_neptune.utils.decorators._execute_teardown_routines_on_graph",
        new_callable=MagicMock,
    )
    @patch(
        "nx_neptune.utils.decorators._execute_setup_routines_on_graph",
        new_callable=MagicMock,
    )
    @patch("nx_neptune.utils.decorators._sync_data_to_neptune")
    def test_with_running_event_loop(
        self,
        mock_sync_data,
        mock_execute_setup_routines_on_graph,
        mock_execute_teardown_routines_on_graph,
        mock_thread_pool,
        mock_get_running_loop,
        mock_neptune_graph,
        mock_get_config,
    ):
        """Test the decorator when an event loop is already running"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.graph_id = "test-graph-id"
        mock_config.create_new_instance = False
        mock_get_config.return_value = mock_config
        mock_config.validate_config.return_value = None

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)
        mock_neptune_graph.from_config.return_value = mock_na_graph
        g = nx.Graph()

        # Setup mock for event loop
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = True
        mock_get_running_loop.return_value = mock_loop

        # Setup mock for ThreadPoolExecutor
        mock_pool = MagicMock()
        mock_thread_pool.return_value = mock_pool
        mock_future = MagicMock()
        mock_future.result.return_value = mock_config
        mock_pool.submit.return_value = mock_future

        # Create a test function
        def test_func(graph, arg1, arg2=None):
            return f"Function called with {graph}, {arg1}, {arg2}"

        # Apply the decorator
        decorated_func = configure_if_nx_active()(test_func)

        # Call the decorated function
        result = decorated_func(g, "test_arg", arg2="test_kwarg")

        # Verify the function was called with the NeptuneGraph
        assert result == f"Function called with {mock_na_graph}, test_arg, test_kwarg"

        # Verify ThreadPoolExecutor was used
        mock_thread_pool.assert_called()
        mock_pool.submit.assert_called()


class TestExecuteSetupRoutinesOnGraph:
    """Tests for _execute_setup_routines_on_graph function"""

    @pytest.mark.asyncio
    async def test_with_import_s3_bucket(self):
        """Test with import_s3_bucket set"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.import_s3_bucket = "test-bucket"
        mock_config.skip_graph_reset = True
        mock_config.restore_snapshot = None

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        # Mock import_csv_from_s3
        with patch("nx_neptune.utils.decorators.import_csv_from_s3") as mock_import:
            mock_import.return_value = asyncio.Future()
            mock_import.return_value.set_result(None)

            # Call the function
            result = await _execute_setup_routines_on_graph(mock_na_graph, mock_config)

            # Verify import_csv_from_s3 was called
            mock_import.assert_called_once_with(
                mock_na_graph, "test-bucket", mock_config.skip_graph_reset
            )

            # Verify the result is the config
            assert result == mock_config

    @pytest.mark.asyncio
    async def test_with_restore_snapshot(self):
        """Test with restore_snapshot set"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.import_s3_bucket = None
        mock_config.restore_snapshot = "test-snapshot"

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        # Call the function and expect exception
        with pytest.raises(
            Exception, match="Not implemented yet \\(workflow: restore_snapshot\\)"
        ):
            await _execute_setup_routines_on_graph(mock_na_graph, mock_config)

    @pytest.mark.asyncio
    async def test_with_no_options(self):
        """Test with no import options set"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.import_s3_bucket = None
        mock_config.restore_snapshot = None

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        # Call the function
        result = await _execute_setup_routines_on_graph(mock_na_graph, mock_config)

        # Verify the result is the config
        assert result == mock_config


class TestExecuteSetupNewGraph:
    """Tests for _execute_setup_new_graph function"""

    @pytest.mark.asyncio
    async def test_with_import_s3_bucket(self):
        """Test with import_s3_bucket set"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.import_s3_bucket = "test-bucket"
        mock_config.restore_snapshot = None

        # Setup mock graph
        mock_graph = MagicMock(spec=nx.Graph)

        # Mock create_na_instance
        with patch("nx_neptune.utils.decorators.create_na_instance") as mock_create:
            mock_create.return_value = "new-graph-id"

            # Mock set_config_graph_id
            with patch(
                "nx_neptune.utils.decorators.set_config_graph_id"
            ) as mock_set_config:
                mock_updated_config = MagicMock(spec=NeptuneConfig)
                mock_updated_config.import_s3_bucket = "test-bucket"
                mock_set_config.return_value = mock_updated_config

                # Mock NeptuneGraph
                with patch(
                    "nx_neptune.utils.decorators.NeptuneGraph"
                ) as mock_neptune_graph:
                    mock_na_graph = MagicMock(spec=NeptuneGraph)
                    mock_neptune_graph.from_config.return_value = mock_na_graph

                    # Mock import_csv_from_s3
                    with patch(
                        "nx_neptune.utils.decorators.import_csv_from_s3"
                    ) as mock_import:
                        future = asyncio.Future()
                        future.set_result(None)
                        mock_import.return_value = future

                        # Call the function
                        result = await _execute_setup_new_graph(mock_config, mock_graph)

                        # Verify create_na_instance was called
                        mock_create.assert_called_once()

                        # Verify set_config_graph_id was called
                        mock_set_config.assert_called_once_with("new-graph-id")

                        # Verify NeptuneGraph.from_config was called
                        mock_neptune_graph.from_config.assert_called_once_with(
                            config=mock_updated_config, graph=mock_graph, logger=ANY
                        )

                        # Verify import_csv_from_s3 was called
                        mock_import.assert_called_once_with(
                            mock_na_graph, "test-bucket"
                        )

                        # Verify the result is the updated config
                        assert result == mock_updated_config

    @pytest.mark.asyncio
    async def test_with_restore_snapshot(self):
        """Test with restore_snapshot set"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.import_s3_bucket = None
        mock_config.restore_snapshot = "test-snapshot"

        # Setup mock graph
        mock_graph = MagicMock(spec=nx.Graph)

        # Call the function and expect exception
        with pytest.raises(
            Exception,
            match="Not implemented yet \\(workflow: create_new_instance w/ restore_snapshot\\)",
        ):
            await _execute_setup_new_graph(mock_config, mock_graph)

    @pytest.mark.asyncio
    async def test_create_empty_instance(self):
        """Test creating an empty instance"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.import_s3_bucket = None
        mock_config.restore_snapshot = None

        # Setup mock graph
        mock_graph = MagicMock(spec=nx.Graph)

        # Mock create_na_instance
        with patch("nx_neptune.utils.decorators.create_na_instance") as mock_create:
            mock_create.return_value = "new-graph-id"

            # Mock set_config_graph_id
            with patch(
                "nx_neptune.utils.decorators.set_config_graph_id"
            ) as mock_set_config:
                mock_updated_config = MagicMock(spec=NeptuneConfig)
                mock_set_config.return_value = mock_updated_config

                # Call the function
                result = await _execute_setup_new_graph(mock_config, mock_graph)

                # Verify create_na_instance was called
                mock_create.assert_called_once()

                # Verify set_config_graph_id was called
                mock_set_config.assert_called_once_with("new-graph-id")

                # Verify the result is the updated config
                assert result == mock_updated_config


class TestSyncDataToNeptune:
    """Tests for _sync_data_to_neptune function"""

    @patch("nx_neptune.utils.decorators.Node.convert_from_nx")
    @patch("nx_neptune.utils.decorators.Edge.convert_from_nx")
    def test_sync_directed_graph(self, mock_edge_convert, mock_node_convert):
        """Test syncing a directed graph"""
        # Create a directed NetworkX graph
        G = nx.DiGraph()
        G.add_node(1, attr1="value1")
        G.add_node(2, attr2="value2")
        G.add_edge(1, 2, weight=1.0)

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        # Setup mock NeptuneConfig
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.batch_update_node_size = 200000
        mock_config.batch_update_edge_size = 200000

        # Setup mock Node and Edge conversions
        mock_node1 = MagicMock()
        mock_node2 = MagicMock()
        mock_edge = MagicMock()
        mock_node_convert.side_effect = [mock_node1, mock_node2]
        mock_edge_convert.return_value = mock_edge

        # Call the function
        result = _sync_data_to_neptune(G, mock_na_graph, mock_config)

        # Verify node conversions
        assert mock_node_convert.call_count == 2

        # Verify edge conversion
        mock_edge_convert.assert_called_once()

        # Verify add_node was called for each node
        mock_na_graph.add_nodes.assert_called()

        # Verify add_edge was called once (directed graph)
        mock_na_graph.add_edges.assert_called()

        # Verify the result is the NeptuneGraph
        assert result == mock_na_graph

    @patch("nx_neptune.utils.decorators.Node.convert_from_nx")
    @patch("nx_neptune.utils.decorators.Edge.convert_from_nx")
    def test_sync_undirected_graph(self, mock_edge_convert, mock_node_convert):
        """Test syncing an undirected graph"""
        # Create an undirected NetworkX graph
        G = nx.Graph()
        G.add_node(1, attr1="value1")
        G.add_node(2, attr2="value2")
        G.add_edge(1, 2, weight=1.0)

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        # Setup mock NeptuneConfig
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.batch_update_node_size = 200000
        mock_config.batch_update_edge_size = 200000

        # Setup mock Node and Edge conversions
        mock_node1 = MagicMock()
        mock_node2 = MagicMock()
        mock_edge = MagicMock()
        mock_reverse_edge = MagicMock()
        mock_node_convert.side_effect = [mock_node1, mock_node2]
        mock_edge_convert.return_value = mock_edge
        mock_edge.to_reverse_edge.return_value = mock_reverse_edge

        # Call the function
        result = _sync_data_to_neptune(G, mock_na_graph, mock_config)

        # Verify node conversions
        assert mock_node_convert.call_count == 2

        # Verify edge conversion
        assert mock_edge_convert.call_count == 1

        # Verify add_node was called for each node
        mock_na_graph.add_nodes.assert_called()

        # Verify add_edge was called twice (undirected graph)
        mock_na_graph.add_edges.assert_called()

        # Verify the result is the NeptuneGraph
        assert result == mock_na_graph


class TestExecuteTeardownRoutinesOnGraph:
    """Tests for _execute_teardown_routines_on_graph function"""

    @pytest.mark.asyncio
    async def test_with_export_s3_bucket(self):
        """Test with export_s3_bucket set"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.graph_id = "test-graph-id"
        mock_config.export_s3_bucket = "test-bucket"
        mock_config.save_snapshot = False
        mock_config.reset_graph = False
        mock_config.destroy_instance = False
        mock_config.stop_instance = False

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        # Mock export_csv_to_s3
        with patch("nx_neptune.utils.decorators.export_csv_to_s3") as mock_export:
            future = asyncio.Future()
            future.set_result(None)
            mock_export.return_value = future

            # Call the function
            result = await _execute_teardown_routines_on_graph(
                mock_na_graph, mock_config
            )

            # Verify export_csv_to_s3 was called
            mock_export.assert_called_once_with(mock_na_graph, "test-bucket")

            # Verify the result is the config
            assert result == mock_config

    @pytest.mark.asyncio
    async def test_with_save_snapshot(self):
        """Test with save_snapshot set"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.graph_id = "test-graph-id"
        mock_config.export_s3_bucket = None
        mock_config.save_snapshot = True
        mock_config.reset_graph = False
        mock_config.destroy_instance = False
        mock_config.stop_instance = False

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        # Call the function and expect exception
        with pytest.raises(
            Exception, match="Not implemented yet \\(workflow: save_snapshot\\)"
        ):
            await _execute_teardown_routines_on_graph(mock_na_graph, mock_config)

    @pytest.mark.asyncio
    async def test_with_reset_graph(self):
        """Reset resets the graph once, keeps graph_id and does not raise NotImplemented"""
        mock_config = _teardown_config(reset_graph=True)
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        with (
            patch(
                "nx_neptune.utils.decorators.reset_graph",
                new_callable=AsyncMock,
                create=True,
            ) as mock_reset,
            patch(
                "nx_neptune.utils.decorators.stop_na_instance",
                new_callable=AsyncMock,
                create=True,
            ) as mock_stop,
            patch(
                "nx_neptune.utils.decorators.delete_na_instance", new_callable=AsyncMock
            ) as mock_delete,
            patch("nx_neptune.utils.decorators.set_config_graph_id") as mock_set_config,
        ):
            result = await _execute_teardown_routines_on_graph(
                mock_na_graph, mock_config
            )

        mock_reset.assert_awaited_once()
        assert mock_reset.await_args.args[0] == "test-graph-id"
        mock_stop.assert_not_called()
        mock_delete.assert_not_called()
        mock_set_config.assert_not_called()
        assert result == mock_config
        assert result.graph_id == "test-graph-id"

    @pytest.mark.asyncio
    async def test_with_stop_instance(self):
        """Stop stops the instance once with graph_id and keeps graph_id"""
        mock_config = _teardown_config(stop_instance=True)
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        with (
            patch(
                "nx_neptune.utils.decorators.stop_na_instance",
                new_callable=AsyncMock,
                create=True,
            ) as mock_stop,
            patch(
                "nx_neptune.utils.decorators.reset_graph",
                new_callable=AsyncMock,
                create=True,
            ) as mock_reset,
            patch(
                "nx_neptune.utils.decorators.delete_na_instance", new_callable=AsyncMock
            ) as mock_delete,
            patch("nx_neptune.utils.decorators.set_config_graph_id") as mock_set_config,
        ):
            result = await _execute_teardown_routines_on_graph(
                mock_na_graph, mock_config
            )

        mock_stop.assert_awaited_once()
        assert mock_stop.await_args.args[0] == "test-graph-id"
        mock_reset.assert_not_called()
        mock_delete.assert_not_called()
        mock_set_config.assert_not_called()
        assert result == mock_config
        assert result.graph_id == "test-graph-id"

    @pytest.mark.asyncio
    async def test_destroy_does_not_stop_or_reset(self):
        """Destroy (legacy) only deletes, and clears the graph_id"""
        mock_config = _teardown_config(destroy_instance=True)
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        with (
            patch(
                "nx_neptune.utils.decorators.stop_na_instance",
                new_callable=AsyncMock,
                create=True,
            ) as mock_stop,
            patch(
                "nx_neptune.utils.decorators.reset_graph",
                new_callable=AsyncMock,
                create=True,
            ) as mock_reset,
            patch(
                "nx_neptune.utils.decorators.delete_na_instance", new_callable=AsyncMock
            ) as mock_delete,
            patch("nx_neptune.utils.decorators.set_config_graph_id") as mock_set_config,
        ):
            await _execute_teardown_routines_on_graph(mock_na_graph, mock_config)

        mock_delete.assert_awaited_once_with("test-graph-id")
        mock_set_config.assert_called_once_with(None)
        mock_stop.assert_not_called()
        mock_reset.assert_not_called()

    @pytest.mark.asyncio
    async def test_default_makes_no_lifecycle_call(self):
        """With no completion action nothing is stopped, reset or deleted and graph_id is unchanged"""
        mock_config = _teardown_config()
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        with (
            patch(
                "nx_neptune.utils.decorators.stop_na_instance",
                new_callable=AsyncMock,
                create=True,
            ) as mock_stop,
            patch(
                "nx_neptune.utils.decorators.reset_graph",
                new_callable=AsyncMock,
                create=True,
            ) as mock_reset,
            patch(
                "nx_neptune.utils.decorators.delete_na_instance", new_callable=AsyncMock
            ) as mock_delete,
            patch("nx_neptune.utils.decorators.set_config_graph_id") as mock_set_config,
        ):
            result = await _execute_teardown_routines_on_graph(
                mock_na_graph, mock_config
            )

        mock_stop.assert_not_called()
        mock_reset.assert_not_called()
        mock_delete.assert_not_called()
        mock_set_config.assert_not_called()
        assert result.graph_id == "test-graph-id"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "action", ["destroy_instance", "stop_instance", "reset_graph"]
    )
    async def test_export_finishes_before_completion_action(self, action):
        """The S3 export has completed before destroy, stop or reset is invoked"""
        mock_config = _teardown_config(export_s3_bucket="test-bucket", **{action: True})
        mock_na_graph = MagicMock(spec=NeptuneGraph)
        events = []

        async def fake_export(*a, **k):
            events.append("export_start")
            await asyncio.sleep(0)
            events.append("export_end")

        def action_recorder(name):
            async def _record(*a, **k):
                events.append(name)

            return _record

        with (
            patch(
                "nx_neptune.utils.decorators.export_csv_to_s3", side_effect=fake_export
            ),
            patch(
                "nx_neptune.utils.decorators.stop_na_instance",
                side_effect=action_recorder("stop_instance"),
                create=True,
            ),
            patch(
                "nx_neptune.utils.decorators.reset_graph",
                side_effect=action_recorder("reset_graph"),
                create=True,
            ),
            patch(
                "nx_neptune.utils.decorators.delete_na_instance",
                side_effect=action_recorder("destroy_instance"),
            ),
            patch("nx_neptune.utils.decorators.set_config_graph_id"),
        ):
            await _execute_teardown_routines_on_graph(mock_na_graph, mock_config)

        assert events == ["export_start", "export_end", action]

    @pytest.mark.asyncio
    async def test_stop_failure_propagates_without_deleting(self):
        """If stop fails its error propagates and the instance is not deleted"""
        mock_config = _teardown_config(stop_instance=True)
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        with (
            patch(
                "nx_neptune.utils.decorators.stop_na_instance",
                new_callable=AsyncMock,
                side_effect=RuntimeError("stop failed"),
                create=True,
            ),
            patch(
                "nx_neptune.utils.decorators.delete_na_instance", new_callable=AsyncMock
            ) as mock_delete,
        ):
            with pytest.raises(RuntimeError, match="stop failed"):
                await _execute_teardown_routines_on_graph(mock_na_graph, mock_config)

        mock_delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_with_destroy_instance(self):
        """Test with destroy_instance set"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.graph_id = "test-graph-id"
        mock_config.export_s3_bucket = None
        mock_config.save_snapshot = False
        mock_config.reset_graph = False
        mock_config.destroy_instance = True
        mock_config.stop_instance = False

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        # Mock delete_na_instance and set_config_graph_id
        with patch("nx_neptune.utils.decorators.delete_na_instance") as mock_delete:
            with patch(
                "nx_neptune.utils.decorators.set_config_graph_id"
            ) as mock_set_config:
                mock_updated_config = MagicMock(spec=NeptuneConfig)
                mock_set_config.return_value = mock_updated_config

                future = asyncio.Future()
                future.set_result(None)
                mock_delete.return_value = future

                # Call the function
                result = await _execute_teardown_routines_on_graph(
                    mock_na_graph, mock_config
                )

                # Verify delete_na_instance was called
                mock_delete.assert_called_once_with("test-graph-id")

                # Verify set_config_graph_id was called
                mock_set_config.assert_called_once_with(None)

                # Verify the result is the updated config
                assert result == mock_updated_config

    @pytest.mark.asyncio
    async def test_with_no_graph_id(self):
        """Test with no graph_id set"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.graph_id = None
        mock_config.export_s3_bucket = "test-bucket"
        mock_config.save_snapshot = True
        mock_config.reset_graph = True
        mock_config.destroy_instance = True

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        # Call the function
        result = await _execute_teardown_routines_on_graph(mock_na_graph, mock_config)

        # Verify the result is the config
        assert result == mock_config

    @pytest.mark.asyncio
    async def test_with_no_options(self):
        """Test with no teardown options set"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.graph_id = "test-graph-id"
        mock_config.export_s3_bucket = None
        mock_config.save_snapshot = False
        mock_config.reset_graph = False
        mock_config.destroy_instance = False
        mock_config.stop_instance = False

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        # Call the function
        result = await _execute_teardown_routines_on_graph(mock_na_graph, mock_config)

        # Verify the result is the config
        assert result == mock_config


class TestCompletionActionRunsOnAlgorithmOutcome:
    """The completion action runs in a finally: on success and on algorithm failure.

    These drive the real decorator and the real teardown coroutine; only the
    AWS-facing calls are mocked.
    """

    def _run(self, config, func, *, stop=None, reset=None, delete=None, export=None):
        """Invoke a decorated `func` with the given config and lifecycle fakes."""
        stop = stop or AsyncMock()
        reset = reset or AsyncMock()
        delete = delete or AsyncMock()
        export = export or AsyncMock()
        setup = AsyncMock(return_value=config)
        with (
            patch("nx_neptune.utils.decorators.get_config", return_value=config),
            patch("nx_neptune.utils.decorators.NeptuneGraph"),
            patch(
                "nx_neptune.utils.decorators._execute_setup_routines_on_graph", setup
            ),
            patch("nx_neptune.utils.decorators._sync_data_to_neptune"),
            patch("nx_neptune.utils.decorators.stop_na_instance", stop, create=True),
            patch("nx_neptune.utils.decorators.reset_graph", reset, create=True),
            patch("nx_neptune.utils.decorators.delete_na_instance", delete),
            patch("nx_neptune.utils.decorators.export_csv_to_s3", export),
            patch("nx_neptune.utils.decorators.set_config_graph_id"),
        ):
            decorated = configure_if_nx_active()(func)
            return decorated(nx.Graph())

    @pytest.fixture(autouse=True)
    def _no_test_env(self, monkeypatch):
        monkeypatch.delenv("NX_ALGORITHM_TEST", raising=False)

    @staticmethod
    def _config(**flags):
        cfg = _teardown_config(**flags)
        cfg.create_new_instance = False
        cfg.validate_config.return_value = None
        return cfg

    @pytest.mark.parametrize(
        "action", ["destroy_instance", "stop_instance", "reset_graph"]
    )
    def test_action_runs_when_algorithm_raises_and_original_exception_propagates(
        self, action
    ):
        original = ValueError("algorithm blew up")
        mocks = {
            "destroy_instance": AsyncMock(),
            "stop_instance": AsyncMock(),
            "reset_graph": AsyncMock(),
        }

        def failing(graph):
            raise original

        with pytest.raises(ValueError) as excinfo:
            self._run(
                self._config(**{action: True}),
                failing,
                delete=mocks["destroy_instance"],
                stop=mocks["stop_instance"],
                reset=mocks["reset_graph"],
            )

        assert excinfo.value is original
        mocks[action].assert_awaited_once()

    def test_action_runs_on_success_and_result_is_returned(self):
        stop = AsyncMock()
        result = self._run(self._config(stop_instance=True), lambda g: "ok", stop=stop)
        assert result == "ok"
        stop.assert_awaited_once()

    def test_no_action_selected_makes_no_lifecycle_call_even_on_failure(self):
        stop, reset, delete = AsyncMock(), AsyncMock(), AsyncMock()

        def failing(graph):
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            self._run(self._config(), failing, stop=stop, reset=reset, delete=delete)

        stop.assert_not_called()
        reset.assert_not_called()
        delete.assert_not_called()

    def test_algorithm_and_teardown_fail_algorithm_error_wins_and_teardown_is_logged(
        self, caplog
    ):
        original = ValueError("algorithm blew up")
        stop = AsyncMock(side_effect=RuntimeError("stop exploded"))

        def failing(graph):
            raise original

        with caplog.at_level("WARNING", logger="nx_neptune.utils.decorators"):
            with pytest.raises(ValueError) as excinfo:
                self._run(self._config(stop_instance=True), failing, stop=stop)

        assert excinfo.value is original
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any(
            "stop exploded" in r.getMessage() and "test-graph-id" in r.getMessage()
            for r in warnings
        ), [r.getMessage() for r in warnings]

    def test_algorithm_succeeds_and_teardown_fails_teardown_error_propagates(self):
        stop = AsyncMock(side_effect=RuntimeError("stop exploded"))
        with pytest.raises(RuntimeError, match="stop exploded"):
            self._run(self._config(stop_instance=True), lambda g: "ok", stop=stop)

    def test_export_runs_before_completion_action_on_algorithm_failure(self):
        events = []

        async def export(*a, **k):
            events.append("export")

        async def stop(*a, **k):
            events.append("stop")

        def failing(graph):
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            self._run(
                self._config(stop_instance=True, export_s3_bucket="test-bucket"),
                failing,
                stop=stop,
                export=export,
            )

        assert events == ["export", "stop"]

    @pytest.mark.parametrize("exc_type", [KeyboardInterrupt, SystemExit])
    def test_action_runs_when_algorithm_raises_base_exception(self, exc_type):
        """KeyboardInterrupt / SystemExit from the algorithm still trigger the
        completion action and propagate unchanged."""
        original = exc_type()
        stop = AsyncMock()

        def interrupted(graph):
            raise original

        with pytest.raises(exc_type) as excinfo:
            self._run(self._config(stop_instance=True), interrupted, stop=stop)

        assert excinfo.value is original
        stop.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("teardown_fails", [False, True])
    async def test_running_loop_teardown_via_thread_pool_and_original_exception_wins(
        self, teardown_fails, caplog
    ):
        """With an event loop already running, teardown is submitted through a
        ThreadPoolExecutor after an algorithm failure; the algorithm's exception
        wins even if the teardown fails too (the failure is logged)."""
        import concurrent.futures

        original = ValueError("algorithm blew up")
        stop = AsyncMock(
            side_effect=RuntimeError("stop exploded") if teardown_fails else None
        )
        submitted = []
        real_pool_cls = concurrent.futures.ThreadPoolExecutor

        def spying_pool(*a, **k):
            pool = real_pool_cls(*a, **k)
            real_submit = pool.submit

            def submit(fn, *args, **kwargs):
                submitted.append(args)
                return real_submit(fn, *args, **kwargs)

            pool.submit = submit
            return pool

        def failing(graph):
            raise original

        with patch(
            "nx_neptune.utils.decorators.concurrent.futures.ThreadPoolExecutor",
            side_effect=spying_pool,
        ), caplog.at_level("WARNING", logger="nx_neptune.utils.decorators"):
            with pytest.raises(ValueError) as excinfo:
                self._run(self._config(stop_instance=True), failing, stop=stop)

        assert excinfo.value is original
        stop.assert_awaited_once()
        # setup and teardown both went through the pool (2 submissions)
        assert len(submitted) == 2
        if teardown_fails:
            assert any(
                "stop exploded" in r.getMessage() for r in caplog.records
            ), [r.getMessage() for r in caplog.records]
