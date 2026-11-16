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
import logging
import os
from contextlib import contextmanager
import asyncio
from asyncio import Future
from unittest.mock import MagicMock, patch, call, ANY

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
    @patch("nx_neptune.utils.decorators._execute_teardown_routines_on_graph")
    @patch("nx_neptune.utils.decorators._execute_setup_routines_on_graph")
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
    @patch("nx_neptune.utils.decorators._execute_teardown_routines_on_graph")
    @patch("nx_neptune.utils.decorators._execute_setup_new_graph")
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
    @patch("nx_neptune.utils.decorators._execute_setup_routines_on_graph")
    @patch("nx_neptune.utils.decorators._sync_data_to_neptune")
    def test_with_running_event_loop(
        self,
        mock_sync_data,
        mock_execute_setup_routines_on_graph,
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

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        # Call the function and expect exception
        with pytest.raises(
            Exception, match="Not implemented yet \\(workflow: save_snapshot\\)"
        ):
            await _execute_teardown_routines_on_graph(mock_na_graph, mock_config)

    @pytest.mark.asyncio
    async def test_with_reset_graph(self):
        """Test with reset_graph set"""
        # Setup mock config
        mock_config = MagicMock(spec=NeptuneConfig)
        mock_config.graph_id = "test-graph-id"
        mock_config.export_s3_bucket = None
        mock_config.save_snapshot = False
        mock_config.reset_graph = True
        mock_config.destroy_instance = False

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        # Call the function and expect exception
        with pytest.raises(
            Exception, match="Not implemented yet \\(workflow: reset_graph\\)"
        ):
            await _execute_teardown_routines_on_graph(mock_na_graph, mock_config)

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

        # Setup mock NeptuneGraph
        mock_na_graph = MagicMock(spec=NeptuneGraph)

        # Call the function
        result = await _execute_teardown_routines_on_graph(mock_na_graph, mock_config)

        # Verify the result is the config
        assert result == mock_config


class TestTeardownAfterAFailedAlgorithmCall:
    """A failing algorithm call must not leave a created instance running.

    The teardown used to run only after a *successful* call, so a call that
    raised skipped it and left the instance AVAILABLE and billing. It now runs
    on failure too, but narrowed so a failure cannot destroy data.
    """

    @staticmethod
    def _config(destroy_instance=True, create_new_instance=True, graph_id=None):
        config = MagicMock(spec=NeptuneConfig)
        config.graph_id = graph_id
        config.create_new_instance = create_new_instance
        config.destroy_instance = destroy_instance
        config.export_s3_bucket = None
        config.import_s3_bucket = None
        config.save_snapshot = False
        config.reset_graph = False
        config.validate_config.return_value = None
        return config

    @contextmanager
    def _patched(self, config, created_graph_id="g-created"):
        """Patch the decorator's AWS surface, yielding its delete/export doubles."""
        with (
            patch("nx_neptune.utils.decorators.get_config", return_value=config),
            patch("nx_neptune.utils.decorators.NeptuneGraph") as na_graph_cls,
            patch("nx_neptune.utils.decorators._sync_data_to_neptune"),
            patch("nx_neptune.utils.decorators._execute_setup_new_graph") as setup_new,
            patch(
                "nx_neptune.utils.decorators._execute_setup_routines_on_graph"
            ) as setup_existing,
            patch("nx_neptune.utils.decorators.delete_na_instance") as delete,
            patch("nx_neptune.utils.decorators.export_csv_to_s3") as export,
            patch("nx_neptune.utils.decorators.set_config_graph_id"),
        ):
            na_graph_cls.from_config.return_value = MagicMock(spec=NeptuneGraph)

            async def _setup_new(cfg, graph):
                cfg.graph_id = created_graph_id
                return cfg

            async def _setup_existing(na_graph, cfg, *a, **k):
                return cfg

            async def _noop(*a, **k):
                return None

            setup_new.side_effect = _setup_new
            setup_existing.side_effect = _setup_existing
            delete.side_effect = _noop
            export.side_effect = _noop
            yield delete, export

    @staticmethod
    def _failing(graph):
        raise ValueError("algorithm boom")

    def test_a_created_instance_is_destroyed_when_the_algorithm_raises(self):
        """The cost leak this fixes: the instance used to be left running."""
        with self._patched(self._config()) as (delete, _):
            with pytest.raises(ValueError, match="algorithm boom"):
                configure_if_nx_active()(self._failing)(nx.Graph())

        delete.assert_called_once_with("g-created")

    def test_the_algorithm_error_propagates_unchanged(self):
        """The caller must see their own error, not a teardown error."""
        original = ValueError("algorithm boom")

        def failing(graph):
            raise original

        with self._patched(self._config()):
            with pytest.raises(ValueError) as excinfo:
                configure_if_nx_active()(failing)(nx.Graph())

        assert excinfo.value is original

    def test_a_caller_supplied_graph_is_not_destroyed(self):
        """destroy_instance is global config; it must not delete a graph this
        call did not create just because an algorithm raised."""
        config = self._config(create_new_instance=False, graph_id="g-caller-supplied")

        with self._patched(config) as (delete, _):
            with pytest.raises(ValueError, match="algorithm boom"):
                configure_if_nx_active()(self._failing)(nx.Graph())

        delete.assert_not_called()

    def test_the_export_is_skipped_on_failure(self):
        """A partial export must not overwrite the last good one."""
        config = self._config()
        config.export_s3_bucket = "s3://bucket/exports"

        with self._patched(config) as (_, export):
            with pytest.raises(ValueError, match="algorithm boom"):
                configure_if_nx_active()(self._failing)(nx.Graph())

        export.assert_not_called()

    @pytest.mark.parametrize(
        "interrupt", [KeyboardInterrupt, lambda: SystemExit(3)], ids=["ctrl-c", "exit"]
    )
    def test_an_interrupt_runs_no_teardown(self, interrupt):
        """Ctrl-C must not be turned into a delete."""

        def interrupted(graph):
            raise interrupt()

        with self._patched(self._config()) as (delete, _):
            with pytest.raises(type(interrupt())):
                configure_if_nx_active()(interrupted)(nx.Graph())

        delete.assert_not_called()

    def test_a_failing_teardown_does_not_replace_the_algorithm_error(self, caplog):
        """The teardown error is logged, the algorithm's error still wins."""
        with self._patched(self._config()) as (delete, _):

            async def _boom(graph_id):
                raise RuntimeError("delete failed")

            delete.side_effect = _boom

            with caplog.at_level(logging.ERROR, logger="nx_neptune.utils.decorators"):
                with pytest.raises(ValueError, match="algorithm boom"):
                    configure_if_nx_active()(self._failing)(nx.Graph())

        assert any(
            "may still be running and billed" in r.getMessage()
            for r in caplog.records
            if r.levelno == logging.ERROR
        )

    def test_a_successful_call_still_runs_the_normal_teardown(self):
        """Regression guard: the success path is unchanged."""

        def ok(graph):
            return "result"

        with self._patched(self._config()) as (delete, _):
            assert configure_if_nx_active()(ok)(nx.Graph()) == "result"

        delete.assert_called_once_with("g-created")
