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
        # `on_created` is checked by identity-agnostic means: it is a closure
        # over the wrapper's local, so only that it was supplied matters here.
        mock_execute_setup_new_graph.assert_called_once()
        call_args, call_kwargs = mock_execute_setup_new_graph.call_args
        assert call_args == (mock_initial_config, graph)
        assert callable(call_kwargs["on_created"])
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

    @pytest.fixture(autouse=True)
    def _no_algorithm_test_env(self, monkeypatch):
        """The decorator is a pass-through when this variable is set."""
        monkeypatch.delenv("NX_ALGORITHM_TEST", raising=False)

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
        """Patch the decorator's AWS surface.

        Yields the delete, export and graph-id-reset doubles.
        """
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
            patch("nx_neptune.utils.decorators.set_config_graph_id") as set_graph_id,
        ):
            na_graph_cls.from_config.return_value = MagicMock(spec=NeptuneGraph)

            async def _setup_new(cfg, graph, *a, on_created=None, **k):
                # The real setup reports the id the moment the graph exists.
                # A stub that skips this hides the teardown's only safe source
                # for it, which is the whole point of the callback.
                cfg.graph_id = created_graph_id
                if on_created:
                    on_created(created_graph_id)
                return cfg

            async def _setup_existing(na_graph, cfg, *a, **k):
                return cfg

            async def _noop(*a, **k):
                return None

            setup_new.side_effect = _setup_new
            setup_existing.side_effect = _setup_existing
            delete.side_effect = _noop
            export.side_effect = _noop
            yield delete, export, set_graph_id

    @staticmethod
    def _failing(graph):
        raise ValueError("algorithm boom")

    def test_a_created_instance_is_destroyed_when_the_algorithm_raises(self):
        """The cost leak this fixes: the instance used to be left running."""
        with self._patched(self._config()) as (delete, _, set_graph_id):
            with pytest.raises(ValueError, match="algorithm boom"):
                configure_if_nx_active()(self._failing)(nx.Graph())

        delete.assert_called_once_with("g-created")
        set_graph_id.assert_called_once_with(None)

    def test_destroy_instance_false_keeps_a_created_instance(self):
        """`destroy_instance=False` is an explicit "keep it" and must be honoured.

        Pins the guard: narrowed to `if not created_here`, a failed call would
        delete an instance the user configured to keep.
        """
        with self._patched(self._config(destroy_instance=False)) as (
            delete,
            _,
            set_graph_id,
        ):
            with pytest.raises(ValueError, match="algorithm boom"):
                configure_if_nx_active()(self._failing)(nx.Graph())

        delete.assert_not_called()
        set_graph_id.assert_not_called()

    def test_the_algorithm_error_propagates_unchanged(self):
        """The caller must see their own error, not a teardown error."""
        original = ValueError("algorithm boom")

        def failing(graph):
            raise original

        with self._patched(self._config()):
            with pytest.raises(ValueError) as excinfo:
                configure_if_nx_active()(failing)(nx.Graph())

        assert excinfo.value is original

    @pytest.mark.parametrize(
        "create_new_instance",
        [False, {"provisionedMemory": 32}],
        ids=["no-new-instance", "instance-options-dict"],
    )
    def test_a_caller_supplied_graph_is_not_destroyed(self, create_new_instance):
        """destroy_instance is global config; it must not delete a graph this
        call did not create just because an algorithm raised.

        A dict create_new_instance (custom instance options) is accepted
        alongside a graph_id, and the wrapper then uses the graph_id, so
        ownership must come from the branch taken, not from the config.
        """
        config = self._config(
            create_new_instance=create_new_instance, graph_id="g-caller-supplied"
        )

        with self._patched(config) as (delete, _, set_graph_id):
            with pytest.raises(ValueError, match="algorithm boom"):
                configure_if_nx_active()(self._failing)(nx.Graph())

        delete.assert_not_called()
        set_graph_id.assert_not_called()

    def test_config_accepts_a_graph_id_with_an_instance_options_dict(self):
        """Pins the premise of the case above against the real validation."""
        config = NeptuneConfig(
            graph_id="g-caller-supplied",
            create_new_instance={"provisionedMemory": 32},
            destroy_instance=True,
        )

        config.validate_config()

    def test_the_export_is_skipped_on_failure(self):
        """A partial export must not overwrite the last good one."""
        config = self._config()
        config.export_s3_bucket = "s3://bucket/exports"

        with self._patched(config) as (_, export, _set_graph_id):
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

        with self._patched(self._config()) as (delete, _, _set_graph_id):
            with pytest.raises(type(interrupt())):
                configure_if_nx_active()(interrupted)(nx.Graph())

        delete.assert_not_called()

    def test_a_failing_teardown_does_not_replace_the_algorithm_error(self, caplog):
        """The teardown error is logged, the algorithm's error still wins."""
        with self._patched(self._config()) as (delete, _, set_graph_id):

            async def _boom(graph_id):
                raise RuntimeError("delete failed")

            delete.side_effect = _boom

            with caplog.at_level(logging.ERROR, logger="nx_neptune.utils.decorators"):
                with pytest.raises(ValueError, match="algorithm boom"):
                    configure_if_nx_active()(self._failing)(nx.Graph())

        errors = [
            r
            for r in caplog.records
            if r.levelno == logging.ERROR
            and "may still be running and billed" in r.getMessage()
        ]
        assert errors
        assert errors[0].exc_info is not None
        assert isinstance(errors[0].exc_info[1], RuntimeError)
        set_graph_id.assert_not_called()

    def test_a_successful_call_still_runs_the_normal_teardown(self):
        """Regression guard: the success path is unchanged."""

        def ok(graph):
            return "result"

        config = self._config()
        config.export_s3_bucket = "s3://bucket/exports"
        order = []

        with self._patched(config) as (delete, export, _set_graph_id):

            async def _export(*a, **k):
                order.append("export")

            async def _delete(*a, **k):
                order.append("delete")

            export.side_effect = _export
            delete.side_effect = _delete
            assert configure_if_nx_active()(ok)(nx.Graph()) == "result"

        export.assert_called_once()
        delete.assert_called_once_with("g-created")
        assert order == ["export", "delete"]

    def test_a_created_instance_is_destroyed_from_inside_a_running_loop(self, caplog):
        """The Jupyter case: teardown must still run when a loop is running."""
        running_loop = MagicMock()
        running_loop.is_running.return_value = True

        with self._patched(self._config()) as (delete, _, set_graph_id):
            with patch(
                "nx_neptune.utils.decorators.asyncio.get_running_loop",
                return_value=running_loop,
            ):
                with caplog.at_level(
                    logging.ERROR, logger="nx_neptune.utils.decorators"
                ):
                    with pytest.raises(ValueError, match="algorithm boom"):
                        configure_if_nx_active()(self._failing)(nx.Graph())

        delete.assert_called_once_with("g-created")
        set_graph_id.assert_called_once_with(None)
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


class TestTeardownAfterASetupOrSyncFailure:
    """A failure before the algorithm runs must not leak a created instance
    either, and must still never delete a caller-supplied graph."""

    @pytest.fixture(autouse=True)
    def _no_algorithm_test_env(self, monkeypatch):
        monkeypatch.delenv("NX_ALGORITHM_TEST", raising=False)

    @staticmethod
    def _config(
        create_new_instance=True,
        graph_id=None,
        import_s3_bucket=None,
        destroy_instance=True,
        save_snapshot=False,
    ):
        config = MagicMock(spec=NeptuneConfig)
        config.graph_id = graph_id
        config.create_new_instance = create_new_instance
        config.destroy_instance = destroy_instance
        config.export_s3_bucket = None
        config.import_s3_bucket = import_s3_bucket
        config.restore_snapshot = None
        config.skip_graph_reset = True
        config.save_snapshot = save_snapshot
        config.reset_graph = False
        config.validate_config.return_value = None
        return config

    @contextmanager
    def _patched(self, config, sync=None, create=None, import_csv=None):
        """Patch the AWS surface, leaving the real setup routines in place."""

        async def _create(options=None, *, on_created=None, **kwargs):
            # The real create_na_instance reports the id as soon as creation is
            # accepted, before waiting for the graph to become available. A stub
            # that does not is a stub in which no graph can ever leak.
            if on_created:
                on_created("g-created")
            return "g-created"

        async def _delete(graph_id):
            return None

        def _set_graph_id(graph_id):
            config.graph_id = graph_id
            return config

        with (
            patch("nx_neptune.utils.decorators.get_config", return_value=config),
            patch("nx_neptune.utils.decorators.NeptuneGraph") as na_graph_cls,
            patch(
                "nx_neptune.utils.decorators._sync_data_to_neptune", side_effect=sync
            ),
            patch(
                "nx_neptune.utils.decorators.create_na_instance",
                side_effect=create or _create,
            ),
            patch(
                "nx_neptune.utils.decorators.import_csv_from_s3",
                side_effect=import_csv,
            ),
            patch(
                "nx_neptune.utils.decorators.delete_na_instance", side_effect=_delete
            ) as delete,
            patch(
                "nx_neptune.utils.decorators.set_config_graph_id",
                side_effect=_set_graph_id,
            ) as set_graph_id,
        ):
            na_graph_cls.from_config.return_value = MagicMock(spec=NeptuneGraph)
            yield delete, set_graph_id

    @staticmethod
    def _algorithm(graph):
        return "result"

    def test_a_created_instance_is_destroyed_when_the_import_raises(self):
        async def _import(*a, **k):
            raise RuntimeError("import boom")

        config = self._config(import_s3_bucket="s3://bucket/imports")
        with self._patched(config, import_csv=_import) as (delete, _):
            with pytest.raises(RuntimeError, match="import boom"):
                configure_if_nx_active()(self._algorithm)(nx.Graph())

        delete.assert_called_once_with("g-created")

    def test_a_created_instance_is_destroyed_when_the_sync_raises(self):
        config = self._config()
        with self._patched(config, sync=RuntimeError("sync boom")) as (delete, _):
            with pytest.raises(RuntimeError, match="sync boom"):
                configure_if_nx_active()(self._algorithm)(nx.Graph())

        delete.assert_called_once_with("g-created")

    @pytest.mark.parametrize("failure", ["sync", "import"], ids=["sync", "import"])
    def test_destroy_instance_false_keeps_a_created_instance(self, failure):
        """Same guard, reached through a setup/sync failure rather than the call."""

        async def _import(*a, **k):
            raise RuntimeError("import boom")

        if failure == "sync":
            config = self._config(destroy_instance=False)
            kwargs = {"sync": RuntimeError("sync boom")}
            match = "sync boom"
        else:
            config = self._config(
                destroy_instance=False, import_s3_bucket="s3://bucket/imports"
            )
            kwargs = {"import_csv": _import}
            match = "import boom"

        with self._patched(config, **kwargs) as (delete, set_graph_id):
            with pytest.raises(RuntimeError, match=match):
                configure_if_nx_active()(self._algorithm)(nx.Graph())

        delete.assert_not_called()
        # Setup legitimately records the new id here; what must not happen is
        # the teardown clearing it, which is the call with None.
        assert call(None) not in set_graph_id.call_args_list

    def test_the_graph_this_call_created_is_deleted_not_the_configs_current_one(self):
        """Ownership is an id, not a flag plus a read of global state.

        `get_config()` returns the one process-global
        `networkx.config.backends.neptune` object, which `set_config_graph_id`
        mutates. Sourcing the deletion target from it meant a failing call
        deleted whatever graph the global named at that moment — another
        concurrent call's, if one had just created it — and leaked its own.
        """
        config = self._config(import_s3_bucket="s3://bucket/imports")

        async def _import(*a, **k):
            # Stands in for another call finishing its own setup against the
            # shared global between this call's creation and its failure.
            config.graph_id = "g-another-calls-graph"
            raise RuntimeError("import boom")

        with self._patched(config, import_csv=_import) as (delete, _):
            with pytest.raises(RuntimeError, match="import boom"):
                configure_if_nx_active()(self._algorithm)(nx.Graph())

        delete.assert_called_once_with("g-created")
        assert call("g-another-calls-graph") not in delete.call_args_list

    def test_a_graph_created_before_a_failed_availability_wait_is_deleted(self):
        """The id is reported when creation is *accepted*, not when it returns.

        `create_na_instance` waits for the graph to become AVAILABLE after
        creating it. If that wait raises, the graph exists and bills, and a
        caller that only captured the return value never learns its id.
        """

        async def _create(options=None, *, on_created=None, **kwargs):
            if on_created:
                on_created("g-accepted")
            raise RuntimeError("timed out waiting for AVAILABLE")

        config = self._config()
        with self._patched(config, create=_create) as (delete, _):
            with pytest.raises(RuntimeError, match="timed out"):
                configure_if_nx_active()(self._algorithm)(nx.Graph())

        delete.assert_called_once_with("g-accepted")

    def test_a_failed_success_teardown_still_releases_the_created_graph(self):
        """The success teardown is inside the `try`, not an `else`.

        `save_snapshot` raises "Not implemented yet" before the destroy step is
        reached. From an `else` that exception escaped the failure handler, so
        the instance this call created was left running and billed — the exact
        leak this path exists to close.
        """
        config = self._config(save_snapshot="snap-1")

        with self._patched(config) as (delete, _):
            with pytest.raises(Exception, match="Not implemented yet"):
                configure_if_nx_active()(self._algorithm)(nx.Graph())

        delete.assert_called_once_with("g-created")

    def test_an_interrupt_names_the_graph_it_leaves_running(self, caplog):
        """No teardown on Ctrl-C, but the id must not vanish with the session."""
        import logging

        async def _create(options=None, *, on_created=None, **kwargs):
            if on_created:
                on_created("g-orphaned")
            raise KeyboardInterrupt

        config = self._config()
        with self._patched(config, create=_create) as (delete, _):
            with caplog.at_level(logging.WARNING):
                with pytest.raises(KeyboardInterrupt):
                    configure_if_nx_active()(self._algorithm)(nx.Graph())

        delete.assert_not_called()
        assert "g-orphaned" in caplog.text

    def test_a_failed_instance_creation_attempts_no_delete(self):
        async def _create(options=None, *, on_created=None, **kwargs):
            # Fails before creation is accepted, so no id is ever reported and
            # there is nothing to delete.
            raise RuntimeError("create boom")

        config = self._config()
        with self._patched(config, create=_create) as (delete, _):
            with pytest.raises(RuntimeError, match="create boom"):
                configure_if_nx_active()(self._algorithm)(nx.Graph())

        delete.assert_not_called()

    @pytest.mark.parametrize(
        "create_new_instance",
        [False, {"provisionedMemory": 32}],
        ids=["no-new-instance", "instance-options-dict"],
    )
    def test_a_caller_supplied_graph_is_not_destroyed_when_the_sync_raises(
        self, create_new_instance
    ):
        config = self._config(
            create_new_instance=create_new_instance, graph_id="g-caller-supplied"
        )
        with self._patched(config, sync=RuntimeError("sync boom")) as (delete, _):
            with pytest.raises(RuntimeError, match="sync boom"):
                configure_if_nx_active()(self._algorithm)(nx.Graph())

        delete.assert_not_called()

    def test_a_caller_supplied_graph_is_not_destroyed_when_the_import_raises(self):
        async def _import(*a, **k):
            raise RuntimeError("import boom")

        config = self._config(
            create_new_instance=False,
            graph_id="g-caller-supplied",
            import_s3_bucket="s3://bucket/imports",
        )
        with self._patched(config, import_csv=_import) as (delete, _):
            with pytest.raises(RuntimeError, match="import boom"):
                configure_if_nx_active()(self._algorithm)(nx.Graph())

        delete.assert_not_called()


class TestTeardownInARunningEventLoop:
    """The Jupyter case: the teardown reaches AWS through a worker thread.

    `_run_coroutine` cannot use `asyncio.run` inside a running loop, so it
    bridges to a thread. A regression in that bridge is caught and logged at the
    failure-teardown call site rather than raised, so without a test the symptom
    would be one log line and an instance left running and billed.
    """

    @pytest.fixture(autouse=True)
    def _no_algorithm_test_env(self, monkeypatch):
        monkeypatch.delenv("NX_ALGORITHM_TEST", raising=False)

    @staticmethod
    def _config(destroy_instance=True):
        config = MagicMock(spec=NeptuneConfig)
        config.graph_id = None
        config.create_new_instance = True
        config.destroy_instance = destroy_instance
        config.export_s3_bucket = None
        config.import_s3_bucket = None
        config.save_snapshot = False
        config.reset_graph = False
        config.validate_config.return_value = None
        return config

    @contextmanager
    def _patched_in_loop(self, config):
        """As the failure-path patches, but with a *running* loop reported."""

        async def _setup_new(cfg, graph, *a, on_created=None, **k):
            cfg.graph_id = "g-created"
            if on_created:
                on_created("g-created")
            return cfg

        async def _noop(*a, **k):
            return None

        running = MagicMock()
        running.is_running.return_value = True

        with (
            patch("nx_neptune.utils.decorators.get_config", return_value=config),
            patch(
                "nx_neptune.utils.decorators.asyncio.get_running_loop",
                return_value=running,
            ),
            patch("nx_neptune.utils.decorators.NeptuneGraph") as na_graph_cls,
            patch("nx_neptune.utils.decorators._sync_data_to_neptune"),
            patch(
                "nx_neptune.utils.decorators._execute_setup_new_graph",
                side_effect=_setup_new,
            ),
            patch(
                "nx_neptune.utils.decorators.delete_na_instance", side_effect=_noop
            ) as delete,
            patch("nx_neptune.utils.decorators.set_config_graph_id") as set_graph_id,
        ):
            na_graph_cls.from_config.return_value = MagicMock(spec=NeptuneGraph)
            yield delete, set_graph_id

    @staticmethod
    def _failing(graph):
        raise ValueError("algorithm boom")

    def test_a_created_instance_is_destroyed_from_inside_a_running_loop(self, caplog):
        config = self._config()

        with caplog.at_level(logging.ERROR, logger="nx_neptune.utils.decorators"):
            with self._patched_in_loop(config) as (delete, set_graph_id):
                with pytest.raises(ValueError, match="algorithm boom"):
                    configure_if_nx_active()(self._failing)(nx.Graph())

        delete.assert_called_once_with("g-created")
        set_graph_id.assert_called_once_with(None)
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert (
            not errors
        ), f"teardown reported a failure: {[r.getMessage() for r in errors]}"

    def test_destroy_instance_false_is_honoured_in_a_running_loop(self, caplog):
        with caplog.at_level(logging.ERROR, logger="nx_neptune.utils.decorators"):
            with self._patched_in_loop(self._config(destroy_instance=False)) as (
                delete,
                set_graph_id,
            ):
                with pytest.raises(ValueError, match="algorithm boom"):
                    configure_if_nx_active()(self._failing)(nx.Graph())

        delete.assert_not_called()
        set_graph_id.assert_not_called()
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
