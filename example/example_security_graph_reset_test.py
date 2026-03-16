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
Security test demonstrating that when skip_graph_reset is True in NeptuneConfig,
_execute_setup_routines_on_graph will not reset the graph, and _sync_data_to_neptune
will not call clear_graph. This ensures existing graph data is preserved.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from nx_plugin.config import NeptuneConfig
from nx_neptune.utils.decorators import (
    _execute_setup_routines_on_graph,
    _sync_data_to_neptune,
)


def _create_mock_na_graph():
    """Create a mock NeptuneGraph with a tracked clear_graph method."""
    mock = MagicMock()
    mock.clear_graph = MagicMock()
    mock.add_nodes = MagicMock()
    mock.add_edges = MagicMock()
    return mock


def _create_mock_nx_graph(nodes=None, edges=None):
    """Create a mock NetworkX graph with nodes and edges."""
    mock = MagicMock()
    mock.nodes = MagicMock(return_value=nodes or [("A", {"label": "Person"})])
    mock.edges = MagicMock(return_value=edges or [("A", "B", {"label": "KNOWS"})])
    mock.is_directed = MagicMock(return_value=True)
    # Make bool(mock.nodes) and bool(mock.edges) truthy
    mock.nodes.__bool__ = MagicMock(return_value=bool(nodes or [("A", {})]))
    mock.edges.__bool__ = MagicMock(return_value=bool(edges or [("A", "B", {})]))
    return mock


def test_setup_routines_skip_reset_true():
    """When skip_graph_reset=True, import_csv_from_s3 must be called with reset_graph_ahead=True (skip reset)."""
    print("=== Test: _execute_setup_routines_on_graph with skip_graph_reset=True ===")

    mock_na_graph = _create_mock_na_graph()
    config = NeptuneConfig(
        graph_id="g-test123",
        import_s3_bucket="s3://test-bucket/data/",
        s3_iam_role="arn:aws:iam::123456789012:role/TestRole",
        skip_graph_reset=True,
    )

    with patch(
        "nx_neptune.utils.decorators.import_csv_from_s3", new_callable=AsyncMock
    ) as mock_import:
        asyncio.run(_execute_setup_routines_on_graph(mock_na_graph, config))

        mock_import.assert_called_once()
        call_args = mock_import.call_args

        # Third positional arg is reset_graph_ahead, which receives skip_graph_reset
        # skip_graph_reset=True means "skip the reset" → passed as reset_graph_ahead=True
        reset_graph_ahead_value = call_args[0][2]
        assert (
            reset_graph_ahead_value is True
        ), f"Expected reset_graph_ahead=True (skip reset), got {reset_graph_ahead_value}"

    print("  PASS: import_csv_from_s3 called with skip_graph_reset=True (reset skipped)")
    print()


def test_setup_routines_skip_reset_false():
    """When skip_graph_reset=False, import_csv_from_s3 must be called with reset_graph_ahead=False."""
    print("=== Test: _execute_setup_routines_on_graph with skip_graph_reset=False ===")

    mock_na_graph = _create_mock_na_graph()
    config = NeptuneConfig(
        graph_id="g-test123",
        import_s3_bucket="s3://test-bucket/data/",
        s3_iam_role="arn:aws:iam::123456789012:role/TestRole",
        skip_graph_reset=False,
    )

    with patch(
        "nx_neptune.utils.decorators.import_csv_from_s3", new_callable=AsyncMock
    ) as mock_import:
        asyncio.run(_execute_setup_routines_on_graph(mock_na_graph, config))

        call_args = mock_import.call_args
        reset_graph_ahead_value = call_args[0][2]
        assert (
            reset_graph_ahead_value is False
        ), f"Expected reset_graph_ahead=False (reset allowed), got {reset_graph_ahead_value}"

    print("  PASS: import_csv_from_s3 called with skip_graph_reset=False (reset allowed)")
    print()


def test_setup_routines_no_import_no_reset():
    """When import_s3_bucket is None, import_csv_from_s3 must not be called at all."""
    print("=== Test: _execute_setup_routines_on_graph with no import ===")

    mock_na_graph = _create_mock_na_graph()
    config = NeptuneConfig(graph_id="g-test123", skip_graph_reset=True)

    with patch(
        "nx_neptune.utils.decorators.import_csv_from_s3", new_callable=AsyncMock
    ) as mock_import:
        asyncio.run(_execute_setup_routines_on_graph(mock_na_graph, config))
        mock_import.assert_not_called()

    print("  PASS: import_csv_from_s3 not called when import_s3_bucket is None")
    print()


def test_sync_data_skip_reset_true_no_clear():
    """When skip_graph_reset=True, _sync_data_to_neptune must NOT call clear_graph."""
    print("=== Test: _sync_data_to_neptune with skip_graph_reset=True ===")

    mock_na_graph = _create_mock_na_graph()
    mock_nx_graph = _create_mock_nx_graph()
    config = NeptuneConfig(graph_id="g-test123", skip_graph_reset=True)

    with patch("nx_neptune.utils.decorators.Node") as MockNode, patch(
        "nx_neptune.utils.decorators.Edge"
    ) as MockEdge:
        MockNode.convert_from_nx = MagicMock(return_value=MagicMock())
        MockEdge.convert_from_nx = MagicMock(return_value=MagicMock())

        _sync_data_to_neptune(mock_nx_graph, mock_na_graph, config)

    mock_na_graph.clear_graph.assert_not_called()
    print("  PASS: clear_graph NOT called when skip_graph_reset=True")
    print()


def test_sync_data_skip_reset_false_calls_clear():
    """When skip_graph_reset=False, _sync_data_to_neptune MUST call clear_graph."""
    print("=== Test: _sync_data_to_neptune with skip_graph_reset=False ===")

    mock_na_graph = _create_mock_na_graph()
    mock_nx_graph = _create_mock_nx_graph()
    config = NeptuneConfig(graph_id="g-test123", skip_graph_reset=False)

    with patch("nx_neptune.utils.decorators.Node") as MockNode, patch(
        "nx_neptune.utils.decorators.Edge"
    ) as MockEdge:
        MockNode.convert_from_nx = MagicMock(return_value=MagicMock())
        MockEdge.convert_from_nx = MagicMock(return_value=MagicMock())

        _sync_data_to_neptune(mock_nx_graph, mock_na_graph, config)

    mock_na_graph.clear_graph.assert_called_once()
    print("  PASS: clear_graph called when skip_graph_reset=False")
    print()


def main():
    test_setup_routines_skip_reset_true()
    test_setup_routines_skip_reset_false()
    test_setup_routines_no_import_no_reset()
    test_sync_data_skip_reset_true_no_clear()
    test_sync_data_skip_reset_false_calls_clear()
    print("=" * 50)
    print("All graph reset security tests passed.")


if __name__ == "__main__":
    main()
