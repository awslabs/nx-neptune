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
from unittest.mock import MagicMock, patch

import pytest

from nx_neptune.algorithms.util.algorithm_utils import execute_mutation_query


class TestAlgorithmUtils:

    @pytest.mark.parametrize(
        "mock_response",
        [
            # Wrong Key
            {"status": "SUCCEEDED"},
            # Correct key w wrong value
            {"success": "False"},
            # Only pass when it's exact match
            {"success": "TRUE"},
            # Only boolean form is accepted
            {"success": "True"},
            # Bool false
            {"success": False},
            # Numeric
            {"success": 123},
        ],
    )
    @patch("nx_neptune.algorithms.util.algorithm_utils.logger")
    def test_execute_mutation_query_failure(self, mock_logger, mock_response):
        # Setup: mock NeptuneGraph
        mock_neptune_graph = MagicMock()
        mock_neptune_graph.execute_call.return_value = [mock_response]

        # # Setup: mock algo_query_call
        mock_algo_query_call = MagicMock()
        mock_algo_query_call.return_value = ("FAKE QUERY", {"key": "value"})

        execute_mutation_query(
            neptune_graph=mock_neptune_graph,
            parameters={"key": "value"},
            algo_name="mock_algo",
            algo_query_call=mock_algo_query_call,
        )

        # Verify error were logged.
        assert mock_logger.error.call_count == 1

        # Check specific error messages
        mock_logger.error.assert_any_call(
            f"Algorithm execution [mock_algo] failed, refer to AWS console for more detail."
        )

    @patch("nx_neptune.algorithms.util.algorithm_utils.logger")
    def test_execute_mutation_query_success(self, mock_logger):
        # Setup: mock NeptuneGraph
        mock_neptune_graph = MagicMock()
        mock_neptune_graph.execute_call.return_value = [{"success": True}]

        # Setup: mock algo_query_call
        mock_algo_query_call = MagicMock()
        mock_algo_query_call.return_value = ("FAKE QUERY", {"key": "value"})

        execute_mutation_query(
            neptune_graph=mock_neptune_graph,
            parameters={"key": "value"},
            algo_name="mock_algo",
            algo_query_call=mock_algo_query_call,
        )

        assert mock_logger.error.call_count == 0
