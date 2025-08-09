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
Utility functions for Neptune Analytics algorithms.
"""

import logging
from typing import Any, Dict

from nx_neptune.clients.neptune_constants import RESPONSE_SUCCESS
from nx_neptune.na_graph import get_config

logger = logging.getLogger(__name__)


def process_unsupported_param(params: Dict[str, Any]) -> None:
    """
    Process unsupported parameters for Neptune Analytics algorithms.
    Only prints warnings for parameters with non-None values.

    :param params: Dictionary with parameter names as keys and parameter values as values
    """
    for param_name, param_value in params.items():
        if param_value is not None:
            logger.warning(
                f"'{param_name}' parameter is not supported in Neptune Analytics implementation. "
                f"This argument will be ignored and execution will proceed without it."
            )


def execute_mutation_query(neptune_graph, parameters, algo_name, algo_query_call):
    """
    Responsible to handle the execution flow of mutate variant of Neptune Analytics Algorithm.

    :param neptune_graph: A NeptuneGraph instance
    :param parameters: Dictionary with parameter names as keys and parameter values as values
    :param algo_name: Name of the algorithm which will be printed on the log statement.
    :param algo_query_call: Function call, which responsible to read an parameters Dict and composite
    the associated query string and parameter map for given algorithm.
    """
    get_config().validate_mutate_execution_config()

    query_str, para_map = algo_query_call(parameters)
    json_result = neptune_graph.execute_call(query_str, para_map)
    execution_result = json_result[0].get(RESPONSE_SUCCESS) is True

    if not execution_result:
        logger.error(
            f"Algorithm execution [{algo_name}] failed, refer to AWS console for more detail."
        )
    return {}
