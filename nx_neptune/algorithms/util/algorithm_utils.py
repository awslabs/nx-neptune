"""
Utility functions for Neptune Analytics algorithms.
"""

import logging
from typing import Any, Dict

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
