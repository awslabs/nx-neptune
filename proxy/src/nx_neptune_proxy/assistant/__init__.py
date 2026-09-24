# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AI assistant agent backend (spec §9).

Framework layer (Group B): the ``BaseAgent`` wrapper, the Bedrock model
factory, JSON-contract schemas, and the ``extract_json`` recovery helper.
Specialist agents, the supervisor, and the endpoints are added in later groups.
"""

from nx_neptune_proxy.assistant.athena_tools import (
    AthenaToolError,
    get_columns,
    list_tables,
    sample_table,
)
from nx_neptune_proxy.assistant.base import BaseAgent
from nx_neptune_proxy.assistant.json_utils import extract_json
from nx_neptune_proxy.assistant.session import Session, SessionStore
from nx_neptune_proxy.assistant.supervisor import Supervisor

__all__ = [
    "BaseAgent",
    "extract_json",
    "list_tables",
    "get_columns",
    "sample_table",
    "AthenaToolError",
    "Supervisor",
    "Session",
    "SessionStore",
]
