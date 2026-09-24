# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Specialist agents behind the assistant supervisor (spec §9.5).

Each is a single-prompt Bedrock agent returning a JSON contract. Strands is
imported lazily inside :mod:`base_specialist`, so importing these modules does
not require ``strands-agents``.
"""

from nx_neptune_proxy.assistant.agents.base_specialist import (
    SpecialistAgent,
    as_tools,
    build_agent,
)
from nx_neptune_proxy.assistant.agents.discovery import DiscoveryAgent
from nx_neptune_proxy.assistant.agents.navigation import NavigationAgent
from nx_neptune_proxy.assistant.agents.page_action import PageActionAgent
from nx_neptune_proxy.assistant.agents.query_planner import QueryPlannerAgent
from nx_neptune_proxy.assistant.agents.sql_mapping import SqlMappingAgent

__all__ = [
    "SpecialistAgent",
    "as_tools",
    "build_agent",
    "DiscoveryAgent",
    "SqlMappingAgent",
    "QueryPlannerAgent",
    "NavigationAgent",
    "PageActionAgent",
]
