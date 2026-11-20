# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Neptune capability skill (spec §9.13): the vendored reference loader, the
always-on catalog constants, and their wiring into the specialist prompts.

Strands is never constructed here — these assert on the compiled prompt strings
and the plain ``load_neptune_skill`` function, so they run without
``strands-agents`` installed.
"""

import pytest

from nx_neptune_proxy.assistant.agents.query_planner import (
    QueryPlannerAgent,
    neptune_skill_reference,
)
from nx_neptune_proxy.assistant.agents.sql_mapping import SqlMappingAgent
from nx_neptune_proxy.assistant.skills import (
    CAPABILITY_CATALOG,
    CAPABILITY_HINTS,
    DATA_MODELING_GUIDANCE,
    SkillTopic,
    load_neptune_skill,
)
from nx_neptune_proxy.assistant.supervisor import SUPERVISOR_SYSTEM_PROMPT

TOPICS = ("querying", "use-cases", "graphrag", "data-modeling")


# --- load_neptune_skill ---------------------------------------------------


@pytest.mark.parametrize("topic", TOPICS)
def test_load_neptune_skill_returns_reference_text(topic):
    text = load_neptune_skill(topic)
    # Each vendored file is a non-trivial Markdown reference.
    assert text.lstrip().startswith("#")
    assert len(text) > 200


def test_load_neptune_skill_unknown_topic_fails_soft():
    result = load_neptune_skill("connectivity")  # real upstream file, not vendored
    assert "unavailable" in result
    # Lists the known topics rather than raising.
    for topic in TOPICS:
        assert topic in result


def test_skill_topic_literal_matches_vendored_set():
    from typing import get_args

    assert set(get_args(SkillTopic)) == set(TOPICS)


def test_neptune_skill_reference_tool_delegates_to_loader():
    assert neptune_skill_reference("querying") == load_neptune_skill("querying")


# --- catalog reconciled against the backend ------------------------------


def test_catalog_cites_runnable_neptune_analytics_procedures():
    # Per #491, the catalog treats every Neptune Analytics procedure as runnable
    # (not just the ones nx-neptune wraps as Python functions), so all of these
    # must be named in the catalog.
    for proc in (
        "pageRank", "degree", "closenessCentrality", "louvain",
        "labelPropagation", "bfs", "bellmanFord", "deltaStepping", "wcc", "scc",
    ):
        assert proc in CAPABILITY_CATALOG
    # The old "NOT available" gating list was removed with that change.
    assert "NOT available" not in CAPABILITY_CATALOG


# --- prompt wiring --------------------------------------------------------


def test_capability_catalog_in_query_planner_prompt():
    assert CAPABILITY_CATALOG in QueryPlannerAgent.SYSTEM_PROMPT
    # The output contract still trails the catalog.
    assert "OUTPUT REQUIREMENTS" in QueryPlannerAgent.SYSTEM_PROMPT
    assert QueryPlannerAgent.SYSTEM_PROMPT.rstrip().endswith("single line.")


def test_capability_hints_in_supervisor_prompt():
    assert CAPABILITY_HINTS in SUPERVISOR_SYSTEM_PROMPT


def test_data_modeling_guidance_in_sql_mapping_prompt():
    assert DATA_MODELING_GUIDANCE in SqlMappingAgent.SYSTEM_PROMPT
    # The reserved-column aliasing contract is preserved alongside the guidance.
    assert '"~id"' in SqlMappingAgent.SYSTEM_PROMPT
    assert "OUTPUT REQUIREMENTS" in SqlMappingAgent.SYSTEM_PROMPT
