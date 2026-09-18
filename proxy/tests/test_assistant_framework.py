# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Assistant agent framework (spec §9, Group B): extract_json, BaseAgent
retry/metrics, and the JSON-contract schemas."""

import json

import pytest

from nx_neptune_proxy.assistant.base import BaseAgent
from nx_neptune_proxy.assistant.json_utils import extract_json
from nx_neptune_proxy.assistant.schemas import (
    AssistantReply,
    ChatAction,
    DiscoveryResult,
    FieldProposal,
    JumpAction,
    SqlMappingResult,
)


# --- extract_json ---------------------------------------------------------


def test_extract_json_plain_object():
    assert extract_json('{"a": 1}') == {"a": 1}


def test_extract_json_from_code_fence():
    text = 'Here you go:\n```json\n{"a": 1, "b": [2, 3]}\n```\nDone.'
    assert extract_json(text) == {"a": 1, "b": [2, 3]}


def test_extract_json_surrounded_by_prose_with_nested_braces():
    text = 'Sure. {"outer": {"inner": 1}} hope that helps'
    assert extract_json(text) == {"outer": {"inner": 1}}


def test_extract_json_collapses_raw_newlines_in_sql_value():
    # Model emitted a multi-line SQL value with unescaped newlines -> invalid
    # JSON until we collapse the whitespace.
    text = '{"sql": "SELECT a,\n       b\nFROM t"}'
    with pytest.raises(json.JSONDecodeError):
        json.loads(text)
    assert extract_json(text) == {"sql": "SELECT a, b FROM t"}


def test_extract_json_coerces_into_pydantic_model():
    text = '```json\n{"node_queries": [{"sql": "SELECT 1"}], "edge_queries": []}\n```'
    result = extract_json(text, SqlMappingResult)
    assert isinstance(result, SqlMappingResult)
    assert result.node_queries[0].sql == "SELECT 1"


def test_extract_json_raises_when_no_object():
    with pytest.raises(json.JSONDecodeError):
        extract_json("no json here")


# --- BaseAgent retry / metrics -------------------------------------------


class _FakeClientError(Exception):
    """Mimics botocore ClientError: carries a ``response`` dict with a code."""

    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class EventLoopException(Exception):
    """Name-matched to the strands retryable set (matched by class name)."""


class _StubAgent(BaseAgent):
    def __init__(self, results):
        super().__init__("stub")
        self._results = list(results)
        self.calls = 0
        self.slept = []

    def _execute_agent(self, **kwargs):
        self.calls += 1
        outcome = self._results.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def _sleep(self, seconds):
        self.slept.append(seconds)


def test_execute_task_returns_result_string():
    agent = _StubAgent(["hello"])
    assert agent.execute_task() == "hello"
    assert agent.calls == 1


def test_execute_task_retries_retryable_client_error_then_succeeds():
    agent = _StubAgent([_FakeClientError("ThrottlingException"), "ok"])
    assert agent.execute_task() == "ok"
    assert agent.calls == 2
    assert agent.slept == [1.0]  # one backoff before the successful retry


def test_execute_task_retries_exception_matched_by_name():
    agent = _StubAgent([EventLoopException(), EventLoopException(), "done"])
    assert agent.execute_task() == "done"
    assert agent.calls == 3
    assert agent.slept == [1.0, 2.0]  # exponential backoff


def test_execute_task_does_not_retry_non_retryable():
    agent = _StubAgent([_FakeClientError("AccessDeniedException")])
    with pytest.raises(_FakeClientError):
        agent.execute_task()
    assert agent.calls == 1
    assert agent.slept == []


def test_execute_task_gives_up_after_max_retries():
    agent = _StubAgent([EventLoopException()] * 10)
    agent.max_retries = 3
    with pytest.raises(EventLoopException):
        agent.execute_task()
    assert agent.calls == 4  # initial + 3 retries


def test_metrics_empty_before_any_run():
    agent = _StubAgent(["x"])
    assert agent.get_statistics() == {}
    assert agent.get_metrics_summary() == {}


# --- schema contracts -----------------------------------------------------


def test_assistant_reply_defaults():
    reply = AssistantReply(text="hi")
    assert reply.jumps == []
    assert reply.actions == []
    assert reply.proposal is None
    assert reply.question is None


def test_jump_and_chat_action_round_trip():
    jump = JumpAction(kind="open-projections", label="TPCH", project_id="p1")
    assert jump.model_dump() == {
        "kind": "open-projections",
        "label": "TPCH",
        "project_id": "p1",
    }
    action = ChatAction(
        kind="graph-action",
        page="graphs",
        label="Stop g1",
        graph_id="g1",
        graph_action="stop",
        destructive=True,
    )
    assert action.graph_id == "g1"
    assert action.action_key is None


def test_field_proposal_only_set_fields_serialized():
    proposal = FieldProposal(database="mitre_attack")
    dumped = proposal.model_dump(exclude_none=True)
    assert dumped == {"database": "mitre_attack"}


def test_discovery_result_parses_columns_and_optional_samples():
    result = DiscoveryResult.model_validate(
        {
            "tables": [
                {
                    "name": "malware",
                    "columns": [{"name": "id", "type": "string"}],
                }
            ]
        }
    )
    assert result.tables[0].columns[0].type == "string"
    assert result.tables[0].sample_rows is None
