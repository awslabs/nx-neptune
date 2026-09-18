# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Base for the assistant's specialist agents (spec §9.5).

Each specialist is a :class:`BaseAgent` wrapping one Strands ``Agent`` with a
fixed system prompt. Strands is imported **lazily** (only when the underlying
agent is first built), so every specialist module stays importable — and its
prompts/parsing testable — without ``strands-agents`` installed. Group E wraps
these as supervisor tools via ``as_tools``.
"""

from nx_neptune_proxy.assistant.base import BaseAgent


def build_agent(name: str, system_prompt: str, model, tools=None):
    """Construct a Strands ``Agent`` (lazy import of ``strands``)."""
    from strands import Agent  # noqa: PLC0415 (lazy: optional dep)

    return Agent(
        name=name,
        system_prompt=system_prompt,
        model=model,
        callback_handler=None,
        tools=tools or [],
    )


def as_tools(funcs):
    """Wrap plain functions as Strands tools (lazy import of ``strands``)."""
    from strands import tool  # noqa: PLC0415 (lazy: optional dep)

    return [tool(f) for f in funcs]


class SpecialistAgent(BaseAgent):
    """A single-prompt Bedrock agent that returns a JSON contract.

    Subclasses set :attr:`NAME` / :attr:`SYSTEM_PROMPT`, and implement
    :meth:`_format_prompt` (turn kwargs → user prompt) plus a typed ``run``-style
    method that feeds :meth:`execute_task` output through
    :meth:`BaseAgent.extract_json`. Override :meth:`_tools` to expose tools.
    """

    NAME: str = "specialist"
    SYSTEM_PROMPT: str = ""

    def __init__(self, bedrock_model):
        super().__init__(self.NAME)
        self._bedrock_model = bedrock_model

    def _tools(self):
        return []

    def _ensure_agent(self):
        if self._agent is None:
            self._agent = build_agent(
                self.NAME, self.SYSTEM_PROMPT, self._bedrock_model, self._tools()
            )
        return self._agent

    def _execute_agent(self, **kwargs):
        return self._ensure_agent()(self._format_prompt(**kwargs))

    def _format_prompt(self, **kwargs) -> str:
        raise NotImplementedError("Subclasses must implement '_format_prompt'.")
