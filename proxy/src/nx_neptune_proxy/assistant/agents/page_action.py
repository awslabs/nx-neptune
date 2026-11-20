# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Page-Action agent (spec §9.5 / §9.10).

Surfaces the current page's registered actions as ``ChatAction`` descriptors
the client executes in ``runChatAction``. Constrained to the page snapshot: the
agent can only reference action keys / graph targets the page actually
registered, enforced here by filtering the model's output against the snapshot
(so a prompt-injected value can never invent an action).
"""

from nx_neptune_proxy.assistant.agents.base_specialist import SpecialistAgent
from nx_neptune_proxy.assistant.schemas import ChatAction, PageContext

SYSTEM_PROMPT = """You surface actions the user can take on the current page of \
a graph-import web app.

You are given the page's registered actions and graph targets. Propose the \
actions relevant to the user's request, using ONLY the keys and targets \
provided — never invent an action key, graph id, or graph action that is not \
listed. A disabled action may still be surfaced (the client shows it disabled).

# OUTPUT REQUIREMENTS
Return ONLY JSON, no prose, no Markdown, with this exact structure:
{"actions": [
  {"kind": "page-action", "page": "import", "label": "Execute", \
"action_key": "execute", "enabled": false},
  {"kind": "graph-action", "page": "graphs", "label": "Stop malware-graph", \
"graph_id": "g-1", "graph_action": "stop", "destructive": true}
]}
Use {"actions": []} when nothing is relevant.
"""

USER_PROMPT = """# Page snapshot
{snapshot}

# Request
{request}
"""


class PageActionAgent(SpecialistAgent):
    NAME = "page_action"
    SYSTEM_PROMPT = SYSTEM_PROMPT

    def _format_prompt(self, **kwargs) -> str:
        return USER_PROMPT.format(
            snapshot=kwargs["snapshot"], request=kwargs["request"]
        )

    def suggest(self, context: PageContext, request: str) -> list[ChatAction]:
        snapshot = context.model_dump_json(exclude_none=True)
        raw = self.execute_task(snapshot=snapshot, request=request)
        data = self.extract_json(raw)
        actions = [ChatAction.model_validate(a) for a in data.get("actions", [])]
        return self._filter_to_snapshot(actions, context)

    @staticmethod
    def _filter_to_snapshot(
        actions: list[ChatAction], context: PageContext
    ) -> list[ChatAction]:
        """Drop any action the page snapshot did not register (§9.10)."""
        page_keys = {a.key for a in context.actions}
        graph_actions = {t.id: set(t.actions) for t in context.graph_targets}

        kept: list[ChatAction] = []
        for a in actions:
            if a.kind == "page-action":
                if a.action_key in page_keys:
                    kept.append(a)
            elif a.kind == "graph-action":
                if a.graph_action in graph_actions.get(a.graph_id, set()):
                    kept.append(a)
        return kept
