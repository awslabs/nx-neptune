# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Navigation agent (spec §9.5 / §9.7).

Resolves cross-page navigation intent into ``JumpAction`` descriptors the
client already executes in ``runJump`` (``new-import`` / ``new-project`` /
``open-projections``). A project named in the request is resolved against the
project store; otherwise the snapshot's current ``project_id`` is used. The
agent only proposes — the client performs the actual navigation.
"""

from nx_neptune_proxy.assistant.agents.base_specialist import SpecialistAgent, as_tools
from nx_neptune_proxy.assistant.schemas import JumpAction
from nx_neptune_proxy.services.project_store import ProjectStore


def list_projects() -> list[dict]:
    """Return existing projects as ``[{"id", "name"}]`` (to resolve a name)."""
    return [{"id": p.id, "name": p.name} for p in ProjectStore().list()]


SYSTEM_PROMPT = """You translate a user's navigation request into jump \
descriptors for a graph-import web app.

Valid jump kinds:
- "new-import": start a new import (optionally for a given project_id).
- "new-project": create a new project.
- "open-projections": open a project's projections list (needs project_id).

If the user names a project, call list_projects() and match by name to get its \
id. If no project is named, use the current project_id from the request when \
one is provided. Only emit jumps the user actually asked for; emit none if the \
request is not about navigation.

# OUTPUT REQUIREMENTS
Return ONLY JSON, no prose, no Markdown, with this exact structure:
{"jumps": [{"kind": "open-projections", "label": "Open TPCH", \
"project_id": "p1"}]}
Use {"jumps": []} when no navigation is requested. Omit project_id when not \
applicable.
"""

USER_PROMPT = """current_project_id: {project_id}

Request:
{request}
"""


class NavigationAgent(SpecialistAgent):
    NAME = "navigation"
    SYSTEM_PROMPT = SYSTEM_PROMPT

    def _tools(self):
        return as_tools([list_projects])

    def _format_prompt(self, **kwargs) -> str:
        return USER_PROMPT.format(
            project_id=kwargs.get("project_id") or "none",
            request=kwargs["request"],
        )

    def navigate(self, request: str, project_id: str = None) -> list[JumpAction]:
        raw = self.execute_task(request=request, project_id=project_id)
        data = self.extract_json(raw)
        return [JumpAction.model_validate(j) for j in data.get("jumps", [])]
