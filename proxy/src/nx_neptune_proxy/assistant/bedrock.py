# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Bedrock model factory for the assistant agents (spec §9.9).

Ported from the sample's ``utilities/bedrock.py``. Strands is imported lazily
so this package (schemas, json_utils, retry logic) stays importable in
environments where ``strands-agents`` is not installed.
"""

from typing import Optional

from nx_neptune_proxy.assistant.agent_aws import agent_boto_session
from nx_neptune_proxy.config import get_settings


def get_bedrock_model(model_id: Optional[str] = None, region_name: Optional[str] = None):
    """Build a Strands ``BedrockModel`` for the agent pipeline.

    Defaults come from settings (``BEDROCK_MODEL`` / ``BEDROCK_REGION``); callers
    may override ``model_id`` per request via the chat panel's model selector.
    """
    from botocore.config import Config  # noqa: PLC0415 (lazy: heavy import)
    from strands.models import BedrockModel  # noqa: PLC0415 (lazy: optional dep)

    settings = get_settings()
    model_id = model_id or settings.bedrock_model
    region_name = region_name or settings.bedrock_region or settings.region

    client_config = Config(
        region_name=region_name,
        signature_version="v4",
        connect_timeout=120,
        read_timeout=120,
        retries={"total_max_attempts": 5, "mode": "adaptive"},
    )
    kwargs = dict(
        model_id=model_id,
        region_name=region_name,
        cache_prompt="default",
        cache_tools="default",
        boto_client_config=client_config,
    )
    # Scope Bedrock inference to the assumed read-only agent role when
    # BEDROCK_AGENT_ROLE_ARN is set (spec §9.11); otherwise use the proxy's
    # ambient process credentials.
    session = agent_boto_session()
    if session is not None:
        kwargs["boto_session"] = session
    return BedrockModel(**kwargs)
