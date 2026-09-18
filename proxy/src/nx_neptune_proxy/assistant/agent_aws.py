# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Scoped credentials for the agent path (spec §9.11, Group H).

The agents hold no credentials of their own. By default their AWS calls run
under the proxy's own process role. When ``BEDROCK_AGENT_ROLE_ARN`` is set, the
proxy assumes that scoped **read-only** role via STS and hands the resulting
short-lived credentials to the agent's Athena and Bedrock clients — so
"the agent can only read/propose" is enforced at the AWS layer, not only by the
application-level guardrails (§9.10). Writes still happen only after human
confirmation, executed by the deterministic Phase 1 endpoints under the proxy's
own (broader) role.

The assumed session is cached and refreshed shortly before it expires, so a
long-running turn never uses lapsed credentials. No import of ``strands`` here,
so the assistant package stays importable without ``strands-agents``.
"""

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
from nx_neptune.clients.client_factory import ClientFactory

from nx_neptune_proxy.config import get_settings

logger = logging.getLogger(__name__)

# Refresh assumed-role credentials this long before they expire so a turn in
# flight never uses creds that lapse mid-call.
_REFRESH_SKEW = timedelta(minutes=5)
_SESSION_NAME = "nx-neptune-assistant"
# Requested lifetime; STS clamps this to the role's configured maximum.
_DURATION_SECONDS = 3600

_lock = threading.Lock()
_cached_session: Optional[boto3.Session] = None
_cached_expiry: Optional[datetime] = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def agent_role_arn() -> str:
    """The scoped read-only role ARN for the agent path, or '' when unset."""
    return get_settings().bedrock_agent_role_arn


def agent_boto_session() -> Optional[boto3.Session]:
    """Return a boto3 session scoped to the agent role, or ``None`` to use the
    proxy's own process role.

    When ``BEDROCK_AGENT_ROLE_ARN`` is set, assumes that role via STS and returns
    a session backed by the resulting short-lived credentials (cached until near
    expiry). When unset, returns ``None`` so callers fall back to the default
    ``ClientFactory`` / ambient credentials.
    """
    role_arn = agent_role_arn()
    if not role_arn:
        return None

    global _cached_session, _cached_expiry
    with _lock:
        if (
            _cached_session is not None
            and _cached_expiry is not None
            and _now() + _REFRESH_SKEW < _cached_expiry
        ):
            return _cached_session

        resp = ClientFactory().sts().assume_role(
            RoleArn=role_arn,
            RoleSessionName=_SESSION_NAME,
            DurationSeconds=_DURATION_SECONDS,
        )
        creds = resp["Credentials"]
        settings = get_settings()
        _cached_session = boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
            region_name=settings.region or None,
        )
        _cached_expiry = creds["Expiration"]
        logger.info("Assumed agent role %s (expires %s)", role_arn, _cached_expiry)
        return _cached_session


def agent_athena_client():
    """Athena client for the agent path: assumed-role-scoped when configured,
    else the proxy's process-role client via ``ClientFactory``."""
    session = agent_boto_session()
    if session is None:
        return ClientFactory().athena()
    return session.client("athena", region_name=get_settings().region or None)


def reset_agent_credentials_cache() -> None:
    """Clear the cached assumed-role session (tests / on configuration change)."""
    global _cached_session, _cached_expiry
    with _lock:
        _cached_session = None
        _cached_expiry = None
