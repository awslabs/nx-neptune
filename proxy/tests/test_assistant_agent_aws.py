# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Scoped agent credentials (spec §9.11, Group H): the agent path assumes a
read-only role when ``BEDROCK_AGENT_ROLE_ARN`` is set, and falls back to the
proxy's process role otherwise. STS is mocked; no real AWS calls."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from nx_neptune_proxy.assistant import agent_aws

AWS = "nx_neptune_proxy.assistant.agent_aws"


@pytest.fixture(autouse=True)
def _clear_cache():
    agent_aws.reset_agent_credentials_cache()
    yield
    agent_aws.reset_agent_credentials_cache()


def _settings(role_arn="", region="us-east-1"):
    return SimpleNamespace(bedrock_agent_role_arn=role_arn, region=region)


# --- fallback (no role configured) ---------------------------------------


@patch(f"{AWS}.ClientFactory")
@patch(f"{AWS}.get_settings")
def test_no_role_returns_none_session(mock_settings, mock_cf):
    mock_settings.return_value = _settings(role_arn="")
    assert agent_aws.agent_boto_session() is None
    mock_cf.return_value.sts.assert_not_called()


@patch(f"{AWS}.ClientFactory")
@patch(f"{AWS}.get_settings")
def test_no_role_athena_client_uses_client_factory(mock_settings, mock_cf):
    mock_settings.return_value = _settings(role_arn="")
    client = agent_aws.agent_athena_client()
    assert client is mock_cf.return_value.athena.return_value


# --- assumed role (role configured) --------------------------------------


def _sts_returning(expiry):
    sts = MagicMock()
    sts.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "AKIA",
            "SecretAccessKey": "secret",
            "SessionToken": "token",
            "Expiration": expiry,
        }
    }
    return sts


@patch(f"{AWS}.boto3")
@patch(f"{AWS}.ClientFactory")
@patch(f"{AWS}.get_settings")
def test_role_assumes_and_builds_session(mock_settings, mock_cf, mock_boto3):
    mock_settings.return_value = _settings(role_arn="arn:aws:iam::1:role/ro")
    expiry = datetime.now(timezone.utc) + timedelta(hours=1)
    mock_cf.return_value.sts.return_value = _sts_returning(expiry)

    session = agent_aws.agent_boto_session()

    assert session is mock_boto3.Session.return_value
    mock_cf.return_value.sts.return_value.assume_role.assert_called_once()
    _, kwargs = mock_boto3.Session.call_args
    assert kwargs["aws_access_key_id"] == "AKIA"
    assert kwargs["aws_session_token"] == "token"


@patch(f"{AWS}.boto3")
@patch(f"{AWS}.ClientFactory")
@patch(f"{AWS}.get_settings")
def test_session_is_cached_until_near_expiry(mock_settings, mock_cf, mock_boto3):
    mock_settings.return_value = _settings(role_arn="arn:aws:iam::1:role/ro")
    sts = _sts_returning(datetime.now(timezone.utc) + timedelta(hours=1))
    mock_cf.return_value.sts.return_value = sts

    agent_aws.agent_boto_session()
    agent_aws.agent_boto_session()
    sts.assume_role.assert_called_once()  # second call served from cache


@patch(f"{AWS}.boto3")
@patch(f"{AWS}.ClientFactory")
@patch(f"{AWS}.get_settings")
def test_session_refreshes_when_expired(mock_settings, mock_cf, mock_boto3):
    mock_settings.return_value = _settings(role_arn="arn:aws:iam::1:role/ro")
    # Expiry already inside the refresh skew → must re-assume every call.
    sts = _sts_returning(datetime.now(timezone.utc) + timedelta(minutes=1))
    mock_cf.return_value.sts.return_value = sts

    agent_aws.agent_boto_session()
    agent_aws.agent_boto_session()
    assert sts.assume_role.call_count == 2


@patch(f"{AWS}.boto3")
@patch(f"{AWS}.ClientFactory")
@patch(f"{AWS}.get_settings")
def test_role_athena_client_from_assumed_session(mock_settings, mock_cf, mock_boto3):
    mock_settings.return_value = _settings(role_arn="arn:aws:iam::1:role/ro")
    mock_cf.return_value.sts.return_value = _sts_returning(
        datetime.now(timezone.utc) + timedelta(hours=1)
    )

    client = agent_aws.agent_athena_client()

    session = mock_boto3.Session.return_value
    session.client.assert_called_once_with("athena", region_name="us-east-1")
    assert client is session.client.return_value
    # Never went through the process-role ClientFactory for the client.
    mock_cf.return_value.athena.assert_not_called()
