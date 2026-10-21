# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The proxy state directory must survive a container recreation.

The proxy keeps its SQLite database under the home directory
(``~/.nx-neptune/proxy.db``). The compose file mounts a named volume on that
directory and the image creates it owned by ``appuser``, so the volume is
writable by the unprivileged container user and the projects/projections
defined in the UI survive ``make up``.

These tests keep the compose file, the Dockerfile and the code default in sync.
"""

import os
import re
from pathlib import Path

import pytest

PROXY_DIR = Path(__file__).resolve().parents[1]
COMPOSE_FILE = PROXY_DIR / "docker-compose.yml"
DOCKERFILE = PROXY_DIR / "Dockerfile"

APPUSER_HOME = "/home/appuser"
STATE_DIR_NAME = ".nx-neptune"
CONTAINER_STATE_DIR = f"{APPUSER_HOME}/{STATE_DIR_NAME}"
VOLUME_NAME = "proxy-state"


def _compose_text() -> str:
    return COMPOSE_FILE.read_text(encoding="utf-8")


def _service_block(text: str, service: str) -> str:
    """Return the body of one service block from a compose file."""
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.rstrip() == f"  {service}:":
            start = i + 1
            break
    if start is None:
        pytest.fail(f"service {service!r} not found in {COMPOSE_FILE.name}")
    block = []
    for line in lines[start:]:
        if line.strip() and not line.startswith("    "):
            break  # next service or a top-level key
        block.append(line)
    return "\n".join(block)


def test_compose_mounts_a_named_volume_on_the_state_dir():
    block = _service_block(_compose_text(), "proxy")
    msg = "the proxy service must mount a volume on the state directory"
    assert f":{CONTAINER_STATE_DIR}" in block, msg


def test_compose_declares_the_named_volume():
    text = _compose_text()
    declared = re.search(
        rf"^volumes:\s*\n(?:[ \t]+.*\n)*?[ \t]+{VOLUME_NAME}:\s*$",
        text,
        re.MULTILINE,
    )
    msg = f"the compose file must declare the {VOLUME_NAME} volume"
    assert declared, msg


def test_dockerfile_creates_the_state_dir_owned_by_appuser():
    """A fresh named volume inherits the image directory's ownership."""
    lines = DOCKERFILE.read_text(encoding="utf-8").splitlines()
    user_index = next(
        (i for i, line in enumerate(lines) if line.strip() == "USER appuser"), None
    )
    msg = "the Dockerfile must switch to the appuser account"
    assert user_index is not None, msg
    before_user = "\n".join(lines[:user_index])

    msg = "the image must create the state directory before switching to appuser"
    assert f"mkdir -p {CONTAINER_STATE_DIR}" in before_user, msg

    msg = "the image must give the state directory to appuser"
    assert f"chown appuser:appuser {CONTAINER_STATE_DIR}" in before_user, msg


def test_default_database_path_is_under_the_mounted_dir():
    """The code default and the mounted directory must be the same location."""
    if os.environ.get("NX_NEPTUNE_DB_PATH"):
        pytest.skip("NX_NEPTUNE_DB_PATH is set; the default path is not in use")
    from nx_neptune_proxy.services import db

    msg = "the default database path must live under the mounted state directory"
    assert Path(db.DB_PATH).parent.name == Path(CONTAINER_STATE_DIR).name, msg
