# Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""Fixtures for running deploy.sh / teardown.sh against a stubbed AWS CLI."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
TEMPLATES = HERE.parents[1] / "cloudformation-templates"
STUBS = HERE / "stubs"


@pytest.fixture
def workspace(tmp_path):
    """A throwaway copy of the scripts so their build/ side effects stay in tmp."""
    ct = tmp_path / "cloudformation-templates"
    ct.mkdir()
    for name in ("deploy.sh", "teardown.sh"):
        dst = ct / name
        shutil.copy(TEMPLATES / name, dst)
        dst.chmod(0o755)
    # deploy.sh passes this path to `cloudformation deploy`; the stub ignores it.
    (ct / "nx-neptune-sagemaker.json").write_text("{}")
    notebooks = tmp_path / "notebooks"
    notebooks.mkdir()
    (notebooks / "demo.ipynb").write_text("{}")
    return ct


@pytest.fixture
def run_script(workspace, tmp_path):
    """Run a script with the stub CLI on PATH; return (process, logged aws calls)."""
    log = tmp_path / "aws-calls.log"

    def _run(script, args=("demo", "us-west-1"), **scenario):
        env = os.environ.copy()
        env["PATH"] = f"{STUBS}{os.pathsep}{env['PATH']}"
        env["AWS_STUB_LOG"] = str(log)
        env.update(scenario)
        proc = subprocess.run(
            ["bash", str(workspace / script), *args],
            capture_output=True,
            text=True,
            env=env,
        )
        calls = log.read_text().splitlines() if log.exists() else []
        return proc, calls

    return _run
