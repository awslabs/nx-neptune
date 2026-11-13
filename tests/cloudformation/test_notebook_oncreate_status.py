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
"""
The notebook OnCreate step runs its body in a ``sudo -u ec2-user -i <<'EOF'``
login shell. The template's outer ``set -e`` does NOT reach inside that heredoc,
so the body must set its own. Without it, a failed ``conda create``, wheel
``pip install`` or ``unzip`` leaves OnCreate exit 0 (the heredoc's status is its
last command, ``conda deactivate``), and the notebook comes up half-provisioned
while SageMaker reports success.

These tests render the real OnCreate body and run it against stubbed commands:

- a failing wheel ``pip install`` must abort the body (regression guard for the
  missing in-heredoc ``set -e``);
- the happy path must still exit 0 (the ``set -e`` must not break provisioning).
"""

import json
import os
import subprocess
from pathlib import Path

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "cloudformation-templates"
TEMPLATE = json.loads((TEMPLATES_DIR / "nx-neptune-sagemaker.json").read_text())


def _render_oncreate_heredoc():
    """Return the OnCreate heredoc body as a runnable shell script.

    Joins the ``Fn::Join`` parts, substituting each intrinsic ({"Ref": ...} etc.)
    with a harmless placeholder, then extracts the text inside the ``<<'EOF'``
    heredoc.
    """
    parts = TEMPLATE["Resources"]["NotebookLifecycleConfig"]["Properties"][
        "OnCreate"
    ][0]["Content"]["Fn::Base64"]["Fn::Join"][1]
    rendered = "".join(p if isinstance(p, str) else "PLACEHOLDER" for p in parts)
    marker = "<<'EOF'\n"
    start = rendered.index(marker) + len(marker)
    end = rendered.index("\nEOF", start)
    return rendered[start:end]


def _make_stub(path, body):
    path.write_text("#!/bin/bash\n" + body)
    path.chmod(0o755)


def _run_oncreate(tmp_path, *, wheel_install_fails):
    """Render and run the OnCreate body with stubbed commands.

    Rewrites the hard-coded ``/home/ec2-user`` and ``/tmp/nx-assets`` paths to
    throwaway locations so the body runs unprivileged, stages a wheel so the
    body takes its wheel-install branch, and returns the process result.
    """
    home = tmp_path / "home"
    (home / "anaconda3" / "bin").mkdir(parents=True)
    (home / "anaconda3" / "bin" / "activate").write_text("")  # sourced, no-op
    assets = tmp_path / "nx-assets"
    assets.mkdir()
    (assets / "nx_neptune.whl").write_text("")  # makes `ls *.whl` succeed

    body = _render_oncreate_heredoc()
    body = body.replace("/home/ec2-user", str(home))
    body = body.replace("/tmp/nx-assets", str(assets))
    script = tmp_path / "oncreate.sh"
    script.write_text(body)

    stubs = tmp_path / "stubs"
    stubs.mkdir()
    _make_stub(stubs / "conda", "exit 0\n")
    _make_stub(stubs / "python", "exit 0\n")
    _make_stub(stubs / "unzip", "exit 0\n")
    _make_stub(stubs / "aws", "exit 0\n")  # s3 cp is a no-op; wheel is pre-staged
    # pip fails only on a wheel install when the scenario asks for it.
    pip_body = (
        'for a in "$@"; do case "$a" in *.whl) exit 1;; esac; done\nexit 0\n'
        if wheel_install_fails
        else "exit 0\n"
    )
    _make_stub(stubs / "pip", pip_body)

    env = os.environ.copy()
    env["PATH"] = f"{stubs}{os.pathsep}{env['PATH']}"
    return subprocess.run(
        ["bash", str(script)], capture_output=True, text=True, env=env
    )


def test_oncreate_body_sets_its_own_errexit():
    # The outer `set -e` does not cross into the login-shell heredoc, so the
    # body itself must enable errexit as its first statement.
    body = _render_oncreate_heredoc()
    first = next(
        line.strip() for line in body.splitlines() if line.strip() and not line.startswith("#")
    )
    assert first.startswith("set -e"), (
        f"OnCreate heredoc must start with `set -e...`, got: {first!r}"
    )


def test_failed_wheel_install_aborts_oncreate(tmp_path):
    proc = _run_oncreate(tmp_path, wheel_install_fails=True)
    assert proc.returncode != 0, (
        "A failed wheel pip install must abort OnCreate; it exited 0, meaning "
        "SageMaker would report a half-provisioned notebook as successful.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )


def test_oncreate_succeeds_when_steps_pass(tmp_path):
    # Positive control: the in-heredoc `set -e` must not break the happy path.
    proc = _run_oncreate(tmp_path, wheel_install_fails=False)
    assert proc.returncode == 0, (
        f"OnCreate should exit 0 when all steps succeed.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
