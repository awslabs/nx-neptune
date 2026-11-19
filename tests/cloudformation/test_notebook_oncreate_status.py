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

import pytest

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "cloudformation-templates"
TEMPLATE = json.loads((TEMPLATES_DIR / "nx-neptune-sagemaker.json").read_text())


def _render_oncreate_heredoc(refs=None):
    """Return the OnCreate heredoc body as a runnable shell script.

    Joins the ``Fn::Join`` parts, resolving ``{"Ref": name}`` from ``refs`` (any
    other intrinsic becomes a harmless placeholder), then extracts the text
    inside the ``<<'EOF'`` heredoc.
    """
    refs = refs or {}
    parts = TEMPLATE["Resources"]["NotebookLifecycleConfig"]["Properties"][
        "OnCreate"
    ][0]["Content"]["Fn::Base64"]["Fn::Join"][1]
    rendered = "".join(
        p if isinstance(p, str) else refs.get(p.get("Ref"), "PLACEHOLDER")
        for p in parts
    )
    marker = "<<'EOF'\n"
    start = rendered.index(marker) + len(marker)
    end = rendered.index("\nEOF", start)
    return rendered[start:end]


def _make_stub(path, body):
    path.write_text("#!/bin/bash\n" + body)
    path.chmod(0o755)


def _run_oncreate(tmp_path, *, wheel_install_fails, custom_notebooks):
    """Render and run the OnCreate body with stubbed commands.

    Hermetic: every external command is stubbed (no network), and the
    hard-coded ``/tmp`` and ``/home/ec2-user`` paths are rewritten to throwaway
    locations so the body runs unprivileged and never touches the real /tmp.
    A wheel is pre-staged so the body takes its wheel-install branch;
    ``custom_notebooks`` selects the bundled-zip branch (what deploy.sh passes)
    or the GitHub-download branch.
    """
    scratch = tmp_path / "tmp"
    home = tmp_path / "home"
    (home / "anaconda3" / "bin").mkdir(parents=True)
    (home / "anaconda3" / "bin" / "activate").write_text("")  # sourced, no-op
    assets = scratch / "nx-assets"
    assets.mkdir(parents=True)
    (assets / "nx_neptune.whl").write_text("")  # makes `ls *.whl` succeed
    (assets / "notebooks.zip").write_text("")  # bundled notebooks branch -> true

    body = _render_oncreate_heredoc(
        {"CustomNotebooks": "true" if custom_notebooks else "false"}
    )
    # Rewrite /tmp before /home: tmp_path itself may live under /tmp.
    body = body.replace("/tmp", str(scratch))
    body = body.replace("/home/ec2-user", str(home))
    script = tmp_path / "oncreate.sh"
    script.write_text(body)

    stubs = tmp_path / "stubs"
    stubs.mkdir()
    for cmd in ("conda", "python", "unzip", "aws", "sha256sum"):
        _make_stub(stubs / cmd, "exit 0\n")
    # curl writes a fake file to its -o target; SHA256SUMS gets a matching line.
    _make_stub(
        stubs / "curl",
        'while [ $# -gt 0 ]; do [ "$1" = -o ] && out=$2; shift; done\n'
        'case "$out" in *SHA256SUMS) echo "0  notebooks.zip" > "$out";; '
        '*) : > "$out";; esac\n',
    )
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


@pytest.mark.parametrize("custom_notebooks", [True, False])
def test_failed_wheel_install_aborts_oncreate(tmp_path, custom_notebooks):
    proc = _run_oncreate(
        tmp_path, wheel_install_fails=True, custom_notebooks=custom_notebooks
    )
    assert proc.returncode != 0, (
        "A failed wheel pip install must abort OnCreate; it exited 0, meaning "
        "SageMaker would report a half-provisioned notebook as successful.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )


@pytest.mark.parametrize("custom_notebooks", [True, False])
def test_oncreate_succeeds_when_steps_pass(tmp_path, custom_notebooks):
    # Positive control: the in-heredoc `set -e` must not break the happy path.
    proc = _run_oncreate(
        tmp_path, wheel_install_fails=False, custom_notebooks=custom_notebooks
    )
    assert proc.returncode == 0, (
        f"OnCreate should exit 0 when all steps succeed.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
