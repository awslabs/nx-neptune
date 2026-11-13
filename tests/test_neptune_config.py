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
"""Tests for the pipeline completion action rules on NeptuneConfig."""

import itertools

import pytest

# Import the backend package first, as tests/test_base.py does. Importing
# nx_plugin on its own first leaves networkx's backend config unregistered,
# which makes other modules (test_base.test_config_init) see an empty config.
import nx_neptune  # noqa: F401  isort: skip
from nx_plugin import NeptuneConfig  # isort: skip

ACTIONS = ["destroy_instance", "stop_instance", "reset_graph"]


def _config(**overrides):
    return NeptuneConfig(graph_id="g-12345", **overrides)


class TestDefaultCompletionAction:
    def test_default_selects_no_action_and_validates(self):
        config = _config()

        config.validate_config()

        assert config.destroy_instance is False
        assert config.reset_graph is False
        assert config.stop_instance is False

    @pytest.mark.parametrize("action", ACTIONS)
    def test_single_action_validates(self, action):
        _config(**{action: True}).validate_config()

    def test_stop_without_export_validates(self):
        _config(stop_instance=True).validate_config()


class TestMutualExclusion:
    @pytest.mark.parametrize(
        "selected",
        [pair for n in (2, 3) for pair in itertools.combinations(ACTIONS, n)],
    )
    def test_more_than_one_action_is_rejected(self, selected):
        config = _config(**{name: True for name in selected})

        with pytest.raises(ValueError, match="only one completion action"):
            config.validate_config()


class TestLegacyDestroy:
    def test_destroy_instance_alone_keeps_working(self):
        config = _config(destroy_instance=True)

        config.validate_config()

        assert config.destroy_instance is True
        assert config.reset_graph is False


class TestMutateExecutionConfig:
    def test_reset_without_export_is_rejected(self):
        config = _config(reset_graph=True)

        with pytest.raises(ValueError, match="reset"):
            config.validate_mutate_execution_config()

    def test_reset_with_export_is_allowed(self):
        _config(
            reset_graph=True, export_s3_bucket="bucket", s3_iam_role="arn:role"
        ).validate_mutate_execution_config()

    def test_stop_without_export_is_allowed(self):
        _config(stop_instance=True).validate_mutate_execution_config()

    def test_destroy_without_export_is_still_rejected(self):
        with pytest.raises(ValueError, match="destroy_instance"):
            _config(destroy_instance=True).validate_mutate_execution_config()

    def test_default_without_export_is_allowed(self):
        _config().validate_mutate_execution_config()
