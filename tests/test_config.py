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
import pytest

from nx_plugin import NeptuneConfig


def test_destroy_instance_with_graph_id_is_rejected():
    """A graph provided through graph_id is never destroyed."""
    config = NeptuneConfig(graph_id="g-123", destroy_instance=True)

    with pytest.raises(ValueError, match="destroy_instance is only supported"):
        config.validate_config()


@pytest.mark.parametrize(
    "settings",
    [
        {"create_new_instance": True, "destroy_instance": True},
        {"graph_id": "g-123", "destroy_instance": False},
    ],
    ids=["created_graph_destroyed", "provided_graph_kept"],
)
def test_destroy_instance_allowed_configs(settings):
    """Destroy is allowed for a graph the decorator creates; a provided graph is only used."""
    NeptuneConfig(**settings).validate_config()
