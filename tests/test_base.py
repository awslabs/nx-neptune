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

import nx_plugin
from nx_neptune import NeptuneGraph
from importlib.metadata import entry_points, EntryPoint


def test_base():
    assert NeptuneGraph.NAME == "nx_neptune"


def test_backends_ep():
    assert entry_points(group="networkx.backends")["neptune"] == EntryPoint(
        name="neptune",
        value="nx_neptune.interface:BackendInterface",
        group="networkx.backends",
    )


def test_backend_info_ep():
    assert entry_points(group="networkx.backend_info")["neptune"] == EntryPoint(
        name="neptune", value="nx_plugin:get_info", group="networkx.backend_info"
    )


@pytest.mark.order(1)
def test_config_init():
    import networkx as nx

    assert dict(nx.config.backends.neptune) == {
        "active": False,
        "backend": "neptune",
        "create_new_instance": False,
        "destroy_instance": False,
        "export_s3_bucket": None,
        "graph_id": None,
        "import_s3_bucket": None,
        "prefer": False,
        "require": False,
        "reset_graph": False,
        "restore_snapshot": None,
        "s3_iam_role": None,
        "save_snapshot": False,
        "skip_graph_reset": False,
        "verbose": 0,
        "batch_update_node_size": 20000,
        "batch_update_edge_size": 10000,
    }

    from nx_plugin.config import _config

    assert nx.config.backends.neptune == _config
