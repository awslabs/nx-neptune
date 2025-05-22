import pytest
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
    }

    from nx_plugin.config import _config

    assert nx.config.backends.neptune == _config
