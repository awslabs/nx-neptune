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
        "verbose": 0,
        "prefer": False,
        "require": False,
        "graph_id": "",
    }

    from nx_plugin.config import _config

    assert nx.config.backends.neptune == _config
