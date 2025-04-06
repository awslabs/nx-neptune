import os
import networkx
from dataclasses import asdict
from functools import wraps

__all__ = ["configure_if_nx_active"]


def configure_if_nx_active():
    """
    Decorator to set the configuration for the connection to Neptune Analytics within nx_neptune.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            backends = networkx.config.backends
            if backends.neptune.active or "PYTEST_CURRENT_TEST" in os.environ:
                # Activate nx config system in nx_neptune with:
                # `nx.config.backends.neptune.active = True`
                # config_dict = asdict(networkx.config.backends.neptune)
                # config_dict.update(config_dict.pop("backend_params"))
                # config_dict.update("active", True)
                # with neptune_config(**config_dict):
                return func(*args, **kwargs)
            return func(*args, **kwargs)

        return wrapper

    return decorator
