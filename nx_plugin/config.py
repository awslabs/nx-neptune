from networkx.utils.configs import Config
from dataclasses import dataclass

__all__ = [
    "_config",
]


@dataclass
class NeptuneConfig(Config):
    active: bool = False
    backend: str = "neptune"
    verbose: int = 0
    prefer: bool = False
    require: bool = False


_config = NeptuneConfig()
