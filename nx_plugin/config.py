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
    graph_id: str = ""
    s3_import_path: str = ""
    create_instance: bool = False
    role_arn: str = ""


_config = NeptuneConfig()
