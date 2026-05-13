from dataclasses import dataclass, field
from typing import Optional

GRAPH_PREFIX = "nxp-"


@dataclass
class GraphState:
    """State for a single graph pipeline."""

    job_id: str
    graph_name: str
    graph_id: Optional[str] = None
    graph_endpoint: Optional[str] = None
    sql_query: Optional[str] = None
    status: str = "running"  # running | complete | failed
    step: Optional[str] = None
    step_label: Optional[str] = None
    progress: float = 0
    error: Optional[str] = None


# All tracked graphs keyed by job_id
graphs: dict[str, GraphState] = {}
