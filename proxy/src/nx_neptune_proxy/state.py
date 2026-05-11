from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ProxyState:
    """Mutable singleton holding pipeline state."""

    graph_endpoint: Optional[str] = None
    graph_name: Optional[str] = None
    graph_id: Optional[str] = None
    job_id: Optional[str] = None
    status: str = "idle"  # idle | running | complete | failed
    step: Optional[str] = None
    step_label: Optional[str] = None
    progress: float = 0
    error: Optional[str] = None


proxy_state = ProxyState()
