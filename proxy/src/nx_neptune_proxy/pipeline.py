import asyncio
import logging
import uuid

from nx_neptune_proxy.state import proxy_state

logger = logging.getLogger(__name__)


async def run_pipeline(config: dict) -> None:
    """Execute the full migration pipeline: create graph → Athena → import."""
    from nx_neptune.session_manager import SessionManager

    proxy_state.job_id = str(uuid.uuid4())
    proxy_state.status = "running"
    proxy_state.error = None

    try:
        # Step 1: Create graph
        _update(step="graph_creation", label="Creating Neptune Analytics graph", progress=5)
        sm = SessionManager(session_name=config["graphName"])
        graph = await sm.get_or_create_graph(
            config={"graph_memory": config.get("graphMemoryGb", 16)}
        )
        proxy_state.graph_id = graph.graph_id
        _update(step="graph_creation", label="Graph available", progress=40)

        # Step 2: Athena query + CSV import
        _update(step="athena_import", label="Running Athena query and importing data", progress=45)
        await sm.import_from_table(
            graph=graph,
            s3_location=config["csvStagingBucket"],
            sql_queries=[config["sqlQuery"]],
            catalog=config.get("athenaCatalog"),
            database=config.get("athenaDatabase"),
            remove_buckets=True,
        )
        _update(step="athena_import", label="Import complete", progress=90)

        # Step 3: Resolve endpoint and mark ready
        endpoint = f"https://{graph.graph_id}.neptune-graph.amazonaws.com"
        proxy_state.graph_endpoint = endpoint
        proxy_state.graph_name = config["graphName"]
        _update(step="ready", label="Graph ready", progress=100)
        proxy_state.status = "complete"

    except Exception as e:
        logger.exception("Pipeline failed")
        proxy_state.status = "failed"
        proxy_state.error = str(e)


def _update(step: str, label: str, progress: float) -> None:
    proxy_state.step = step
    proxy_state.step_label = label
    proxy_state.progress = progress
