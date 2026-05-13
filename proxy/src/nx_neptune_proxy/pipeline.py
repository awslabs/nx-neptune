import asyncio
import logging
import uuid

from nx_neptune_proxy.state import GraphState, graphs, GRAPH_PREFIX

logger = logging.getLogger(__name__)


async def run_pipeline(config: dict) -> None:
    """Execute the full migration pipeline: create graph → Athena → import."""
    from nx_neptune.session_manager import SessionManager

    job_id = str(uuid.uuid4())
    graph_name = f"{GRAPH_PREFIX}{config['graphName']}"
    s3_location = config["csvStagingBucket"].rstrip("/") + f"/{job_id}/"
    state = GraphState(job_id=job_id, graph_name=graph_name, sql_query=config["sqlQuery"])
    graphs[job_id] = state

    try:
        # Ensure boto3 uses the requested region
        import os
        os.environ["AWS_DEFAULT_REGION"] = config.get("region", "us-west-2")

        # Step 1: Create graph
        _update(state, step="graph_creation", label="Creating Neptune Analytics graph", progress=5)
        sm = SessionManager(session_name=graph_name)
        graph = await sm.get_or_create_graph(
            config={"provisionedMemory": config.get("graphMemoryGb", 16)}
        )
        state.graph_id = graph.graph_id
        _update(state, step="graph_creation", label="Graph available", progress=40)

        # Step 2: Athena query + CSV import
        _update(state, step="athena_import", label="Running Athena query and importing data", progress=45)
        sql_queries = [q.strip() for q in config["sqlQuery"].split(";") if q.strip()]
        await sm.import_from_table(
            graph=graph,
            s3_location=s3_location,
            sql_queries=sql_queries,
            catalog=config.get("athenaCatalog"),
            database=config.get("athenaDatabase"),
            remove_buckets=True,
        )
        _update(state, step="athena_import", label="Import complete", progress=90)

        # Step 3: Resolve endpoint and mark ready
        state.graph_endpoint = f"https://{graph.graph_id}.neptune-graph.amazonaws.com"
        _update(state, step="ready", label="Graph ready", progress=100)
        state.status = "complete"

    except Exception as e:
        logger.exception("Pipeline failed")
        state.status = "failed"
        state.error = str(e)


def _update(state: GraphState, step: str, label: str, progress: float) -> None:
    state.step = step
    state.step_label = label
    state.progress = progress
