from pathlib import Path
import logging

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional
import asyncio

from nx_neptune_proxy.state import graphs, GRAPH_PREFIX

logging.basicConfig(level=logging.INFO)
logging.getLogger("nx_neptune").setLevel(logging.DEBUG)

app = FastAPI(title="nx-neptune-proxy", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class SetupRequest(BaseModel):
    region: str
    athenaDatabase: str
    sqlQuery: str
    csvStagingBucket: str
    graphName: str
    graphMemoryGb: int = 16
    athenaCatalog: Optional[str] = None


class QueryRequest(BaseModel):
    query: str
    graphId: Optional[str] = None


# --- Datasource info ---


@app.get("/api/v0/datasource/info")
def get_info():
    return {
        "name": "nx-neptune",
        "version": "0.1.0",
        "queryLanguages": ["openCypher"],
        "capabilities": ["rawQuery", "setup"],
    }


@app.get("/api/v0/datasource/ready")
def get_ready():
    ready_graphs = [g for g in graphs.values() if g.status == "complete"]
    if ready_graphs:
        latest = ready_graphs[-1]
        return {
            "ready": True,
            "message": f"Neptune Analytics graph '{latest.graph_name}' is available",
            "details": {"graphEndpoint": latest.graph_endpoint, "graphId": latest.graph_id},
        }
    return {"ready": False, "message": "No graph provisioned yet"}


# --- Setup + status ---


@app.post("/api/v0/datasource/setup", status_code=202)
def post_setup(req: SetupRequest, background_tasks: BackgroundTasks):
    from nx_neptune_proxy.pipeline import run_pipeline

    background_tasks.add_task(asyncio.run, run_pipeline(req.model_dump()))
    return {"status": "accepted"}


@app.get("/api/v0/datasource/setup/status")
def get_setup_status(job_id: Optional[str] = None):
    if job_id and job_id in graphs:
        state = graphs[job_id]
    elif graphs:
        state = list(graphs.values())[-1]
    else:
        return {"jobId": None, "status": "idle", "step": None, "stepLabel": None, "progress": 0}

    resp = {
        "jobId": state.job_id,
        "status": state.status,
        "step": state.step,
        "stepLabel": state.step_label,
        "progress": state.progress,
    }
    if state.graph_endpoint:
        resp["graphEndpoint"] = state.graph_endpoint
    if state.error:
        resp["error"] = state.error
    return resp


# --- Validation ---


@app.post("/api/v0/datasource/test")
def post_test(req: SetupRequest):
    from nx_neptune.validators import validate_resources

    checks = validate_resources(
        region=req.region,
        s3_staging_bucket=req.csvStagingBucket,
        athena_catalog=req.athenaCatalog or "AwsDataCatalog",
        athena_database=req.athenaDatabase,
        sql_query=req.sqlQuery,
        graph_name=f"{GRAPH_PREFIX}{req.graphName}",
    )
    return {"valid": all(c["passed"] for c in checks), "checks": checks}


@app.post("/api/v0/datasource/test/query")
def post_test_query(req: SetupRequest):
    from nx_neptune.validators import check_athena_query

    result = check_athena_query(
        sql_query=req.sqlQuery,
        catalog=req.athenaCatalog or "AwsDataCatalog",
        database=req.athenaDatabase,
        output_location=req.csvStagingBucket,
        region=req.region,
    )
    return {"valid": result.passed, "checks": [result.to_dict()]}


# --- Graphs management ---


@app.get("/api/v0/graphs")
def get_graphs(region: str = "us-west-2"):
    import boto3

    client = boto3.client("neptune-graph", region_name=region)
    resp = client.list_graphs()
    result = []
    for g in resp.get("graphs", []):
        if g["name"].startswith(GRAPH_PREFIX):
            entry = {
                "id": g["id"],
                "name": g["name"],
                "status": g["status"],
                "endpoint": g.get("endpoint"),
                "memory": g.get("provisionedMemory"),
                "importStatus": None,
                "importStep": None,
                "importError": None,
            }
            # Merge local pipeline state if available
            for state in graphs.values():
                if state.graph_id == g["id"]:
                    entry["importStatus"] = state.status
                    entry["importStep"] = state.step_label
                    entry["importError"] = state.error
                    break
            result.append(entry)
    return result


@app.delete("/api/v0/graphs/{graph_id}")
def delete_graph(graph_id: str, region: str = "us-west-2"):
    import boto3

    client = boto3.client("neptune-graph", region_name=region)
    client.delete_graph(graphIdentifier=graph_id, skipSnapshot=True)
    # Remove from local state if tracked
    to_remove = [k for k, v in graphs.items() if v.graph_id == graph_id]
    for k in to_remove:
        del graphs[k]
    return {"status": "deleting", "graphId": graph_id}


# --- Query ---


@app.post("/api/v0/query")
def post_query(req: QueryRequest):
    graph_id = req.graphId
    if not graph_id:
        # Use latest complete graph
        ready = [g for g in graphs.values() if g.status == "complete"]
        if not ready:
            raise HTTPException(status_code=503, detail="No graph ready")
        graph_id = ready[-1].graph_id

    import boto3
    import json

    client = boto3.client("neptune-graph")
    response = client.execute_query(
        graphIdentifier=graph_id,
        queryString=req.query,
        language="OPEN_CYPHER",
    )
    payload = json.loads(response["payload"].read())
    return {"results": payload}


# --- Athena metadata ---


@app.get("/api/v0/athena/databases")
def get_athena_databases(region: str, catalog: str = "AwsDataCatalog"):
    import boto3

    athena = boto3.client("athena", region_name=region)
    resp = athena.list_databases(CatalogName=catalog)
    return [db["Name"] for db in resp.get("DatabaseList", [])]


@app.get("/api/v0/athena/tables")
def get_athena_tables(region: str, database: str, catalog: str = "AwsDataCatalog"):
    import boto3

    athena = boto3.client("athena", region_name=region)
    resp = athena.list_table_metadata(CatalogName=catalog, DatabaseName=database)
    return [t["Name"] for t in resp.get("TableMetadataList", [])]


@app.get("/api/v0/athena/columns")
def get_athena_columns(region: str, database: str, table: str, catalog: str = "AwsDataCatalog"):
    import boto3

    athena = boto3.client("athena", region_name=region)
    resp = athena.get_table_metadata(CatalogName=catalog, DatabaseName=database, TableName=table)
    columns = resp["TableMetadata"].get("Columns", [])
    return [{"name": c["Name"], "type": c["Type"]} for c in columns]


# --- Static UI (must be last) ---

UI_DIR = Path(__file__).parent.parent.parent / "ui"
if UI_DIR.exists():
    app.mount("/", StaticFiles(directory=UI_DIR, html=True), name="ui")
