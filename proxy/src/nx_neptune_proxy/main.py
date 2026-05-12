from pathlib import Path
import logging

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional
import asyncio

from nx_neptune_proxy.state import proxy_state

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
    if proxy_state.graph_endpoint:
        return {
            "ready": True,
            "message": f"Neptune Analytics graph '{proxy_state.graph_name}' is available",
            "details": {"graphEndpoint": proxy_state.graph_endpoint},
        }
    return {"ready": False, "message": "No graph provisioned yet"}


@app.post("/api/v0/datasource/setup", status_code=202)
def post_setup(req: SetupRequest, background_tasks: BackgroundTasks):
    if proxy_state.status == "running":
        raise HTTPException(status_code=409, detail="Setup already running")

    from nx_neptune_proxy.pipeline import run_pipeline

    background_tasks.add_task(asyncio.run, run_pipeline(req.model_dump()))
    return {"jobId": proxy_state.job_id or "pending", "status": "accepted"}


@app.post("/api/v0/datasource/test")
def post_test(req: SetupRequest):
    from nx_neptune.validators import validate_resources

    checks = validate_resources(
        region=req.region,
        s3_staging_bucket=req.csvStagingBucket,
        athena_catalog=req.athenaCatalog or "AwsDataCatalog",
        athena_database=req.athenaDatabase,
        graph_name=req.graphName,
    )
    return {"valid": all(c["passed"] for c in checks), "checks": checks}


@app.get("/api/v0/datasource/setup/status")
def get_setup_status():
    resp = {
        "jobId": proxy_state.job_id,
        "status": proxy_state.status,
        "step": proxy_state.step,
        "stepLabel": proxy_state.step_label,
        "progress": proxy_state.progress,
    }
    if proxy_state.graph_endpoint:
        resp["graphEndpoint"] = proxy_state.graph_endpoint
    if proxy_state.error:
        resp["error"] = proxy_state.error
    return resp


class QueryRequest(BaseModel):
    query: str


@app.post("/api/v0/query")
def post_query(req: QueryRequest):
    if not proxy_state.graph_id:
        raise HTTPException(status_code=503, detail="Graph not ready")

    import boto3

    client = boto3.client("neptune-graph")
    response = client.execute_query(
        graphIdentifier=proxy_state.graph_id,
        queryString=req.query,
        language="OPEN_CYPHER",
    )
    import json

    payload = json.loads(response["payload"].read())
    return {"results": payload}


UI_DIR = Path(__file__).parent.parent.parent / "ui"
if UI_DIR.exists():
    app.mount("/", StaticFiles(directory=UI_DIR, html=True), name="ui")
