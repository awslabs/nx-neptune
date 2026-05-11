# Release Guide

## Components

This repository contains three independently released components:

| Component | Language | Artifact | Release mechanism |
|---|---|---|---|
| `nx_neptune` | Python | [PyPI](https://pypi.org/project/nx-neptune/) | GitHub Release → `python-publish.yml` |
| Athena Databricks Connector | Java | SAM / Lambda | Manual deploy from `connectors/athena-databricks-connector/` |
| Athena S3Vector Connector | Java | SAM / Lambda | Manual deploy from `connectors/athena-s3vector-connector/` |

## Pre-release checklist

### 1. Ensure CI is green

All of the following must pass on [`main`](https://github.com/awslabs/nx-neptune/tree/main):

- **Lint + unit tests** (`main.yml`) — runs `make lint` and `make test`
- **Lock file check** (`main.yml`) — verifies `requirements.txt` and `requirements-dev.txt` are in sync with `pyproject.toml`
- **License check** (`main.yml`) — validates dependency licenses
- **Connector builds** (`athena-databricks-connector.yml`, `athena-s3vector-connector.yml`) — Maven build on Java 11 and 17

### 2. Run integration tests

Integration tests require a live Neptune Analytics instance and are not run in CI. Run them manually before each release. See [Integration tests](#integration-tests) below.

### 3. Update the version

The Python module version is defined in `nx_neptune/__init__.py`:

```python
__version__ = "0.5.0"
```

The connector versions are defined in their respective `pom.xml` files:

- `connectors/pom.xml` (parent)
- `connectors/athena-databricks-connector/pom.xml`
- `connectors/athena-s3vector-connector/pom.xml`

```xml
<version>0.7.0</version>
```

Update all to the new version number.

### 4. Update lock files

If any dependencies changed:

```bash
make lock
```

> `make lock` must be run with Python 3.11 to match CI. See [README.md](README.md) for details.

### 5. Connector releases

The Athena connectors are deployed separately as Lambda functions via SAM. They are not published to PyPI. Build and deploy with:

```bash
# Databricks connector
cd connectors/athena-databricks-connector
mvn clean install

# S3Vector connector
cd connectors/athena-s3vector-connector
mvn clean install
```

Refer to each connector's README for SAM deployment instructions.

---

## Integration tests

Integration tests live in `integ_test/` and are organized into two directories. Tests auto-skip if the required environment variables are not set.

### Structure

| Directory | What it tests | Env vars required |
|---|---|---|
| `integ_test/graph_operations/` | CRUD, algorithms, security — operations within an existing graph | `NETWORKX_GRAPH_ID` |
| `integ_test/session_manager/` | Read ops, S3 export/import, snapshots, IAM checks, instance lifecycle, fleet management | `NETWORKX_GRAPH_ID` (+ `NETWORKX_S3_EXPORT_BUCKET_PATH` for S3 tests) |

### Environment variables

```bash
# Required for all tests — an existing Neptune Analytics graph
export NETWORKX_GRAPH_ID=g-your-graph-id

# Required for S3 export/import and IAM tests (must have KMS encryption + versioning enabled)
export NETWORKX_S3_EXPORT_BUCKET_PATH=s3://your-bucket/path/
```

Lifecycle tests (instance create/delete/start/stop) also require the IAM role to have `neptune-graph:Create*`, `Delete*`, `Start*`, `Stop*` permissions.

### Running tests

**Graph operations** — Run for any changes to CRUD, algorithms, or query logic. Fast and safe; operates on an existing graph only.
```bash
# ~1 min, requires NETWORKX_GRAPH_ID
pytest integ_test/graph_operations/ -v
```

**Session manager** — Run for changes to SessionManager, S3 import/export, snapshots, IAM checks, or instance lifecycle. Lifecycle tests create and destroy real instances, so expect longer runtimes.
```bash
# ~1-1.5 hours, requires NETWORKX_GRAPH_ID (+ NETWORKX_S3_EXPORT_BUCKET_PATH for S3 tests)
pytest integ_test/session_manager/ -v -s
```

**All integration tests** — Run before a release or when changes span both graph operations and session management.
```bash
make integ-test
```

### What is covered

| Test suite | Directory | What it covers |
|---|---|---|
| Graph CRUD | `integ_test/graph_operations/test_graph_crud.py` | add/update/delete nodes & edges, clear_graph, execute_call |
| Algorithms | `integ_test/graph_operations/test_algo_*.py` | PageRank, closeness, degree, Louvain, LPA, BFS |
| Security | `integ_test/graph_operations/test_security_*.py` | Injection prevention, graph reset, S3 versioning |
| SessionManager Read | `integ_test/session_manager/test_session_manager_read.py` | list/get graphs, session name filtering, validate_permissions |
| S3 Import/Export | `integ_test/session_manager/test_s3_import_export.py` | export_csv_to_s3, export with filter, round-trip import, empty_s3_bucket |
| Snapshots | `integ_test/session_manager/test_snapshot.py` | create_graph_snapshot, delete_graph_snapshot |
| IAM Permissions | `integ_test/session_manager/test_iam_permissions.py` | has_import/export/delete_s3_permissions, check_s3_versioning, S3 ARN parsing |
| Instance Lifecycle | `integ_test/session_manager/test_instance_lifecycle.py` | create/delete instance, snapshot→restore→cleanup, stop→start→delete |
| SessionManager Lifecycle | `integ_test/session_manager/test_session_manager_lifecycle.py` | get_or_create_graph, create_from_csv, create_multiple_instances + destroy_all, context manager with DESTROY cleanup |

### Resource cleanup

A session-scoped `ResourceTracker` in `integ_test/conftest.py` records AWS resources created during the test run. At session teardown, any resources not cleaned up by individual tests are deleted automatically.

If you suspect leaked resources after a failed run:

```bash
aws neptune-graph list-graphs --query 'graphs[].{id:id,name:name,status:status}'
```
