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

All of the following must pass on `main`:

- **Lint + unit tests** (`main.yml`) — runs `make lint` and `make test`
- **Lock file check** (`main.yml`) — verifies `requirements.txt` and `requirements-dev.txt` are in sync with `pyproject.toml`
- **License check** (`main.yml`) — validates dependency licenses
- **Connector builds** (`athena-databricks-connector.yml`, `athena-s3vector-connector.yml`) — Maven build on Java 11 and 17

### 2. Run integration tests

Integration tests require a live Neptune Analytics instance and are not run in CI. Run them manually before each release. See [Integration tests](#integration-tests) below.

### 3. Update the version

The version is defined in `nx_neptune/__init__.py`:

```python
__version__ = "0.5.0"
```

Update this to the new version number.

### 4. Update lock files

If any dependencies changed:

```bash
make lock
```

> `make lock` must be run with Python 3.11 to match CI. See [README.md](README.md) for details.

### 5. Create the release

```bash
make release
```

This will:
1. Prompt for the version number
2. Write it to `nx_neptune/VERSION`
3. Commit and tag
4. Push the tag to GitHub

GitHub Actions will then:
- **Prerelease** (`python-prerelease.yml`): Triggered by `prereleased` events. Builds the wheel and uploads to GitHub Releases.
- **Release** (`python-publish.yml`): Triggered by `released` events. Builds the wheel and publishes to PyPI via trusted publishing.

### 6. Connector releases

The Athena connectors are deployed separately as Lambda functions via SAM. They are not published to PyPI. Build and deploy with:

```bash
# Databricks connector
cd connectors/athena-databricks-connector
mvn clean install -Dcheckstyle.skip=true

# S3Vector connector
cd connectors/athena-s3vector-connector
mvn clean install
```

Refer to each connector's README for SAM deployment instructions.

---

## Integration tests

Integration tests live in `integ_test/` and are designed to be run incrementally — each tier adds more coverage as you provide more AWS resources. Tests auto-skip if the required environment variables are not set.

### Tiers

| Tier | What you provide | What you can test |
|---|---|---|
| Tier 1 | `NETWORKX_GRAPH_ID` | NeptuneGraph CRUD, SessionManager read ops, algorithms, security |
| Tier 2 | Tier 1 + `NETWORKX_S3_EXPORT_BUCKET_PATH` | S3 export/import, snapshots, IAM permission checks |
| Tier 3 | Tier 2 + IAM role with create/delete permissions | Instance create/delete/start/stop, SessionManager lifecycle, context manager cleanup |

### Environment variables

```bash
# Tier 1 — just a graph ID
export NETWORKX_GRAPH_ID=g-your-graph-id

# Tier 2 — add an S3 bucket (must have KMS encryption + versioning enabled)
export NETWORKX_S3_EXPORT_BUCKET_PATH=s3://your-bucket/path/

# Tier 3 — no additional env vars, but IAM role must have neptune-graph:Create*, Delete*, Start*, Stop* permissions
```

### Running tests

```bash
# Tier 1 only (~1 min, requires NETWORKX_GRAPH_ID)
pytest integ_test/tier1_graph/ -v

# Tier 2 only (~5 min, requires NETWORKX_GRAPH_ID + NETWORKX_S3_EXPORT_BUCKET_PATH)
pytest integ_test/tier2_export_import/ -v -s

# Tier 3 only (~15 min, creates/destroys real instances)
pytest integ_test/tier3_lifecycle/ -v -s

# All integration tests
make integ-test
```

### What is covered

| Test suite | Directory | Tier | What it covers |
|---|---|---|---|
| Algorithms | `integ_test/test_algo_*.py` | 1 | PageRank, closeness, degree, Louvain, LPA, BFS |
| Security | `integ_test/test_security_*.py` | 1 | Injection prevention, graph reset, S3 versioning |
| Graph CRUD | `integ_test/tier1_graph/` | 1 | add/update/delete nodes & edges, clear_graph, execute_call, SessionManager list/get graphs, validate_permissions |
| S3 Import/Export | `integ_test/tier2_export_import/test_s3_import_export.py` | 2 | export_csv_to_s3, export with filter, round-trip import, empty_s3_bucket |
| Snapshots | `integ_test/tier2_export_import/test_snapshot.py` | 2 | create_graph_snapshot, delete_graph_snapshot |
| IAM Permissions | `integ_test/tier2_export_import/test_iam_permissions.py` | 2 | has_import/export/delete_s3_permissions, check_s3_versioning, S3 ARN parsing |
| Instance Lifecycle | `integ_test/tier3_lifecycle/test_instance_lifecycle.py` | 3 | create/delete instance, snapshot→restore→cleanup, stop→start→delete |
| SessionManager Lifecycle | `integ_test/tier3_lifecycle/test_session_manager_lifecycle.py` | 3 | get_or_create_graph, create_from_csv, create_multiple_instances + destroy_all, context manager with DESTROY cleanup |

### Resource cleanup

A session-scoped `ResourceTracker` in `integ_test/conftest.py` records AWS resources created during the test run. At session teardown, any resources not cleaned up by individual tests are deleted automatically.

If you suspect leaked resources after a failed run:

```bash
aws neptune-graph list-graphs --query 'graphs[].{id:id,name:name,status:status}'
```
