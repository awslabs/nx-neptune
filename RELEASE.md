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

Integration tests live in `integ_test/` and require a live Neptune Analytics graph instance. They are not run in CI and must be executed manually before each release.

### Prerequisites

Set the following environment variables:

```bash
export NETWORKX_GRAPH_ID=g-your-graph-id
export NETWORKX_S3_EXPORT_BUCKET_PATH=s3://your-bucket/path/  # for tier 2 (S3 bucket must have KMS + versioning enabled)
```

### Running tests

```bash
# Run tier 1 only (NeptuneGraph CRUD + SessionManager read ops, ~1 min)
pytest integ_test/tier1_graph/ -v

# Run tier 2 only (S3 import/export + IAM permissions, ~3 min)
pytest integ_test/tier2_s3/ -v -s

# Run all integration tests (algorithms + security + tier 1 + tier 2)
make integ-test
```

### What is covered

| Test suite | Directory | What it covers |
|---|---|---|
| Algorithms | `integ_test/test_algo_*.py` | PageRank, closeness, degree, Louvain, LPA, BFS |
| Security | `integ_test/test_security_*.py` | Injection prevention, graph reset, S3 versioning |
| Tier 1 — Graph CRUD | `integ_test/tier1_graph/` | add/update/delete nodes & edges, clear_graph, execute_call, SessionManager list/get graphs, validate_permissions |
| Tier 2 — S3 Import/Export | `integ_test/tier2_s3/test_s3_import_export.py` | export_csv_to_s3, export with filter, round-trip import, empty_s3_bucket |
| Tier 2 — IAM Permissions | `integ_test/tier2_s3/test_iam_permissions.py` | has_import/export/delete_s3_permissions, check_s3_versioning, S3 ARN parsing |

### Resource cleanup

A session-scoped `ResourceTracker` in `integ_test/conftest.py` records AWS resources created during the test run. At session teardown, any resources not cleaned up by individual tests are deleted automatically.

If you suspect leaked resources after a failed run:

```bash
aws neptune-graph list-graphs --query 'graphs[].{id:id,name:name,status:status}'
```
