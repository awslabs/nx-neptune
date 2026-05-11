
---
# nx_neptune

[![CI](https://github.com/awslabs/nx-neptune/actions/workflows/main.yml/badge.svg)](https://github.com/awslabs/nx-neptune/actions/workflows/main.yml)
[![Upload Python Package](https://github.com/awslabs/nx-neptune/actions/workflows/python-publish.yml/badge.svg)](https://github.com/awslabs/nx-neptune/actions/workflows/python-publish.yml)

📖 **[Documentation](https://awslabs.github.io/nx-neptune/)**

**nx-neptune** is a Python library that brings graph analytics to your data lake. Project your data from Amazon S3 Tables, S3 Vectors, Databricks, Snowflake, OpenSearch, and other sources into [Neptune Analytics](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html) for graph analysis — with results exported back to S3 or persisted as Iceberg tables.

### Graph Over Data Lake

Run graph algorithms on data that lives in your data lake — without moving it permanently into a graph database. nx-neptune projects your data into a Neptune Analytics graph using SQL queries (via Amazon Athena), so you can run graph algorithms, explore relationships with openCypher queries, and visualize connections that are invisible in tabular form. When you're done, export the results back to S3 or destroy the graph.

What previously required complex ETL pipelines to get data into a graph database is now a streamlined workflow: define your Athena query, give the module the right permissions, and it handles the projection for you.

**Supported data sources:**

- **Amazon S3 Tables** — query Iceberg tables via Athena SQL ([demo](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_s3_table_demo.ipynb))
- **Amazon S3 Vectors** — project vector embeddings via a custom Athena connector ([connector](https://github.com/awslabs/nx-neptune/blob/main/connectors/athena-s3vector-connector/), [demo](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_s3_vector_embedding_demo.ipynb))
- **Databricks Unity Catalog** — query Databricks tables via a JDBC-based Athena connector ([connector](https://github.com/awslabs/nx-neptune/blob/main/connectors/athena-databricks-connector/), [demo](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_databricks_demo.ipynb))
- **Snowflake** — query tables via the Athena Snowflake connector ([demo](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_snowflake_table_demo.ipynb))
- **Amazon OpenSearch** — project embeddings from OpenSearch indices ([demo](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_open_search_embedding_demo.ipynb))
- Any source accessible through [Athena federated queries](https://docs.aws.amazon.com/athena/latest/ug/connect-to-a-data-source.html) — [25+ connectors available](https://docs.aws.amazon.com/athena/latest/ug/connectors-available.html)

For data already in S3-compatible formats (CSV, Parquet), Neptune Analytics also supports [native S3 import](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/import-s3.html) without Athena.

**Use cases demonstrated in the notebooks:**

- **Fraud detection** — project financial transactions as a graph, run community detection (Louvain) to identify fraud rings ([S3 Tables demo](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_s3_table_demo.ipynb), [Databricks demo](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_databricks_demo.ipynb))
- **Product recommendation** — project product catalogs with vector embeddings, run similarity search to find related items ([S3 Vectors demo](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_s3_vector_embedding_demo.ipynb), [OpenSearch demo](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_open_search_embedding_demo.ipynb))

Vector embeddings are a natural add-on to graph analytics — import them alongside your graph data to combine structural traversal with semantic similarity search.

**Session management:**

The `SessionManager` API manages the full lifecycle of a Neptune Analytics graph: create, import, analyze, export, and destroy. See the [session manager demo](https://github.com/awslabs/nx-neptune/blob/main/notebooks/session_manager_comprehensive_demo.ipynb) and [instance lifecycle demo](https://github.com/awslabs/nx-neptune/blob/main/notebooks/instance_mgmt_lifecycle_demo.ipynb).

### NetworkX Backend

nx-neptune also serves as a [NetworkX](https://networkx.org/)-compatible backend for Neptune Analytics, enabling you to offload graph algorithm workloads to AWS with no code changes. Use familiar NetworkX APIs to seamlessly scale graph computations on-demand. This combines the simplicity of local development with the performance and scalability of a fully managed AWS graph analytics service. For more on NetworkX backends, see the [NetworkX backends documentation](https://networkx.org/documentation/stable/backends.html).

```python
import networkx as nx

G = nx.Graph()
G.add_edge("Bill", "John")
r = nx.pagerank(G, backend="neptune")
```

#### Supported Algorithms

For details of all supported NetworkX algorithms see [algorithms.md](https://github.com/awslabs/nx-neptune/blob/main/algorithms.md)

## Preview Status

This project is in **Alpha Preview**. We welcome questions, suggestions, and contributions. It is recommended for testing purposes only — we're tracking production readiness on the [roadmap](https://github.com/awslabs/nx-neptune/issues).

## Installation

### Install it from PyPI

```bash
pip install nx_neptune
```

### Build and install from package wheel

```bash
# Package the project from source:
python -m pip wheel -w dist .

# Install with Jupyter dependencies from wheel: 
pip install "dist/nx_neptune-0.6.0-py3-none-any.whl"
```

### Install from source

```bash
git clone git@github.com:awslabs/nx-neptune.git
cd nx-neptune

# install from source directly
make install
```

Dependencies are pinned in lock files (`requirements.txt`, `requirements-dev.txt`) generated by [pip-tools](https://pip-tools.readthedocs.io/). If you update `pyproject.toml`, regenerate them with:

```bash
make lock
```

> **Note:** `make lock` must be run with a Python 3.11 interpreter to match CI. If your default Python is a different version, create a 3.11 venv:
> ```bash
> python3.11 -m venv .venv-lock
> source .venv-lock/bin/activate
> pip install pip-tools
> make lock
> ```
>

## Prerequisite 
Before using this backend, ensure the following prerequisites are met:

### AWS IAM Permissions
The IAM role or user accessing Neptune Analytics must have the following permissions:

These permissions are required to read, write, and manage graph data via queries on Neptune Analytics:

  - `neptune-graph:ReadDataViaQuery`
  - `neptune-graph:WriteDataViaQuery`
  - `neptune-graph:DeleteDataViaQuery`

These permissions are required to start/stop a Neptune Analytics graph:

  - `neptune-graph:StartGraph`
  - `neptune-graph:StopGraph`

These permissions are required to save/restore a Neptune Analytics snapshot:

  - `neptune-graph:CreateGraphSnapshot` (for save)
  - `neptune-graph:RestoreGraphFromSnapshot` (for restore)
  - `neptune-graph:DeleteGraphSnapshot` (for delete)
  - `neptune-graph:TagResource`

These permissions are required to import/export between S3 and Neptune Analytics:

  - `s3:GetObject` (for import)
  - `s3:PutObject` (for export)
  - `s3:ListBucket` (for export)
  - `s3:DeleteBucket` (for delete)
  - `kms:Decrypt`
  - `kms:GenerateDataKey`
  - `kms:DescribeKey`

In Addition to the S3 import/export permissions, to read from/write to an existing S3 Tables datalake: 

  - `athena:StartQueryExecution`
  - `athena:GetQueryExecution`

The ARN with the above permissions must be added to your environment variables

### Python Runtime

  - Python 3.11 is required.
  - Ensure your environment uses Python 3.11 to maintain compatibility with dependencies and API integrations.

_Note: As part of the preview status, we are recommending that you run the library using Python 3.11_. 

## Usage

```py
import networkx as nx

G = nx.Graph()
G.add_node("Bill")
G.add_node("John")
G.add_edge("Bill", "John")

r = nx.pagerank(G, backend="neptune")
```

And run with:

```bash
# Set the NETWORKX_GRAPH_ID environment variable
export NETWORKX_GRAPH_ID=your-neptune-analytics-graph-id
python ./nx_example.py
```

Alternatively, you can pass the `NETWORKX_GRAPH_ID` directly:

```bash
NETWORKX_GRAPH_ID=your-neptune-analytics-graph-id python ./nx_example.py
````

Without a valid `NETWORKX_GRAPH_ID`, the examples will fail to connect to your Neptune
Analytics instance. Make sure your AWS credentials are properly configured and
your IAM role/user has the required permissions (ReadDataViaQuery,
WriteDataViaQuery, DeleteDataViaQuery).

## Running tests

Unit tests can be run with make, this runs all tests in the `test` folder:
```bash
make test
```

Integration tests are included in the `integ_test` folder and run examples against an existing instance of Neptune 
Analytics, by passing the graph identifier available in the AWS account. 
```bash
export NETWORKX_GRAPH_ID=g-test12345
make integ-test
```

You can set `BACKEND=False` to run the test suite using NetworkX without nx-neptune as the backend. 

## CloudFormation Deployment

A CloudFormation template is provided to deploy a complete Neptune Analytics + SageMaker notebook environment with a single command. The stack creates a Neptune Analytics graph, a SageMaker notebook instance with `nx_neptune` pre-installed, an S3 staging bucket with KMS encryption, and all required IAM permissions.

### Quick deploy

By default, the stack installs `nx_neptune` from PyPI:

```bash
./cloudformation-templates/deploy.sh                        # defaults: nx-neptune-demo, us-west-1
./cloudformation-templates/deploy.sh my-stack us-east-1     # custom stack name and region
```

To deploy with a locally built wheel instead, pass `true` as the third argument:

```bash
./cloudformation-templates/deploy.sh nx-neptune-demo us-west-1 true
```

### Teardown

```bash
./cloudformation-templates/teardown.sh                      # defaults: nx-neptune-demo, us-west-1
./cloudformation-templates/teardown.sh my-stack us-east-1
```

For full parameter reference, manual deploy steps, and environment variable details, see [cloudformation-templates/README.md](https://github.com/awslabs/nx-neptune/blob/main/cloudformation-templates/README.md).

## Jupyter Notebook Integration

For interactive exploration and visualization, you can use the Jupyter notebook integration. To deploy a pre-configured SageMaker notebook environment, see [CloudFormation Deployment](#cloudformation-deployment) above.

### Notebooks

The notebooks directory contains interactive demonstrations:

**Data lake integration:**
- [import_s3_table_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_s3_table_demo.ipynb): Project S3 Tables data into a graph, run Louvain, export results back to Iceberg
- [import_s3_vector_embedding_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_s3_vector_embedding_demo.ipynb): Project S3 Vector embeddings via Athena federated query
- [import_databricks_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_databricks_demo.ipynb): Project Databricks tables via Athena federated query
- [import_snowflake_table_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_snowflake_table_demo.ipynb): Project Snowflake tables via Athena federated query
- [import_open_search_embedding_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/import_open_search_embedding_demo.ipynb): Project OpenSearch embeddings via Athena federated query
- [s3_import_export_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/s3_import_export_demo.ipynb): Import from and export to S3

**Session and lifecycle management:**
- [session_manager_comprehensive_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/session_manager_comprehensive_demo.ipynb): SessionManager API — create, import, analyze, export, destroy
- [instance_mgmt_lifecycle_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/instance_mgmt_lifecycle_demo.ipynb): Explicit instance lifecycle management
- [instance_mgmt_with_configuration.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/instance_mgmt_with_configuration.ipynb): Configuration-based instance management

**Algorithm demos:**
- [pagerank_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/pagerank_demo.ipynb): Focused demonstration of the PageRank algorithm
- [bfs_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/bfs_demo.ipynb): Demonstration of Breadth-First Search traversal
- [degree_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/degree_demo.ipynb): Demonstration of Degree Centrality algorithm
- [closeness_centrality_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/closeness_centrality_demo.ipynb): Focused demonstration of the Closeness Centrality algorithm
- [louvain_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/louvain_demo.ipynb): Demonstration of Louvain algorithm
- [label_propagation_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/label_propagation_demo.ipynb): Demonstration of Label Propagation algorithm

### Running locally

[A full tutorial is available to run in Neptune Jupyter Notebooks](https://github.com/awslabs/nx-neptune/blob/main/notebooks/README.md).

To install the required dependencies for the Jupyter notebook (including the `Jupyter` dependencies):

```bash
pip install "nx_neptune[jupyter]"
```

To run the Jupyter notebooks:

1. Set your Neptune Analytics Graph ID as an environment variable:
   ```bash
   export NETWORKX_GRAPH_ID=your-neptune-analytics-graph-id
   ```

2. You will also need to specify the IAM roles that will execute S3 import or export:

   ```bash
   export NETWORKX_ARN_IAM_ROLE=arn:aws:iam::AWS_ACCOUNT:role/IAM_ROLE_NAME
   export NETWORKX_S3_IMPORT_BUCKET_PATH=s3://S3_BUCKET_PATH
   export NETWORKX_S3_EXPORT_BUCKET_PATH=s3://S3_BUCKET_PATH
   ```

3. Launch Jupyter Notebook:
   ```bash
   jupyter notebook notebooks/
   ```

## Security

See [CONTRIBUTING](https://github.com/awslabs/nx-neptune/blob/main/CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

