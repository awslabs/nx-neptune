
---
# nx_neptune

[![CI](https://github.com/awslabs/nx-neptune/actions/workflows/main.yml/badge.svg)](https://github.com/awslabs/nx-neptune/actions/workflows/main.yml)
[![Upload Python Package](https://github.com/awslabs/nx-neptune/actions/workflows/python-publish.yml/badge.svg)](https://github.com/awslabs/nx-neptune/actions/workflows/python-publish.yml)

This project offers a NetworkX-compatible backend for Neptune Analytics, enabling users to offload graph algorithm workloads to AWS with no code changes. By using familiar NetworkX APIs, developers can seamlessly scale graph computations on-demand through Neptune Analytics. This approach combines the simplicity of local development with the performance and scalability of a fully managed AWS graph analytics service.

## Supported Algorithms

For details of all supported NetworkX algorithms see [algorithms.md](https://github.com/awslabs/nx-neptune/blob/main/algorithms.md)

## Preview Status: Alpha Release

We're making the `nx_neptune` plugin library an open-source project, and are releasing it now as an Alpha Preview to the community to gather feedback, and actively collaborate on the project roadmap. We welcome questions, suggestions, and contributions from all community members. At this point in development, the project has not been fully released to the public and is recommended for testing purposes only.  We're tracking its production readiness for general availability on the roadmap.   

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
pip install "dist/nx_neptune-0.4.1-py3-none-any.whl"
```

### Installation

To install the required nx_neptune dependencies:

```bash
git clone git@github.com:awslabs/nx-neptune.git
cd nx-neptune

# install from source directly
make install
```

## Prerequisite 
Before using this backend, ensure the following prerequisites are met:

### AWS IAM Permissions
The IAM role or user accessing Neptune Analytics must have the following permissions:

These permissions are required to read, write, and manage graph data via queries on Neptune Analytics:

 - neptune-graph:ReadDataViaQuery
 - neptune-graph:WriteDataViaQuery
 - neptune-graph:DeleteDataViaQuery

These permissions are required to import/export between S3 and Neptune Analytics: 

 - s3:GetObject (for import)
 - s3:PutObject (for export)
 - s3:ListBucket (for export)
 - kms:Decrypt
 - kms:GenerateDataKey
 - kms:DescribeKey

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

## Jupyter Notebook Integration

For interactive exploration and visualization, you can use the Jupyter notebook integration.

### Features

The notebooks directory contains interactive demonstrations of using Neptune Analytics with NetworkX:

- [pagerank_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/pagerank_demo.ipynb): Focused demonstration of the PageRank algorithm

- [bfs_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/bfs_demo.ipynb): Demonstration of Breadth-First Search traversal

- [degree_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/degree_demo.ipynb): Demonstration of Degree Centrality algorithm

- [label_propagation_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/label_propagation_demo.ipynb): Demonstration of Label Propagation algorithm

- [closeness_centrality_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/closeness_centrality_demo.ipynb): Focused demonstration of the Closeness Centrality algorithm

- [louvain_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/louvain_demo.ipynb): Demonstration of Louvain algorithm

- [s3_import_export_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/s3_import_export_demo.ipynb): A notebook demonstrating the process of importing from and exporting to an S3 bucket.

- [instance_mgmt_lifecycle_demo.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/instance_mgmt_lifecycle_demo.ipynb): A notebook to demonstrates the explicit workflow for managing the lifecycle of an instance.   

- [instance_mgmt_with_configuration.ipynb](https://github.com/awslabs/nx-neptune/blob/main/notebooks/instance_mgmt_with_configuration.ipynb): A notebook to demonstrates a simplified approach to instance lifecycle management.   


### Uploading a package wheel

We recommend uploading your package as a wheel to Jupyter Notebooks. 

```bash
# Package the project from source:
python -m pip wheel -w dist .
# creates dist/nx_neptune-0.4.1-py3-none-any.whl
```

### Installation

To install the required dependencies for the Jupyter notebook (including the `Jupyter` dependencies):

```bash
# Install with Jupyter dependencies from wheel: 
pip install "dist/nx_neptune-0.4.1-py3-none-any.whl[jupyter]"
```

### Running the Jupyter Notebook

[A full tutorial is available to run in Neptune Jupyter Notebooks](https://github.com/awslabs/nx-neptune/blob/main/notebooks/README.md).

To run the Jupyter notebooks:

1. Set your Neptune Analytics Graph ID as an environment variable:
   ```bash
   export NETWORKX_GRAPH_ID=your-neptune-analytics-graph-id
   ```

2. You will also need to specify the IAM roles that will execute S3 import or export:

   ```bash
   export NETWORKX_ARN_IAM_ROLE=arn:aws:iam::AWS_ACCOUNT:role/IAM_ROLE_NAME
   export NETWORKX_S3_IMPORT_BUCKET_PATH=s://S3_BUCKET_PATH
   export NETWORKX_S3_EXPORT_BUCKET_PATH=s://S3_BUCKET_PATH
   ```

3. Launch Jupyter Notebook:
   ```bash
   jupyter notebook notebooks/
   ```

4. You can also set the Graph ID directly in the notebook using:
   ```python
   %env NETWORKX_GRAPH_ID=your-neptune-analytics-graph-id
   ```

## Security

See [CONTRIBUTING](https://github.com/awslabs/nx-neptune/blob/main/CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

