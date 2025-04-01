
---
# nx_neptune

<a href="https://networkx.github.io/">
    <img src="https://avatars.githubusercontent.com/u/388785?s=200&v=4" alt="NetworkX" height="60">
</a>
<a href="https://aws.amazon.com/neptune/">
    <img src="https://avatars.githubusercontent.com/u/2232217?s=200&v=4" alt="AWS" height="60">
</a>


[![CI](https://github.com/Bit-Quill/nx-neptune-analytics/actions/workflows/main.yml/badge.svg)](https://github.com/Bit-Quill/nx-neptune-analytics/actions/workflows/main.yml)
[![Upload Python Package](https://github.com/Bit-Quill/nx-neptune-analytics/actions/workflows/release.yml/badge.svg)](https://github.com/Bit-Quill/nx-neptune-analytics/actions/workflows/release.yml)


This project offers a NetworkX-compatible backend for Neptune Analytics, enabling users to offload graph algorithm workloads to AWS with no code changes. By using familiar NetworkX APIs, developers can seamlessly scale graph computations on-demand through Neptune Analytics. This approach combines the simplicity of local development with the performance and scalability of a fully managed AWS graph analytics service.

## Install it from PyPI

```bash
pip install nx_neptune
```

## Prerequisite 
Before using this backend, ensure the following prerequisites are met:

### AWS IAM Permissions
The IAM role or user accessing Neptune Analytics must have the following permissions:

 - neptune-graph:ReadDataViaQuery
 - neptune-graph:WriteDataViaQuery
 - neptune-graph:DeleteDataViaQuery

These permissions are required to read, write, and manage graph data via queries on Neptune Analytics.

### Python Runtime
 - Python 3.9 is required.
 - Ensure your environment uses Python 3.9 to maintain compatibility with dependencies and API integrations.


## Usage

```py
import networkx as nx

G = nx.Graph()
G.add_node("Bill")
G.add_node("John")
G.add_edge("Bill", "John")

nx.shortest_path(
    G, source="John", target="Bill", backend="neptune_analytics"
)
```

```bash
$ python -m nx_neptune
#or
$ nx_neptune
```


## Examples
Before running the examples, you must specify your Neptune Analytics Graph ID 
as an environment variable:

```bash
# Set the GRAPH_ID environment variable
export GRAPH_ID=your-neptune-analytics-graph-id

# Then run the example
.venv/bin/python ./example/nx_client_example.py
```

Alternatively, you can pass the GRAPH_ID directly when running the example:

```bash
GRAPH_ID=your-neptune-analytics-graph-id .venv/bin/python ./example/nx_client_example.py
````

Without a valid GRAPH_ID, the examples will fail to connect to your Neptune 
Analytics instance. Make sure your AWS credentials are properly configured and 
your IAM role/user has the required permissions (ReadDataViaQuery, 
WriteDataViaQuery, DeleteDataViaQuery).

## Development

Read the [CONTRIBUTING.md](CONTRIBUTING.md) file.


## License
- License for the repo