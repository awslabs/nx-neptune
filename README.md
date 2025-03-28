
---
# nx_neptune_analytics

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
pip install nx_neptune_analytics
```

## Prerequisite 
 - Some prerequisites on AWS IAM actions and credentials 
 - Python runtime requirements

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
$ python -m nx_neptune_analytics
#or
$ nx_neptune_analytics
```

## Development

Read the [CONTRIBUTING.md](CONTRIBUTING.md) file.


## License
- License for the repo