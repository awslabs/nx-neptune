
---
# nx_neptune_analytics

[![codecov](https://codecov.io/gh/Bit-Quill/nx-neptune-analytics/branch/main/graph/badge.svg?token=nx-neptune-analytics_token_here)](https://codecov.io/gh/Bit-Quill/nx-neptune-analytics)
[![CI](https://github.com/Bit-Quill/nx-neptune-analytics/actions/workflows/main.yml/badge.svg)](https://github.com/Bit-Quill/nx-neptune-analytics/actions/workflows/main.yml)
[![CodeQL](https://https://github.com/Bit-Quill/nx-neptune-analytics/actions/workflows/codeql.yml/badge.svg)](https://github.com/Bit-Quill/nx-neptune-analytics/actions/workflows/codeql.yml)
[![Upload Python Package](https://github.com/Bit-Quill/nx-neptune-analytics/actions/workflows/release.yml/badge.svg)](https://github.com/Bit-Quill/nx-neptune-analytics/actions/workflows/release.yml)



Awesome nx_neptune_analytics created by Improving

## Install it from PyPI

```bash
pip install nx_neptune_analytics
```

## Prerequisite 
 - Some prerequisites on AWS IAM actions and credentials 
 - Python runtime requirements

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