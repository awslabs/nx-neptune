# Algorithm Documentation Guide

This guide covers how to create documentation and examples for new nx-neptune algorithms. There are three artifacts to produce:

1. `algorithms.md` (root) — detailed parameter reference
2. `docs-site/src/content/docs/networkx-backend/algorithms.mdx` — Starlight docs site
3. `notebooks/{algorithm_name}_demo.ipynb` — interactive Jupyter demo

---

## Updating the Docs Site (`algorithms.mdx`)

Location: `docs-site/src/content/docs/networkx-backend/algorithms.mdx`

This is an Astro Starlight page using MDX. Add a new section under the appropriate category heading (## Traversal Algorithms, ## Centrality Algorithms, ## Community Algorithms, etc.). Follow this exact template:

```mdx
---

### {function_name}

{One-sentence description of what the algorithm does.}

- **Neptune Analytics docs:** [{Algorithm Name}](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/{page}.html)
- **Source:** [`nx_neptune/algorithms/{category}/{file}.py`](https://github.com/awslabs/nx-neptune/blob/main/nx_neptune/algorithms/{category}/{file}.py)

\`\`\`python
@configure_if_nx_active()
def {function_name}(
    neptune_graph: NeptuneGraph,
    # ... full signature
):
\`\`\`

| Parameter | Type | Description |
|-----------|------|-------------|
| `param_name` | type | Description |
| `unsupported_param` | type | ⚠️ Not supported in Neptune Analytics |
| `vertex_label` | str | Vertex label filter |
| `edge_labels` | list | Edge label filter |
| `concurrency` | int | Thread count (0 = all available) |
| `write_property` | str | Persist results as node property |

**Returns:** {Description of return format matching NetworkX behavior.}
```

Key formatting rules:
- Separate each algorithm section with `---` (horizontal rule)
- Use `⚠️ Not supported in Neptune Analytics` for unsupported NX parameters
- Always include Neptune Analytics extension parameters (`vertex_label`, `edge_labels`, `concurrency`, `write_property`) in the table
- Link the source file to the GitHub blob URL (`https://github.com/awslabs/nx-neptune/blob/main/...`)
- The page imports `import { Aside } from '@astrojs/starlight/components';` at the top — use `<Aside>` only for cross-cutting notes, not per-algorithm

---

## Creating a Notebook Demo

Location: `notebooks/{algorithm_name}_demo.ipynb`

Notebooks are standard Jupyter `.ipynb` files (JSON format). They follow a fixed cell structure:

**Cell structure (in order):**

| Cell # | Type | Content |
|--------|------|---------|
| 0 | markdown | `# {Algorithm Name} with Neptune Analytics` + one-paragraph description |
| 1 | markdown | `## Setup and Imports` |
| 2 | code | Version check + imports (see template below) |
| 3 | code | Logger setup (see template below) |
| 4 | markdown | `## Check for Neptune Analytics Graph ID` |
| 5 | code | Graph ID from env var (see template below) |
| 6 | markdown | `## Download and configure Air route dataset` |
| 7 | code | Download air-routes dataset + build NetworkX graph (see template below) |
| 8+ | markdown + code | Examples (1 markdown heading + 1 code cell per example) |

### Required imports (Cell 2)

```python
# Check the Python version:
from sys import version_info
assert version_info >= (3, 11), "Python 3.11 or higher is required"

import os
import requests
import pandas as pd

import networkx as nx
from nx_neptune import NeptuneGraph
from nx_neptune.clients import Node
from nx_neptune.utils.utils import get_stdout_logger
```

### Logger setup (Cell 3)

```python
logger = get_stdout_logger(__name__,[
                    'nx_neptune.algorithms.{category}.{module_name}',
                    'nx_neptune.na_graph', 'nx_neptune.utils.decorators',
                    'nx_neptune.instance_management',__name__])

# Ignore cache warnings
nx.config.warnings_to_ignore.add("cache")
```

### Graph ID check (Cell 5)

```python
# Read and load graphId from environment variable
graph_id = os.getenv('NETWORKX_GRAPH_ID')

# If not set, you can set it here
if not graph_id:
    # Uncomment and set your Graph ID
    # %env NETWORKX_GRAPH_ID=your-neptune-analytics-graph-id
    # graph_id = os.getenv('NETWORKX_GRAPH_ID')
    print("Warning: Environment Variable NETWORKX_GRAPH_ID is not defined")
    print("You can set it using: %env NETWORKX_GRAPH_ID=your-neptune-analytics-graph-id")
else:
    print(f"Using Neptune Analytics Graph ID: {graph_id}")
```

### Dataset loading (Cell 7) — standard air-routes pattern

```python
# Download routes data
routes_url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
routes_file = "resources/notebook_test_data_routes.dat"

# Ensure the directory exists
os.makedirs(os.path.dirname(routes_file), exist_ok=True)

# Download only if file doesn't exist
if not os.path.isfile(routes_file):
    with open(routes_file, "wb") as f:
        f.write(requests.get(routes_url).content)

cols = [
    "airline", "airline_id", "source_airport", "source_airport_id",
    "dest_airport", "dest_airport_id", "codeshare", "stops", "equipment",
]
routes = pd.read_csv(routes_file, names=cols)
routes = routes[["source_airport", "dest_airport"]].dropna()

air_route_graph = nx.from_pandas_edgelist(
    routes, source="source_airport", target="dest_airport",
    create_using=nx.DiGraph()
)
print(f"Graph loaded: {air_route_graph.number_of_nodes()} nodes, {air_route_graph.number_of_edges()} edges")
```

### Example cells — patterns by algorithm type

**For centrality/scoring algorithms** (returns `{node: score}`):
```python
### Example 1: Basic execution
result = nx.{function_name}(air_route_graph, backend="neptune")
# Top 5 results
for key, value in sorted(result.items(), key=lambda x: (x[1], x[0]), reverse=True)[:5]:
    print(f"{key}: {value:.6f}")
```

**For community algorithms** (returns list of sets):
```python
### Example 1: Community detection
result = nx.community.{function_name}(air_route_graph, backend="neptune")
# Sort by size, show top 5
sorted_communities = sorted(result, key=len, reverse=True)
for i, community in enumerate(sorted_communities[:5], 1):
    sample = list(community)[:3]
    logger.info(f"Size:{len(community)} - {sample}......")
```

**For pathfinding algorithms** (returns path/distance):
```python
### Example 1: Find shortest path
result = nx.{function_name}(air_route_graph, source="JFK", target="LAX", backend="neptune")
logger.info(f"Path: {result}")
```

**Mutation example (always include as last example):**
```python
### Example N: Mutation (write_property)
nx.{function_name}(air_route_graph, backend="neptune", write_property="{property_name}")
"""List 10 nodes to verify"""
nx_graph = NeptuneGraph.from_config(graph=air_route_graph)
for item in nx_graph.get_all_nodes()[:10]:
    logger.info(Node.from_neptune_response(item))
```

### Notebook metadata

Must be included in the `.ipynb` JSON:

```json
{
  "metadata": {
    "kernelspec": {
      "display_name": "Python 3 (ipykernel)",
      "language": "python",
      "name": "python3"
    },
    "language_info": {
      "codemirror_mode": {"name": "ipython", "version": 3},
      "file_extension": ".py",
      "mimetype": "text/x-python",
      "name": "python",
      "pygments_lexer": "ipython3",
      "version": "3.13.5"
    }
  },
  "nbformat": 4,
  "nbformat_minor": 5
}
```

### Key rules

- All code cells must have `"outputs": []` and `"execution_count": null` (no stale output committed)
- Each cell's `"source"` is a list of strings (one per line, each ending with `\n` except the last)
- Keep examples minimal — demonstrate the API, not complex analysis
- Always include: basic execution, one variant (e.g. with filtering), and mutation
- The notebook filename must match: `{algorithm_name}_demo.ipynb` (e.g., `wcc_demo.ipynb`)

---

## Updating the Plugin Info (`nx_plugin/__init__.py`)

After implementing a new algorithm, register it in `nx_plugin/__init__.py`:

1. Add the function name to the `"functions"` set in `_info`
2. Add an entry to `"additional_docs"` with notes about unsupported parameters and a link to `algorithms.md`:

```python
_info = {
    ...
    "functions": {
        ...
        "new_algorithm_name",  # Add here
    },
    "additional_docs": {
        ...
        "new_algorithm_name": f"""- Note about unsupported params if any.
- For additional parameters, see {algorithms_url}#new_algorithm_name""",
    },
}
```

If the algorithm has no unsupported parameters, use the short form:
```python
"new_algorithm_name": f"For additional details, see {algorithms_url}#new_algorithm_name",
```
