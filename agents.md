# nx-neptune Agent Context

## Project Overview
nx-neptune is a NetworkX-compatible backend for Amazon Neptune Analytics that enables seamless graph algorithm execution on AWS infrastructure. This library allows developers to use familiar NetworkX APIs while leveraging the scalability and performance of Neptune Analytics.

## Key Architecture Components

### Core Module Structure
- `nx_neptune/` - Main package directory containing core backend implementation and Neptune Analytics integration

### Plugin Integration
- `nx_plugin/` - NetworkX plugin integration and backend registration

### Algorithm Implementation
- `algorithms/` - Algorithm implementations organized by category folders (centrality, communities, link_analysis, traversal)
- `algorithms/util/` - Utility methods and helper functions for algorithms

### Client Integration
- `clients/` - AWS service clients and authentication handling

### Utilities
- `utils/` - Helper functions and common utilities

## Development Context

### Technology Stack
- **Python**: Check `pyproject.toml` for current version requirements
- **Dependencies**: Check `pyproject.toml` for current version requirements
- **AWS Services**: Neptune Analytics, S3, IAM
- **Testing**: pytest with coverage reporting
  - Run full test suite: `pytest tests/`
  - Run specific test: `pytest tests/algorithms/{category}/test_{algorithm_name}.py`

## Code Patterns

### Backend Registration
```python
import networkx as nx
result = nx.algorithm_name(graph, backend="neptune")
```

### Algorithm Implementation Pattern
```python
@configure_if_nx_active()
def algorithm_name(neptune_graph: NeptuneGraph, **kwargs):
    # Neptune Analytics API call
    # Result processing
    # Return NetworkX-compatible format
```

### Graph Operations
- Graph data is synchronized between NetworkX and Neptune Analytics
- Algorithms execute on Neptune Analytics infrastructure
- Results are returned in NetworkX-compatible formats

## Testing Strategy
- Unit tests in `tests/` directory
- Integration tests with mock Neptune Analytics responses
- Coverage reporting with pytest-cov
- CI/CD via GitHub Actions

## Documentation
- `README.md` - Installation and usage guide
- `algorithms.md` - Comprehensive algorithm documentation
- `notebooks/` - Interactive Jupyter demonstrations
- `CONTRIBUTING.md` - Development guidelines
- `AGENTS.md` - Agent context and development guidance

## Common Development Tasks

### Adding New Algorithms to Algorithm Implementation Module
Add new algorithms to `algorithms/` directory, organized by appropriate category folder. For example, when implementing betweenness centrality, create `algorithms/centrality/betweenness_centrality.py`. When adding shortest path algorithms, create files under `algorithms/traversal/`. Also update the corresponding `__init__.py` files to export new algorithms.

#### Step 1: Choose Algorithm Category and Location
Directory structure under `algorithms/` must match NetworkX's algorithms directory structure. Method names must also match NetworkX exactly.

To determine the correct location:
1. Find the algorithm in NetworkX source code at `networkx/algorithms/`
2. Use the same directory structure and file name
3. Use the exact same function name as NetworkX

Existing categories:
- **Centrality**: `nx_neptune/algorithms/centrality/` (PageRank, degree, closeness, betweenness)
- **Communities**: `nx_neptune/algorithms/communities/` (Louvain, label propagation)
- **Traversal**: `nx_neptune/algorithms/traversal/` (BFS, DFS)
- **Link Analysis**: `nx_neptune/algorithms/link_analysis/` (HITS, authority)
- **Utilities**: `nx_neptune/algorithms/util/` (Helper algorithms)
- **Create new category**: If algorithm doesn't fit existing categories, create new directory under `nx_neptune/algorithms/` matching NetworkX structure

#### Step 2: Create Algorithm Implementation File
Create `{algorithm_name}.py` in the appropriate category directory with:
- Required copyright header (Apache 2.0 license)
- Import necessary modules from `nx_neptune.clients.neptune_constants`
- Import query builders from `nx_neptune.clients.opencypher_builder`
- Use `@configure_if_nx_active()` decorator on main function
- Implement parameter processing and validation
- Handle both query and mutation execution paths
- Transform Neptune Analytics results to NetworkX-compatible format

#### Step 3: Add Query Builder Functions
Add query building functions to `nx_neptune/clients/opencypher_builder.py`:
- `{algorithm_name}_query()` - Build openCypher query for read operations
- `{algorithm_name}_mutation_query()` - Build mutation query for write operations
- Handle parameter mapping between NetworkX and Neptune Analytics
- Return tuple of (query_string, parameter_map)

#### Step 4: Add Constants
Add algorithm-specific constants to `nx_neptune/clients/neptune_constants.py`:
- Algorithm mutation constant (`{ALGORITHM_NAME}_MUTATE_ALG`)
- Response field constants (`RESPONSE_{FIELD_NAME}`)
- Parameter constants (`PARAM_{PARAMETER_NAME}`)

#### Step 5: Update Module Exports
Add algorithm to `nx_neptune/algorithms/{category}/__init__.py`:
- Import the new algorithm function
- Add to `__all__` list for proper module exports

#### Step 6: Create Comprehensive Tests
Create `tests/algorithms/{category}/test_{algorithm_name}.py`:
- Test class with descriptive name
- Mock NeptuneGraph fixture with sample return data
- Test basic algorithm execution
- Test with various parameter combinations
- Test mutation operations (write_property)
- Test error handling and edge cases
- Verify NetworkX compatibility

#### Step 7: Add Documentation
Update `algorithms.md` with algorithm documentation:
- Algorithm description and purpose
- Link to Neptune Analytics documentation
- Source file location
- Parameter descriptions with types and defaults
- Return value format and structure
- Usage examples

#### Step 8: Integration Testing
- Test with actual Neptune Analytics instance using environment variables
- Verify NetworkX compatibility and result format consistency
- Performance benchmarking against NetworkX native implementation
- Edge case handling (empty graphs, disconnected components)
- Parameter validation and error scenarios

#### Step 9: Update Package Exports
Add to main `nx_neptune/__init__.py`:
- Import the algorithm from its module
- Add to main package `__all__` list
- Ensure proper backend registration

### Step 10: Update exported documentation
Update `nx_plugin/__init__.py` with new or updated information on the algorithm.
- Include new algorithm in the list of `functions`.
- Include `additional_docs` if necessary, and link to algorithm in the repository.

#### Parameter Compatibility Requirements
- **NetworkX Parameters**: All parameters from NetworkX must be included in the method with the same defaults
- **Neptune Analytics Parameters**: All parameters defined in the Neptune Analytics algorithm must be included
- **Unsupported Parameters**: If any parameters cannot be supported by the mapping:
  - Document the parameter as "not supported" in the algorithm documentation
  - Raise a warning when the unsupported parameter is used

#### Key Implementation Guidelines
- **Parameter Mapping**: Map NetworkX parameters to Neptune Analytics equivalents
- **Result Transformation**: Convert Neptune Analytics JSON results to NetworkX format
- **Error Handling**: Handle AWS service exceptions and parameter validation
- **Logging**: Use structured logging with algorithm name for debugging
- **Documentation**: Include Neptune Analytics documentation links and examples
- **Testing**: Cover both query and mutation variants with comprehensive test cases
- **Performance**: Consider result size and memory usage for large graphs
- **Compatibility**: Ensure results match NetworkX behavior and format expectations

#### Parameter Compatibility
Ensuring complete parameter compatibility between NetworkX and Neptune Analytics is critical for seamless backend integration. All NetworkX parameters must be preserved with identical defaults to maintain API compatibility. Additionally, Neptune Analytics-specific parameters should be exposed to leverage the full capabilities of the service. When parameters cannot be mapped due to fundamental differences between the implementations, clear documentation and runtime warnings help users understand limitations and make informed decisions about parameter usage.

### Adding New AWS Service Integrations to Client Module
Add new AWS service connections and API integrations to `clients/` directory. For example, when adding support for Neptune Database (in addition to Neptune Analytics), create new client classes in this module. When implementing new authentication methods or API wrappers for other AWS services like CloudWatch or Lambda, add them here.

### Adding Shared Utilities
Add shared utility functions and common helpers to `utils/` directory. For example, when creating graph data transformation utilities, parameter validation helpers, or common error handling functions that are used across multiple algorithms, place them in this module.

### Creating Documentation and Examples

For new algorithms, documentation and examples must be created in three locations:
1. `algorithms.md` (root) — detailed reference (covered in Step 7)
2. `docs-site/src/content/docs/networkx-backend/algorithms.mdx` — Starlight docs site
3. `notebooks/{algorithm_name}_demo.ipynb` — interactive Jupyter demo

#### Updating the Docs Site (`algorithms.mdx`)

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

#### Creating a Notebook Demo

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

**Required imports (Cell 2):**
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

**Logger setup (Cell 3):**
```python
logger = get_stdout_logger(__name__,[
                    'nx_neptune.algorithms.{category}.{module_name}',
                    'nx_neptune.na_graph', 'nx_neptune.utils.decorators',
                    'nx_neptune.instance_management',__name__])

# Ignore cache warnings
nx.config.warnings_to_ignore.add("cache")
```

**Graph ID check (Cell 5):**
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

**Dataset loading (Cell 7) — standard air-routes pattern:**
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

**Example cells — follow these patterns by algorithm type:**

For **centrality/scoring** algorithms (returns `{node: score}`):
```python
### Example 1: Basic execution
result = nx.{function_name}(air_route_graph, backend="neptune")
# Top 5 results
for key, value in sorted(result.items(), key=lambda x: (x[1], x[0]), reverse=True)[:5]:
    print(f"{key}: {value:.6f}")
```

For **community** algorithms (returns list of sets):
```python
### Example 1: Community detection
result = nx.community.{function_name}(air_route_graph, backend="neptune")
# Sort by size, show top 5
sorted_communities = sorted(result, key=len, reverse=True)
for i, community in enumerate(sorted_communities[:5], 1):
    sample = list(community)[:3]
    logger.info(f"Size:{len(community)} - {sample}......")
```

For **pathfinding** algorithms (returns path/distance):
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

**Notebook metadata (must be included in the `.ipynb` JSON):**
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

**Key rules:**
- All code cells must have `"outputs": []` and `"execution_count": null` (no stale output committed)
- Each cell's `"source"` is a list of strings (one per line, each ending with `\n` except the last)
- Keep examples minimal — demonstrate the API, not complex analysis
- Always include: basic execution, one variant (e.g. with filtering), and mutation
- The notebook filename must match: `{algorithm_name}_demo.ipynb` (e.g., `wcc_demo.ipynb`)

#### Updating the Plugin Info (`nx_plugin/__init__.py`)

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

### Updating Agent Context
Update `AGENTS.md` when making architectural changes, discovering new development patterns, or implementing reusable solutions.

### Instance Management
Configuration settings for Neptune instances are handled through the NeptuneConfig class found in `nx-plugin/config.py`. When adding new instance management workflows, they should be documented within this configuration class.

The core implementation of instance management operations resides in `nx_neptune/instance_management.py`. Task orchestration and control flow are managed through decorators located in `nx_neptune/utils/decorators.py`.

- Programmatic creation/deletion of Neptune Analytics instances
- Configuration-based lifecycle management
- S3 import/export capabilities

### Error Handling
- AWS service exceptions
- NetworkX compatibility validation
- Parameter validation and transformation

## Project Status
- Open source project seeking community feedback
- Active development and collaboration encouraged
