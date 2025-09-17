# nx-neptune Agent Context

## Project Overview
nx-neptune is a NetworkX-compatible backend for Amazon Neptune Analytics that enables seamless graph algorithm execution on AWS infrastructure. This library allows developers to use familiar NetworkX APIs while leveraging the scalability and performance of Neptune Analytics.

## Key Architecture Components

### Core Module Structure
- `nx_neptune/` - Main package directory
  - `__init__.py` - Package initialization and backend registration
  - `interface.py` - NetworkX backend interface implementation
  - `na_graph.py` - Neptune Analytics graph wrapper class
  - `instance_management.py` - Neptune Analytics instance lifecycle management

### Algorithm Categories
- `algorithms/centrality/` - Centrality algorithms (PageRank, degree, closeness)
- `algorithms/communities/` - Community detection (Louvain, label propagation)
- `algorithms/link_analysis/` - Link analysis algorithms
- `algorithms/traversal/` - Graph traversal (BFS, DFS)
- `algorithms/util/` - Utility methods for algorithms

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

### Environment Requirements
- `NETWORKX_GRAPH_ID` - Neptune Analytics graph identifier
- `NETWORKX_ARN_IAM_ROLE` - IAM role ARN for S3 operations
- `NETWORKX_S3_IMPORT_BUCKET_PATH` - S3 bucket for data import
- `NETWORKX_S3_EXPORT_BUCKET_PATH` - S3 bucket for data export

### Required AWS Permissions
- `neptune-graph:ReadDataViaQuery`
- `neptune-graph:WriteDataViaQuery` 
- `neptune-graph:DeleteDataViaQuery`
- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`
- `kms:Decrypt`, `kms:GenerateDataKey`, `kms:DescribeKey`

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

## Common Development Tasks

### Adding New Algorithms

#### Step 1: Choose Algorithm Category and Location
- **Centrality**: `nx_neptune/algorithms/centrality/` (PageRank, degree, closeness, betweenness)
- **Communities**: `nx_neptune/algorithms/communities/` (Louvain, label propagation)
- **Traversal**: `nx_neptune/algorithms/traversal/` (BFS, DFS)
- **Link Analysis**: `nx_neptune/algorithms/link_analysis/` (HITS, authority)
- **Utilities**: `nx_neptune/algorithms/util/` (Helper algorithms)
- **Create new category**: If algorithm doesn't fit existing categories, create new directory under `nx_neptune/algorithms/`

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

#### Key Implementation Guidelines
- **Parameter Mapping**: Map NetworkX parameters to Neptune Analytics equivalents
- **Result Transformation**: Convert Neptune Analytics JSON results to NetworkX format
- **Error Handling**: Handle AWS service exceptions and parameter validation
- **Logging**: Use structured logging with algorithm name for debugging
- **Documentation**: Include Neptune Analytics documentation links and examples
- **Testing**: Cover both query and mutation variants with comprehensive test cases
- **Performance**: Consider result size and memory usage for large graphs
- **Compatibility**: Ensure results match NetworkX behavior and format expectations

### Instance Management
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
