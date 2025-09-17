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
- `algorithms/centrality/` - Centrality algorithms (PageRank, degree, closeness, betweenness)
- `algorithms/communities/` - Community detection (Louvain, label propagation)
- `algorithms/link_analysis/` - Link analysis algorithms
- `algorithms/traversal/` - Graph traversal (BFS, DFS)
- `algorithms/util/` - Utility algorithms

### Client Integration
- `clients/` - AWS service clients and authentication handling

### Utilities
- `utils/` - Helper functions and common utilities

## Development Context

### Technology Stack
- **Python**: 3.11+ required
- **Dependencies**: NetworkX (>=3.4.2), boto3 (>=1.37), cymple (>=0.12.0)
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
1. Implement in appropriate `algorithms/` subdirectory
2. Use `@configure_if_nx_active()` decorator
3. Map NetworkX parameters to Neptune Analytics API
4. Add tests and documentation

### Instance Management
- Programmatic creation/deletion of Neptune Analytics instances
- Configuration-based lifecycle management
- S3 import/export capabilities

### Error Handling
- AWS service exceptions
- NetworkX compatibility validation
- Parameter validation and transformation

## Project Status
- **Alpha Preview** - Testing purposes only
- Open source project seeking community feedback
- Production readiness tracked on roadmap
- Active development and collaboration encouraged
