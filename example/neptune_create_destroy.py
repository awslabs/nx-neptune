import networkx as nx
from nx_neptune.utils.utils import get_stdout_logger

""" 
This sample script demonstrates how to setup and teardown AWS Neptune Analytics 
clusters to use with the NetworkX algorithms.  
In this example, we'll be creating a new graph instance, running the pagerank algorithm, 
then tearing down the cluster.
"""
logger = get_stdout_logger(__name__,  [
    'nx_neptune.instance_management',
    'nx_neptune.interface',
    __name__
])

nx.config.warnings_to_ignore.add("cache")

BACKEND = "neptune"
print(f"Using backend={BACKEND}")

nx.config.backends.neptune.create_new_instance = True
nx.config.backends.neptune.destroy_instance = True

g = nx.DiGraph()
# Test data - explicitly defining the graph with alphabetical nodes and directed edges
# Add nodes
g.add_nodes_from(['A', 'B', 'C', 'D', 'E'])
# Graph structure:
#
#    A→B→C→D→E
#        ↑   |
#        └───┘
#
#    X(DCd)
#
# Add directed edges to create a directed path graph (A→B→C→D→E)
g.add_edge('A', 'B')
g.add_edge('B', 'C')
g.add_edge('C', 'D')
g.add_edge('D', 'E')
# Add a cycle by connecting E back to C
g.add_edge('E', 'C')
# Add an isolated node
g.add_node("X(DCd)")

logger.info("\n-------------------\n")
# scenario: AWS
# Note: PageRank values may differ between NetworkX and Neptune Analytics due to implementation differences
# Expected result:
# INFO - C: 0.3152066
# INFO - D: 0.2962857
# INFO - E: 0.276372
# INFO - B: 0.05388349
# INFO - X(DCd): 0.02912621
# INFO - A: 0.02912621
r = nx.pagerank(g, backend="neptune")
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")