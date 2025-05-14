import os

import networkx as nx

from nx_neptune import NeptuneGraph
from nx_neptune.utils.utils import get_stdout_logger

""" 
Example script to demonstrate how PageRank algorithm computation can be offloaded into remote AWS Neptune Analytics instance.  
"""

"""Read and load graphId from environment variable. """
graph_id = os.getenv('GRAPH_ID')
if not graph_id:
    raise Exception('Environment Variable GRAPH_ID is not defined')
nx.config.warnings_to_ignore.add("cache")

logger = get_stdout_logger(__name__,[
                    'nx_neptune.algorithms.link_analysis.pagerank',
                    'nx_neptune.na_graph', __name__])

backend = "neptune"
# Clean up remote graph and populate test data.
g = nx.DiGraph()
na_graph = NeptuneGraph(graph=g)
na_graph.clear_graph()
# Test data - explicitly defining the graph with alphabetical nodes and directed edges
# Add nodes
nodes = ['A', 'B', 'C', 'D', 'E']
g.add_nodes_from(nodes)
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

# Scenario: Local execution
# Expected result:
# INFO - C: 0.31286951643922406
# INFO - D: 0.2950649889212595
# INFO - E: 0.27992957230941956
# INFO - B: 0.053883495145631066
# INFO - X(DCd): 0.02912621359223301
# INFO - A: 0.02912621359223301
r = nx.pagerank(g)
logger.info("Algorithm execution - NetworkX: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")


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
logger.info("Algorithm execution - Neptun Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")
