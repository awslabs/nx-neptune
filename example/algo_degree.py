import networkx as nx

from nx_neptune import NETWORKX_GRAPH_ID
from nx_neptune.utils.utils import get_stdout_logger

""" 
Example script to demonstrate how Degree algorithm computation can be offloaded into remote AWS Neptune Analytics instance.  
"""


"""Read and load graphId from environment variable. """
if not NETWORKX_GRAPH_ID:
    raise Exception('Environment Variable "NETWORKX_GRAPH_ID" is not defined')
nx.config.warnings_to_ignore.add("cache")

logger = get_stdout_logger(__name__,[
                    'nx_neptune.algorithms.centrality.degree_centrality',
                    'nx_neptune.na_graph', 'nx_neptune.utils.decorators', __name__])

backend = "neptune"
# Clean up remote graph and populate test data.
g = nx.DiGraph()
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
g.add_edge('E', 'C', weight=1)
# Add an isolated node
g.add_node("X(DCd)")


logger.info("\n---------scenario: NetworkX - Degree----------\n")
# scenario: NetworkX
r = nx.degree_centrality(g)
logger.info("Algorithm execution - NetworkX: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")


logger.info("\n---------scenario: AWS - Degree----------\n")
# scenario: AWS
r = nx.degree_centrality(g, backend="neptune")
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")

logger.info("\n---------scenario: AWS - AWS-specific Options----------\n")
# scenario: AWS
r = nx.degree_centrality(g, backend="neptune", vertex_label="Node", edge_labels=["RELATES_TO"], concurrency=0)
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")



logger.info("\n---------scenario: NetworkX - In Degree----------\n")
# scenario: AWS - In Degree Centrality
r = nx.in_degree_centrality(g, backend="neptune")
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")

logger.info("\n---------scenario: AWS - In Degree----------\n")
# scenario: AWS - In Degree Centrality
r = nx.in_degree_centrality(g, backend="neptune")
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")


logger.info("\n---------scenario: AWS - In Degree - AWS-specific Options----------\n")
# scenario: AWS
r = nx.in_degree_centrality(g, backend="neptune", vertex_label="Node", edge_labels=["RELATES_TO"], concurrency=0)
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")



logger.info("\n---------scenario: NetworkX - Out Degree----------\n")
# scenario: AWS - In Degree Centrality
r = nx.out_degree_centrality(g, backend="neptune")
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")

logger.info("\n---------scenario: AWS - Out Degree----------\n")
# scenario: AWS - Out Degree Centrality
r = nx.out_degree_centrality(g, backend="neptune")
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")

logger.info("\n---------scenario: AWS - Out Degree - AWS-specific Options----------\n")
# scenario: AWS
r = nx.out_degree_centrality(g, backend="neptune", vertex_label="Node", edge_labels=["RELATES_TO"], concurrency=0)
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")

