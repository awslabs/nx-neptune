import networkx as nx

from nx_neptune import NeptuneGraph, NETWORKX_GRAPH_ID, Node
from nx_neptune.utils.utils import get_stdout_logger

""" 
Example script to demonstrate how PageRank algorithm computation can be offloaded into remote AWS Neptune Analytics instance.  
"""


"""Read and load graphId from environment variable. """
if not NETWORKX_GRAPH_ID:
    raise Exception('Environment Variable "NETWORKX_GRAPH_ID" is not defined')
nx.config.warnings_to_ignore.add("cache")

logger = get_stdout_logger(__name__,[
                    'nx_neptune.algorithms.link_analysis.pagerank',
                    'nx_neptune.na_graph', 'nx_neptune.utils.decorators', __name__])

backend = "neptune"
# Clean up remote graph and populate test data.
g = nx.DiGraph()
na_graph = NeptuneGraph.from_config(graph=g)
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
g.add_edge('E', 'C', weight=1)
# Add an isolated node
g.add_node("X(DCd)")

nx.config.backends.neptune.skip_graph_reset = False

logger.info("\n-------------------\n")
# scenario: AWS
r = nx.pagerank(g, backend="neptune")
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")


logger.info("\n-------------------\n")
# scenario: AWS - vertex_label
r = nx.pagerank(g, backend="neptune", vertex_label="A")
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")

logger.info("\n-------------------\n")
# scenario: AWS - edge_labels
r = nx.pagerank(g, backend="neptune", edge_labels=["RELATES_TO"])
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")

logger.info("\n-------------------\n")
# scenario: AWS - concurrency
r = nx.pagerank(g, backend="neptune", concurrency=0)
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")

logger.info("\n-------------------\n")
# scenario: AWS - traversal_direction
r = nx.pagerank(g, backend="neptune", traversal_direction="inbound")
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")


logger.info("\n-------------------\n")
# scenario: AWS - edge_weight_type & edge_weight_property
r = nx.pagerank(g, backend="neptune", edge_weight_type="int", edge_weight_property="weight")
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")

logger.info("\n-------------------\n")
# scenario: AWS - source_nodes & source_weights
r = nx.pagerank(g, backend="neptune", source_nodes=["A", "B"], source_weights=[1, 1.5])
logger.info("Algorithm execution - Neptune Analytics: ")
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True):
    logger.info(f"{key}: {value}")

logger.info("\n-------------------\n")
# scenario: AWS - write_property
r = nx.pagerank(g, backend="neptune", write_property="rank")
logger.info("Algorithm execution - Neptune Analytics: ")
for item in na_graph.get_all_nodes()[:10]:
    logger.info(Node.from_neptune_response(item))
