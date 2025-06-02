import asyncio

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
    'nx_neptune.utils.decorators',
    'nx_neptune.interface',
    __name__
])

nx.config.warnings_to_ignore.add("cache")

BACKEND = "neptune"
print(f"Using backend={BACKEND}")

nx.config.backends.neptune.create_new_instance = False
nx.config.backends.neptune.destroy_instance = False
nx.config.backends.neptune.skip_graph_reset = False

g = nx.barabasi_albert_graph(n=10000, m=1)

r = nx.pagerank(g, backend="neptune")
logger.info("Algorithm execution - Neptune Analytics: ")
# Last 10
for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True)[:10]:
    logger.info(f"{key}: {value}")

