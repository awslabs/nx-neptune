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
    'nx_neptune.clients.iam_client',
    __name__
])

nx.config.warnings_to_ignore.add("cache")

BACKEND = "neptune"
print(f"Using backend={BACKEND}")

nx.config.backends.neptune.create_new_instance = True
nx.config.backends.neptune.import_s3_bucket = "<your-s3-bucket>/cit-Patents"
nx.config.backends.neptune.s3_iam_role = "<your-role>"
nx.config.backends.neptune.export_s3_bucket = "<your-s3-bucket>/export"
nx.config.backends.neptune.destroy_instance = True

g = nx.DiGraph()

logger.info("\n-------------------\n")
r = list(nx.bfs_edges(g, "3858244", backend=BACKEND, reverse=True, depth_limit=1))
print(r)