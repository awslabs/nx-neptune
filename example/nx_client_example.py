from nx_neptune_analytics import NeptuneAnalyticsClient
import logging
import os

""" 
This is an sample script which demonstrate how class NeptuneAnalyticsClient 
can be used to perform basic CRUD operation against an existing Neptune Analytics graph.
"""

logger = logging.getLogger(__name__)
logging.basicConfig(filename="stdout.log", level=os.getenv("LOGLEVEL", "INFO").upper())

"""Provide the graph ID as constructor argument. """
client = NeptuneAnalyticsClient(graphId="g-r4g1koz7v9")

""" The below lines of script demonstrate how basic node operation can be done, via client's API call. """

client.clear_graph()

"""Populate the dataset by inserting nodes into the graph"""
client.add_node('a:Person {name: \'Alice\'}')
client.add_node('a:Person {name: \'Bob\'}')
client.add_edge('(a)-[:FRIEND_WITH]->(b)',
                '(a:Person {name: \'Alice\'}), (b:Person {name: \'Bob\'})')

client.update_nodes('(a:Person)', 'a.name = \'Alice\'', 'a.age=\'25\'')

"""Update an edge"""
client.update_edges('(a:Person)-[r:FRIEND_WITH]->(b:Person)', 
               'a.name = \'Alice\' AND b.name = \'Bob\'', 
               'r.since = 1997')

""" To demonstrate how to print existing nodes and edges which exist on the graph """

"""List all nodes"""
for item in client.get_all_nodes():
    print(item)


"""List all edges"""
for item in client.get_all_edges():
    print(item)
