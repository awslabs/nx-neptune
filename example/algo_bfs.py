from nx_neptune_analytics import NeptuneAnalyticsClient
import logging
import os

""" 
This is an sample script to demonstrate how AWS Neptune Analytic service 
can be leveraged to offload graph algorithm computation from local cluster.

"""
logger = logging.getLogger(__name__)
logging.basicConfig(filename="stdout.log", level=os.getenv("LOGLEVEL", "INFO").upper())

"""Provide the graph ID as constructor argument.""" 
client = NeptuneAnalyticsClient("g-r4g1koz7v9")
client.clear_graph()

"""Adding nodes and edges to composite a simple graph on AWS NA graph for test dataset """
client.add_node('a:Person {name: \'Alice\'}')
client.add_node('a:Person {name: \'Bob\'}')
client.add_node('a:Person {name: \'Kathy\'}')
client.add_node('a:Person {name: \'Ken\'}')
client.add_node('a:Person {name: \'Ben\'}')

client.add_edge('(a)-[:FRIEND_WITH]->(b)',
                '(a:Person {name: \'Alice\'}), (b:Person {name: \'Bob\'})')
client.add_edge('(a)-[:FRIEND_WITH]->(b)',
                '(a:Person {name: \'Bob\'}), (b:Person {name: \'Kathy\'})')
client.add_edge('(a)-[:FRIEND_WITH]->(b)',
                '(a:Person {name: \'Kathy\'}), (b:Person {name: \'Ken\'})')

""" Trigger the remote execution of BFS algorithm with provided dataset."""
result = client.execute_algo_bfs('n', 'n.name=\"Alice\"')

""" Printing the result"""
for item in result:
    print(item)
