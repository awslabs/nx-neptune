from nx_neptune_analytics import NeptuneAnalyticsClient
import logging
import os

""" 
This is an sample script which demonstrate how the client can be used to invoke bfs algorithm remotely
over Neptune Analytic cluster.
"""

logger = logging.getLogger(__name__)
logging.basicConfig(filename="stdout.log", level=os.getenv("LOGLEVEL", "INFO").upper())

# Provide the graph ID as constructor argument. 
client = NeptuneAnalyticsClient("g-r4g1koz7v9")

client.clear_graph()

client.add_node('Person {name: \'Alice\'}')
client.add_node('Person {name: \'Bob\'}')
client.add_node('Person {name: \'Kathy\'}')
client.add_node('Person {name: \'Ken\'}')
client.add_node('Person {name: \'Ben\'}')


client.add_edge('Person {name: \'Alice\'}',
            'Person {name: \'Bob\'}',
            'FRIEND_WITH')

client.add_edge('Person {name: \'Bob\'}',
            'Person {name: \'Kathy\'}',
            'FRIEND_WITH')

client.add_edge('Person {name: \'Kathy\'}',
            'Person {name: \'Ken\'}',
            'FRIEND_WITH')

# Run BFS from node Alice.
result = client.execute_algo_bfs('n', 'n.name=\"Alice\"')
for item in result:
    print(item)
