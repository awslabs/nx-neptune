# Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import asyncio
import os
import networkx as nx
from nx_neptune import instance_management
from nx_neptune.utils.utils import get_stdout_logger
""" 
This sample script demonstrates AWS Neptune Analytics, which can be paused/resumed via instance_management API calls
in order to lower operational costs when the instance is not being used temporarily.
In this example, an instance will be created with demo data, then execute the PageRank algorithm.
Once PageRank has been computed, we will pause the instance and eventually resume it to run the same PageRank algorithm again.
"""


async def main():
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
    logger.info("Algorithm execution - Neptune Analytics (Before): ")
    # Last 10
    for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True)[:10]:
        logger.info(f"{key}: {value}")

    graph_id = os.getenv("NETWORKX_GRAPH_ID")
    # Pause the instance
    await instance_management.stop_na_instance(graph_id)
    # Resume the instance
    await instance_management.start_na_instance(graph_id)

    # Re-run the algorithm after resuming
    r = nx.pagerank(g, backend="neptune")
    logger.info("Algorithm execution - Neptune Analytics (After): ")
    # Last 10
    for key, value in sorted(r.items(), key=lambda x: (x[1], x[0]), reverse=True)[:10]:
        logger.info(f"{key}: {value}")


if __name__ == "__main__":
    asyncio.run(main())

