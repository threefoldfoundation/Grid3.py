from threading import Thread
from queue import Queue

import grid3.proxy
import grid3.graphql

class GridNetwork():
    """
    A container for GridProxy, GraphQL, and maybe later also TF Chain client corresponding to a given network. Since different operations are more or less efficient using different data sources (querying a single node or farm is fast on Grid Proxy, while getting the set of all nodes or farms is faster with GraphQL), we could add some meta methods here that use the best underlying option.
    """

    def __init__(self, net='main'):
        self.net = net

        if net == 'main':
            proxy_url = 'https://gridproxy.grid.tf/'
        else:
            proxy_url = 'https://gridproxy.{}.grid.tf/'.format(net)

        self.proxy = grid3.proxy.GridProxy(proxy_url)

        if net == 'main':
            graphql_url = 'https://graphql.grid.tf/graphql'
        else:
            graphql_url = 'https://graphql.{}.grid.tf/graphql'.format(net)
            
        self.graphql = grid3.graphql.GraphQL(graphql_url)

    def get_node(self, node_id):
        """
        Return basic info about a node, or None if node id is invalid. Designed to be fast and responsive even if one data source is down.

        Fun fact! My measured time for proxy requests is ~.2s and for GraphQL it's ~.6s (including time to build the query)
        """

        q = Queue()
        Thread(None, return_to_queue, args=[self.proxy.get_node, q, node_id]).start()
        Thread(None, return_to_queue, args=[self.graphql.nodes, q, ['nodeID', 'twinID', 'farmID']], kwargs={'nodeID_eq': node_id}).start()

        reply = q.get()

        if not reply or 'error' in reply:
            return None
        else:
            return reply

def return_to_queue(func, queue, *args, **kwds):
    queue.put(func(*args, **kwds))
