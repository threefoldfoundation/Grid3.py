import grid_proxy, grid_graphql

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

        self.proxy = grid_proxy.GridProxy(proxy_url)

        if net == 'main':
            graphql_url = 'https://graphql.grid.tf/graphql'
        else:
            graphql_url = 'https://graphql.{}.grid.tf/graphql'.format(net)
            
        self.graphql = grid_graphql.GraphQL(graphql_url)