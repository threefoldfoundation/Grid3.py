import requests

from grid_types import Node, Farm, Twin

class GridProxy():
    """
    Abstraction of a Grid Proxy endpoint at a given URL, corresponding to a given network. 
    """

    def __init__(self, url):
        if url[-1] != '/':
            url += '/'
        self.url = url

    def fetch_pages(self, slug):
        results = []
        page = 1
        while r := requests.get(self.url + slug + '?page=' + str(page)).json():
            results += r
            page += 1

        return results

    def get_farm(self, farm_id):
        r = requests.get(self.url + 'farms?farm_id=' + str(farm_id))
        return Farm(**r.json()[0])

    def get_farms(self):
        """
        Return all farms belonging to this network
        """
        return [Farm(**farm) for farm in self.fetch_pages('farms')]

    def get_node(self, node_id):
        r = requests.get(self.url + 'nodes/' + str(node_id))
        return Node(**r.json())

    def get_nodes(self):
        """
        Return all nodes belonging to this network
        """
        return [Node(**node) for node in self.fetch_pages('nodes')]

    def get_twin(self, twin_id):
        r = requests.get(self.url + 'twins?twin_id=' + str(twin_id))
        return Twin(**r.json()[0])

    def get_twins(self):
        """
        Return all twins belonging to this network
        """
        return [Twin(**twin) for twin in self.fetch_pages('twins')]


    def get_stats(self):
        return requests.get(self.url + 'stats').json()