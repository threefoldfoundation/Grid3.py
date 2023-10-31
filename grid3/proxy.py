import requests
from requests.adapters import HTTPAdapter, Retry


from grid3.types import Node, Farm, Twin

class GridProxy():
    """
    Abstraction of a Grid Proxy endpoint at a given URL, corresponding to a given network. 
    """

    def __init__(self, url):
        if url[-1] != '/':
            url += '/'
        self.url = url

        self.session = requests.Session()

        retries = Retry(total=3, backoff_factor=0.3, 
                        status_forcelist=[ 500, 502, 503, 504 ])

        self.session.mount('https://', HTTPAdapter(max_retries=retries))


    def _get(self, url):
        reply = self.session.get(url, timeout=5)
        # Some 404s contain useful errors, like "node not found"
        if reply.text == '404 page not found\n':
            reply.raise_for_status()
        return reply

    def fetch_pages(self, slug):
        results = []
        page = 1
        while r := self._get(self.url + slug + '?page=' + str(page)).json():
            results += r
            page += 1

        return results

    def get_farm(self, farm_id):
        r = self._get(self.url + 'farms?farm_id=' + str(farm_id))
        return r.json()[0]

    def get_farms(self):
        """
        Return all farms belonging to this network
        """
        return self.fetch_pages('farms')

    def get_node(self, node_id):
        r = self._get(self.url + 'nodes/' + str(node_id))
        return r.json()

    def get_nodes(self):
        """
        Return all nodes belonging to this network
        """
        return self.fetch_pages('nodes')

    def get_twin(self, twin_id):
        r = self._get(self.url + 'twins?twin_id=' + str(twin_id))
        return r.json()[0]

    def get_twins(self):
        """
        Return all twins belonging to this network
        """
        return self.fetch_pages('twins')


    def get_stats(self):
        return self._get(self.url + 'stats').json()
