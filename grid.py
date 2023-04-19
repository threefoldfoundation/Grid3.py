import threading, functools

import requests
import graphql
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.exceptions import TransportServerError
from gql.dsl import DSLSchema, DSLQuery, dsl_gql

# We use Farm, Node, and Twin essentially as dictionaries with the convenience of instance variable access using dot notation. No assumptions are made about which data are present (if any) or their types, and objects returned by GridProxy versus GraphQL, for example, will have more or less fields present. Because different data sources call the same items by slightly different names, we coerce incoming keys to a canonical set, namely those used by TF Chain. Performing this inside the object instantiation, rather than during retrieval of data, allows all transformations to appear in a central source.

def replace(dictionary, replacements):
    for replacement in replacements:
        if replacement[0] in dictionary:
            dictionary[replacement[1]] = dictionary.pop(replacement[0])

def mirror(dictionary):
    # Returns a dict that has a {value: key} for every {key: value} in the input
    return dict(map(reversed, dictionary.items()))

def cast_types(items, types, synonyms):
    new_items = {}
    for item in items:
        if item in types:
            new_items[item] = types[item](items[item])
        elif item in synonyms:
            new_items[item] = types[synonyms[item]](items[item])
        else:
            new_items[item] = items[item]
    return new_items

class Farm():
    synonyms = {'farmID': 'farmId',
                'twinID': 'twinId',
                'pricingPolicyID': 'pricingPolicyId',
                'certificationType': 'certification'}

    synonyms.update(mirror(synonyms))

    types = {'dedicatedFarm': bool,
             'farmId': int,
             'pricingPolicyID': int,
             'twinId': int}

    def __init__(self, *args, **kwds):
        # Take either a dict as positional arg or kwds
        if args:
            kwds = args[0]
        
        kwds = cast_types(kwds, self.types, self.synonyms)
        self.__dict__.update(kwds)

    def __getattr__(self, name):
        try:
            return self.__dict__[self.synonyms[name]]
        except AttributeError:
            raise AttributeError("'Farm' object has no attribute '{}'".format(name))

class Node():
    synonyms = {'nodeID': 'nodeId',
                'farmID': 'farmId',
                'twinID': 'twinId',
                'certificationType': 'certification'}

    synonyms.update(mirror(synonyms))

    types = {
        'nodeID': int,
        'connectionPrice': int,
        'created': int,
        'createdAt': int,
        'farmID': int,
        'farmingPolicyId': int,
        'gridVersion': int,
        'secure': bool,
        'twinID': int,
        'updatedAt': int,
        'uptime': int,
        'virtualized': bool
      }

    def __init__(self, *args, **kwds):
        # Take either a dict as positional arg or kwds
        if args:
            kwds = args[0]

        kwds = cast_types(kwds, self.types, self.synonyms)
        self.__dict__.update(kwds)

    def __getattr__(self, name):
        try:
            return self.__dict__[self.synonyms[name]]
        except AttributeError:
            raise AttributeError("'Node' object has no attribute '{}'".format(name))

    def __eq__(self, other):
        return self.nodeId == other.nodeId

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        # Okay, maybe nodes should need a node id and farm id at creation?
        # But, they can also be uniquely identified by twin, so maybe not
        try:
            nodeId = self.nodeId
        except AttributeError:
            nodeId = None
        try:
            farmId = self.farmId
        except AttributeError:
            farmId = None
        return 'Node(nodeId: {})'.format(nodeId)

class Twin():
    synonyms = {'twinID': 'twinId',
                'accountID': 'accountId'}

    synonyms.update(mirror(synonyms))

    types = {
        'gridVersion': int,
        'twinID': int}

    def __init__(self, *args, **kwds):
        # Take either a dict as positional arg or kwds
        if args:
            kwds = args[0]

        kwds = cast_types(kwds, self.types, self.synonyms)
        self.__dict__.update(kwds)

    def __getattr__(self, name):
        try:
            return self.__dict__[self.synonyms[name]]
        except AttributeError:
            raise AttributeError("'Twin' object has no attribute '{}'".format(name))

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

        self.proxy = GridProxy(proxy_url)

        if net == 'main':
            graphql_url = 'https://graphql.grid.tf/graphql'
        else:
            graphql_url = 'https://graphql.{}.grid.tf/graphql'.format(net)
            
        self.graphql = GraphQL(graphql_url)

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

class GraphQL():
    """
    Abstraction of a Grid GraphQL endpoint at a given URL, corresponding to a given network. 
    """

    def __init__(self, url=None, fetch_schema=True):
        self.transport = RequestsHTTPTransport(url=url, verify=True, retries=3)
        self.client = Client(transport=self.transport,
                             fetch_schema_from_transport=True)

        # Really meant as a convenience for interactive use, probably not safe, or needed, for code that immediately runs queries. Actually, seems there's a lock on opening the transport, so it's not unsafe. We get some errors if we try to do stuff that requires the schema too soon, but do we really want a locking mechanism in here?
        if fetch_schema:
            threading.Thread(target=self.fetch_schema).start()

        self._dsl_schema = None

    def __getattr__(self, name):
        # We don't support the forms ending in "ById" and "ByUniqueInput", cause, who uses em?
        if name in self.client.schema.query_type.fields and "By" not in name:
            return functools.partial(self.build_dsl_query, name)
        else:
            raise AttributeError

    def fetch_schema(self):
        """Force fetching the schema. Client also does this automatically when executing queries, so is only needed for building DSL queries before any other exeuction."""

        with self.client as client:
            client.fetch_schema()

        return self.client.schema

    def create_dsl_schema(self):
        if not self.client.schema:
            self.fetch_schema()
        return DSLSchema(self.client.schema)

    @property
    def dsl_schema(self):
        if not self._dsl_schema:
            self._dsl_schema = self.create_dsl_schema()
        return self._dsl_schema

    def execute(self, query):
        return self.client.execute(gql(query))

    def execute_dsl_query(self, query):
        return self.client.execute(dsl_gql(DSLQuery(query)))
    
    def unwrap_type(self, gql_type):
        while 1:
            try:
                gql_type = gql_type.of_type
            except AttributeError:
                return gql_type

    def build_dsl_query(self, query_field_name, outputs, **kwds):
        """
        Generate queries using "domain specific language" features.

        This function generates queries in a convenient way that covers most use cases of our schema which corresponds to the data types in TF Chain. Here's a rough sketch of how the arguments are rendered into a query:

        query {
          query_field_name(kwds){
            output{
              output_subfield
            }
          }
        }
        """
        if not self.client.schema:
            self.fetch_schema()

        query_field = self.client.schema.query_type.fields[query_field_name]
        query_where_fields = query_field.args['where'].type.fields

        arguments = {}
        for arg in ('limit', 'offset', 'orderBy'):
            if arg in kwds:
                arguments[arg] = kwds.pop(arg)
        
        # TODO: also validate that arguments to 'orderBy' are valid, and maybe provide some "autocorrection", such that nodeID=1 becomes nodeID_eq=1 and nodeID=[1,2,3,4] becomes nodeID_in=[1,2,3,4]
        # Also, validate that subfields and their inputs are valid, eg: power={'target': 'down', 'state': 'down'})
        for arg in kwds:
            if arg not in query_where_fields.keys():
                raise graphql.error.GraphQLError('Not a valid "where" field: ' + arg)
        arguments['where'] = kwds
        query = self.dsl_schema.Query.__getattr__(query_field_name)(**arguments)

        # Return types are composite of NonNull and List, we just want the root
        return_type = self.unwrap_type(query_field.type)
        return_type_dsl = self.dsl_schema.__getattr__(return_type.name)

        # Outputs are either a string or a single item dict in the form of {'name': ['subfields', ...]}
        # For fields with subfields, specifying them via the dict form is optional and all non compound subfields except 'id' are returned
        for output in outputs:
            try:
                name, subfields = output.popitem()
            except AttributeError:
                name, subfields = output, None

            field_type = self.unwrap_type(return_type.fields[name].type)
           
            if graphql.type.is_scalar_type(field_type):
                query.select(return_type_dsl.__getattr__(name))
            elif subfields:
                subfields = [self.dsl_schema.__getattr__(field_type.name).__getattr__(name) for name in subfields]
                query.select(return_type_dsl.__getattr__(name).select(*subfields))
            else:
                subfields = []
                for subfield_name, subfield in field_type.fields.items():
                    subfield_type = self.unwrap_type(subfield.type)
                    if graphql.type.is_scalar_type(subfield_type) and not subfield_name == 'id':
                        subfields.append(self.dsl_schema.__getattr__(field_type.name).__getattr__(subfield_name))
                query.select(return_type_dsl.__getattr__(name).select(*subfields))

        return self.execute_dsl_query(query)


        # Belonged to the old "node" specific function. Some good stuff to save for docstring above or other docs
        """
        Build and execute a query dynamically to fetch node data

        This method provides a convenient way to access most of the use cases for the Grid's GraphQL nodes endpoint (generalizing to all the others shouldn't be hard). For example, the following query:

        query {
          nodes(where: {farmID_eq: 1}, limit: 5) {
            nodeID
            twinID
          }
        }

        Can be executed with:

        nodes(['nodeID', 'twinID'], farmID_eq=1, limit=5)

        Notice that "where" argument fields get flattened into one set of keyword args including "limit" and the other top level args. This is okay because there are no possible conflicts.

        In cases where an output field has subfields, then can be specified or when unspecified will be filled automatically (so far only one layer deep). So both:

        nodes(['location'], nodeID_eq=1)

        And:

        nodes([{'location': ['latitude', 'longitude']}], nodeID_eq=1)

        Render to:

        query {
          nodes(where: {nodeID_eq: 1}) {
            location {
              latitude
              longitude
            }
          }
        }

        Node that 'id', a TF Chain identifier which is usually meaningless to clients, is omitted by default.
        """