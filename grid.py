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

class Farm():
    def __init__(self, **kwds):
        replacements = [('farmID', 'farmId'),
                        ('twinID', 'twinId'),
                        ('pricingPolicyID', 'pricingPolicyId'),
                        ('certificationType', 'certification'),
                       ]
        replace(kwds, replacements)
        self.__dict__.update(kwds)

class Node():
    def __init__(self, *args, **kwds):
        # Take either a dict as positional arg or kwds
        if args:
            kwds = args[0]
        # TODO: make replacement synonyms.
        replacements = [('nodeID', 'nodeId'),
                        ('farmID', 'farmId'),
                        ('twinID', 'twinId'),
                        ('certificationType', 'certification'),
                       ]
        replace(kwds, replacements)
        # TODO: do a "casting" system based on key/type tuples
        if 'uptime' in kwds:
            kwds['uptime'] = int(kwds['uptime'])
        self.__dict__.update(kwds)

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
    def __init__(self, **kwds):
        self.__dict__.update(kwds)

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

        if fetch_schema:
            self.fetch_schema()

        self._dsl_schema = None

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

    def get_farm(self, farm_id):
        query = gql(
        """
        query GetFarm ($farm_id: Int) {
            farms(where: {farmID_eq: $farm_id}) {
                certification
                dedicatedFarm
                farmID
                name
                pricingPolicyID
                stellarAddress
                twinID
            }
        }
        """
        )

        params = {'farm_id': farm_id}
        result = self.client.execute(query, variable_values=params)
        return result

    def get_farms(self):
        query = gql(
        """
        query GetFarms {
            farms {
                certification
                dedicatedFarm
                farmID
                name
                pricingPolicyID
                stellarAddress
                twinID
            }
        }
        """
        )

        result = self.client.execute(query)
        return result

    def get_node(self, node_id):
        query = gql(
        """
        query GetNode ($node_id: Int){
        nodes(where: {nodeID_eq: $node_id}) {
            certification
            city
            connectionPrice
            country
            created
            createdAt
            farmID
            farmingPolicyId
            gridVersion
            id
            location {
                latitude
                longitude
            }
            nodeID
            secure
            serialNumber
            twinID
            updatedAt
            uptime
            virtualized
            interfaces {
                ips
                mac
                name
            }
            publicConfig {
                domain
                gw4
                gw6
                ipv4
                ipv6
            }
            resourcesTotal {
                cru
                hru
                mru
                sru
            }
        }
        }
        """
        )
    
    def unwrap_type(self, gql_type):
        while 1:
            try:
                gql_type = gql_type.of_type
            except AttributeError:
                return gql_type

    def nodes(self, outputs, **kwds):
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

        if not self.client.schema:
            self.fetch_schema()

        query_field = self.client.schema.query_type.fields['nodes']
        query_where_fields = query_field.args['where'].type.fields

        arguments = {}
        for arg in ('limit', 'offset', 'orderBy'):
            if arg in kwds:
                arguments[arg] = kwds.pop(arg)
        
        # TODO: also validate that arguments to 'orderBy' are valid, and maybe provide some "autocorrection", such that nodeID=1 becomes nodeID_eq=1 and nodeID=[1,2,3,4] becomes nodeID_in=[1,2,3,4]
        for arg in kwds:
            if arg not in query_where_fields.keys():
                raise graphql.error.GraphQLError('Not a valid "where" field: ' + arg)
        arguments['where'] = kwds
        query = self.dsl_schema.Query.nodes(**arguments)

        return_type = self.client.schema.type_map['Node']
        # or self.dsl_schema.Node._type

        # Outputs are either a string or a single item dict in the form of {'name': ['subfields', ...]}
        # For fields with subfields, specifying them via the dict form is optional and all non compound subfields except 'id' are returned
        for output in outputs:
            try:
                name, subfields = output.popitem()
            except AttributeError:
                name, subfields = output, None

            field_type = self.unwrap_type(return_type.fields[name].type)
           
            if graphql.type.is_scalar_type(field_type):
                query.select(self.dsl_schema.Node.__getattr__(name))
            elif subfields:
                subfields = [self.dsl_schema.__getattr__(field_type.name).__getattr__(name) for name in subfields]
                query.select(self.dsl_schema.Node.__getattr__(name).select(*subfields))
            else:
                subfields = []
                for name, subfield in field_type.fields.items():
                    subfield_type = self.unwrap_type(subfield.type)
                    if graphql.type.is_scalar_type(subfield_type) and not name == 'id':
                        subfields.append(self.dsl_schema.__getattr__(field_type.name).__getattr__(name))
                query.select(self.dsl_schema.Node.__getattr__(name).select(*subfields))

        return self.execute_dsl_query(query)

    def __getattr__(self, name):
        # Want to generalize to the full schema. We have two kinds of fields, ones that return lists and ones that return single objects. These also have (?) consistent arg specs
        if name in self.client.schema.query_type.fields:
            pass