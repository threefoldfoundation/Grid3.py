import threading, functools

import graphql
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.exceptions import TransportServerError
from gql.dsl import DSLSchema, DSLQuery, dsl_gql


class GraphQL:
    """
    Abstraction of a Grid GraphQL endpoint at a given URL, corresponding to a given network.
    """

    def __init__(self, url=None, fetch_schema=True):
        self.transport = RequestsHTTPTransport(url=url, verify=True, retries=3)
        # This won't fetch the schema until needed later
        self.client = Client(transport=self.transport, fetch_schema_from_transport=True)
        self.fetching_schema = False
        # Really meant as a convenience for interactive use, probably not safe, or needed, for code that immediately runs queries. Actually, seems there's a lock on opening the transport, so it's not unsafe. We get some errors if we try to do stuff that requires the schema too soon, but do we really want a locking mechanism in here?
        # Now I'm getting a lot of errors like below that go away if I fetch_schema first
        # gql.transport.exceptions.TransportAlreadyConnected: Transport is already connected
        # Seems the issue is that it's not safe to share clients among different threads: https://github.com/graphql-python/gql/issues/314
        # The background fetching initiates a session, so this can be part of the issue, but I'm not sure why because we guard around trying to execute queries while the fetching is ongoing. Fetch session should be closed before the event is set due to client context manager

        if fetch_schema:
            self.fetching_schema = True
            self.schema_event = threading.Event()
            threading.Thread(target=self.fetch_schema, args=[True]).start()

        self._dsl_schema = None

    def __getattr__(self, name):
        # We don't support the forms ending in "ById" and "ByUniqueInput", cause, who uses em?
        if not self.client.schema:
            self.fetch_schema()

        if name in self.client.schema.query_type.fields and "By" not in name:
            return functools.partial(self.build_dsl_query, name)
        else:
            raise AttributeError

    def fetch_schema(self, background=False):
        """Force fetching the schema. Client also does this automatically when executing queries, so is only needed for building DSL queries before any other exeuction."""

        if background or not self.fetching_schema:
            with self.client as client:
                client.fetch_schema()
            self.fetching_schema = False
            try:
                self.schema_event.set()
            except AttributeError:
                pass
            return self.client.schema
        elif self.fetching_schema:
            self.schema_event.wait()
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
        query_where_fields = query_field.args["where"].type.fields

        arguments = {}
        for arg in ("limit", "offset", "orderBy"):
            if arg in kwds:
                arguments[arg] = kwds.pop(arg)

        # TODO: also validate that arguments to 'orderBy' are valid, and maybe provide some "autocorrection", such that nodeID=1 becomes nodeID_eq=1 and nodeID=[1,2,3,4] becomes nodeID_in=[1,2,3,4]
        # Also, validate that subfields and their inputs are valid, eg: power={'target': 'down', 'state': 'down'})
        for arg in kwds:
            if arg not in query_where_fields.keys():
                raise graphql.error.GraphQLError('Not a valid "where" field: ' + arg)
        arguments["where"] = kwds
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

            # Scalars and enums have no subfields. Are there others?
            # Could also try field_type.fields and except AttributeError
            if not graphql.type.is_composite_type(field_type):
                query.select(return_type_dsl.__getattr__(name))
            elif subfields:
                subfields = [
                    self.dsl_schema.__getattr__(field_type.name).__getattr__(name)
                    for name in subfields
                ]
                query.select(return_type_dsl.__getattr__(name).select(*subfields))
            else:
                subfields = []
                for subfield_name, subfield in field_type.fields.items():
                    subfield_type = self.unwrap_type(subfield.type)
                    if (
                        not graphql.type.is_composite_type(subfield_type)
                        and not subfield_name == "id"
                    ):
                        subfields.append(
                            self.dsl_schema.__getattr__(field_type.name).__getattr__(
                                subfield_name
                            )
                        )
                query.select(return_type_dsl.__getattr__(name).select(*subfields))

        return self.execute_dsl_query(query)[query_field_name]

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
