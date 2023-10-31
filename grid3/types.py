# We use Farm, Node, and Twin essentially as data containers with some fancy features. These are designed to be instantiated with the output from a Grid Proxy or GraphQL call that returns the object of the same name. We coerce non string types, namely int and bool to their appropriate type. A synonym system is provided to account for the differences in names between different data sources. Ultimately the result is objects that can be used interchangeably, regardless of their original data source

def replace(dictionary, replacements):
    for replacement in replacements:
        if replacement[0] in dictionary:
            dictionary[replacement[1]] = dictionary.pop(replacement[0])

def mirror(dictionary):
    # Returns a dict that has a {value: key} for every {key: value} in the input
    return dict(map(reversed, dictionary.items()))

def cast_types(items, types, synonyms):
    #TODO: handles nested types like location
    new_items = {}
    for item in items:
        if item in types:
            new_items[item] = types[item](items[item])
        elif item in synonyms and synonyms[item] in types:
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
        except KeyError:
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
        except KeyError:
            raise AttributeError("'Node' object has no attribute '{}'".format(name))

    def __repr__(self):
        return "Node({})".format(self.__dict__)

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
        except KeyError:
            raise AttributeError("'Twin' object has no attribute '{}'".format(name))
