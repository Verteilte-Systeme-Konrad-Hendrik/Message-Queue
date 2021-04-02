import uuid

class node_info:
    def __init__(self, uuid = None, ip_address = None, port = None, pool = None):
        self.uuid = uuid
        self.ipAddress = ip_address
        self.port = port
        self.pool_id = pool

        self.parents = []
        self.children = []
        self.pool_members = []

    def get_related_node(self, node_id):
        for n in (self.parents + self.children + self.pool_members):
            if n.uuid == node_id:
                return n
        return None

def rpc_node_to_node_info(rpc_node):
    print(rpc_node.poolId.poolId)
    return node_info(
        uuid=uuid.UUID(rpc_node.nodeId.nodeId),
        ip_address=rpc_node.nodeIpAddress,
        port=rpc_node.port,
        pool=uuid.UUID(rpc_node.poolId.poolId)
    )

def rpc_node_list_to_node_info_list(nodes_list):
    return [rpc_node_to_node_info(n) for n in nodes_list.nodes]

nf = None

def getNodeInfo():
    global nf
    if nf is None:
        nf = node_info()
    return nf

def get_distinct_child_pools():
    my_node = getNodeInfo()

    c_uuids = set()

    for c in my_node.children:
        c_uuids.add(c.pool_id)

    return c_uuids

def get_child_uuids_by_pool():
    pools = {p: [] for p in get_distinct_child_pools()}

    for c in getNodeInfo().children:
        if c.pool_id not in pools:
            pools[c.pool_id] = []
        pools[c.pool_id].append(c.uuid)
    
    return pools

def get_pool_for_sender(sender_uuid):
    my_node = getNodeInfo()

    all_members = my_node.pool_members + my_node.children + my_node.parents

    for m in all_members:
        if m.uuid == sender_uuid:
            return m.pool_id
    
    return None
