import uuid

class node_info:
    def __init__(self, uuid = None, ip_address = None, port = None):
        self.uuid = uuid
        self.ipAddress = ip_address
        self.port = port

        self.parents = []
        self.children = []
        self.pool_members = []

    def get_related_node(self, node_id):
        for n in (self.parents + self.children + self.pool_members):
            if n.uuid == node_id:
                return n
        return None

def rpc_node_to_node_info(rpc_node):
    return node_info(
        uuid=uuid.UUID(rpc_node.nodeId.nodeId),
        ip_address=rpc_node.nodeIpAddress,
        port=rpc_node.port
    )

def rpc_node_list_to_node_info_list(nodes_list):
    return [rpc_node_to_node_info(n) for n in nodes_list.nodes]

nf = None

def getNodeInfo():
    global nf
    if nf is None:
        nf = node_info()
    return nf
