import orchestrator_connection_manager as ocm
import orchestrator_rpc_functions as orf
import uuid

# Node representation
class node:
    def __init__(self, uuid, ip_address, port):
        self.ip_address = ip_address
        self.port = port
        self.uuid = uuid
        self.pool_id = None

        self.parents = [] # single pool
        self.children = [] # pools


    def add_pool_members(self, members: []):
        orf.add_members(ocm.get_connection_manager().get_node_stub(self), members)
        print("{} adding {} pool members".format(self.port, len(members)))

    def add_child_pool_members(self, members: []):
        orf.add_children(ocm.get_connection_manager().get_node_stub(self), members)
        print("{} adding {} child members".format(self.port, len(members)))

    def add_parent_pool_members(self, members: []):
        orf.add_parents(ocm.get_connection_manager().get_node_stub(self), members)
        print("{} adding {} parent members".format(self.port, len(members)))


class pool:
    def __init__(self, parent):
        self.members = []
        # If parents None than pool is root
        self.parent = parent
        self.children = []
        self.pool_id = uuid.uuid4()
    
    def add_children(self, child_pools: []):
        self.children += child_pools

    def add_member(self, member: node):
        # set pool Id
        member.pool_id = self.pool_id
        # notify other pool members
        for node in self.members:
            node.add_pool_members([member])

        # append member to model
        self.members.append(member)
        
        # notify the parents if present
        if self.parent is not None:
            self.notify_pool_new_child(member)

        # notify the children if present
        self.notify_pool_new_parent(member)

    def introduce_pool_to_member(self, member: node):
        member.add_pool_members(self.members)

    def introduce_children_to_member(self, member: node):
        all_children = []
        for child in self.children:
            all_children += child.members
        member.add_child_pool_members(all_children)

    def introduce_parents_to_member(self, member: node):
        if self.parent != None:
            member.add_parent_pool_members(self.parent.members)
        else:
            member.add_parent_pool_members([])

    def notify_pool_new_child(self, member: node):
        for parent in self.parent.members:
            parent.add_child_pool_members([member])

    def notify_pool_new_parent(self, member: node):
        for pool in self.children:
            for child in pool.members:
                child.add_parent_pool_members([member])



