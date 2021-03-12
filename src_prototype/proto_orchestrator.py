import socket
import uuid
import time

import proto_orchestrator_classes as poc
from proto_orchestrator_classes import node
from proto_orchestrator_classes import pool


# The prototype orchestrator server

nodes_per_pool = 2
children_per_pool = 2

# orchestrator socket
orch_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
orch_sock.bind(("127.0.0.1", 5005))

poc.main_socket = orch_sock

next_node_port = 5050

# Tree
root_pool = pool(None)


# Tree Insertion for new nodes:
#   - search lowest level (top down)
#   - Find pool with min number of nodes
#   IF number of nodes in pool < N
#   - Insert into this pool
#   ELSE
#   - create new level with M children for each pool in higher level
#   - insert into pool with min number of entries

def get_lowest_level():
    # traverse tree to find leaves
    current_pool_list = [root_pool]
    current_children_pools = root_pool.children
    while len(current_children_pools) > 0:
        if len(current_children_pools) > len(current_pool_list):
            current_pool_list = current_children_pools
        else:
            raise Exception("Child list shorter than parent... This is not a tree")
        next_children_pools = []
        for children_pool in current_children_pools:
            next_children_pools += children_pool.children
        current_children_pools = next_children_pools
    return current_pool_list


def get_insert_pool(the_node: node):
    lowest_tree_level = get_lowest_level()
    # get minimum from level
    min_used_pool = min(lowest_tree_level, key=lambda x: len(x.members))
    # check level for space
    if len(min_used_pool.members) < nodes_per_pool:
        # Insert into this pool
        return min_used_pool
    else:
        # Create new level
        for lt_pool in lowest_tree_level:
            new_children = [pool(lt_pool) for _ in range(children_per_pool)]
            # Add children to the parent
            lt_pool.add_children(new_children)
        
        # Make recursive call to insert node
        print("Inserting recursive")
        return get_insert_pool(the_node)

print("Started up, entering main loop")

while True:
    data, adr = orch_sock.recvfrom(1024)
    msg = data.decode('UTF-8')
    
    print("Message received")

    node_ip = "127.0.0.1"
    node_port = next_node_port
    node_uuid = uuid.uuid4()

    orch_sock.sendto(str.encode("initial_info["+node_uuid.hex+","+node_ip+","+str(node_port)+"]"), adr)
    # Replace this by some ACK message 
    time.sleep(1)

    # make node object
    the_node = node(node_uuid, node_ip, node_port)
    
    insert_pool = get_insert_pool(the_node)

    insert_pool.introduce_pool_to_member(the_node)
    insert_pool.introduce_parents_to_member(the_node)
    insert_pool.introduce_children_to_member(the_node)

    insert_pool.add_member(the_node)

    next_node_port += 1

# a pool is a interconnected list
# each pool has a fully connected parent pool


