import node_info
import threading

# contains nodes that have been heard from in sequence
nodes_heard_from = {}

# contains received bulk sender uuid's in pool
pool_bulks = {}

# contains pool member nodes that have send an ack that says they wanted to send something
expected_bulks = {}

# check if I sent my status to pool members
did_send_to_nodes = set()

# log the complete receiving sequences for pools
pool_complete = set()

pool_complete_lock = threading.Lock()

def remember_bulk(sequence_number, sending_node):
    print("got bulk from "+str(sending_node))
    if sequence_number not in pool_bulks:
        pool_bulks[sequence_number] = set()
    
    pool_bulks[sequence_number].add(sending_node)

def did_receive_bulk(seq_number, sending_node):
    return seq_number in pool_bulks and sending_node in pool_bulks[seq_number]


def initialize_expected_bulk_dict(seq_number):
    expected_bulks[seq_number] = set()

def add_expected_bulk(seq_number, sending_node):
    expected_bulks[seq_number].add(sending_node)

def initialize_heard_from(seq_number):
    nodes_heard_from[seq_number] = set()

def add_heard_from(seq_number, node_id):
    nodes_heard_from[seq_number].add(node_id)

def have_heard_from_all_nodes(seq_number):
    my_node_info = node_info.getNodeInfo()
    print(nodes_heard_from[seq_number])
    for uuid in [m.uuid for m in my_node_info.pool_members]:
        if uuid not in nodes_heard_from[seq_number]:
            return False
    print("Have heard from all")
    return True

# only call after you've heard from all other pool members
def check_bulks_complete(seq_number):
    if seq_number in expected_bulks:
        if seq_number in pool_bulks:
            for entry in expected_bulks[seq_number]:
                if entry not in pool_bulks[seq_number]:
                    # if something is missing return false
                    print("missing bulk seq {} node {}".format(seq_number, entry.hex))
                    return False
            # if everything is there return true
            print("All bulks are here")
            return True
        else:
            # yes if expected bulks is zero size else no
            return len(expected_bulks[seq_number]) == 0
    else:
        # Bulk not opened
        return False

def add_pool_complete(seq_number):
    try:
        pool_complete_lock.acquire(True)
        pool_complete.add(seq_number)
    finally:
        pool_complete_lock.release()

def check_pool_complete(seq_number):
    seq_is_in = False
    try:
        pool_complete_lock.acquire(True)
        seq_is_in = seq_number in pool_complete
    finally:
        pool_complete_lock.release()
    return seq_is_in
