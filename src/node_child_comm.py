import node_info

children_pools_heard_from = {}

def check_messages_from_all_children_received(seq_number):
    child_pools = node_info.get_distinct_child_pools()

    return child_pools == children_pools_heard_from[seq_number]


def add_children_heard_from(seq_number, sender_id):
    pool_id = node_info.get_pool_for_sender(sender_id)

    if seq_number not in children_pools_heard_from:
        children_pools_heard_from[seq_number] = set()

    children_pools_heard_from[seq_number].add(pool_id)
