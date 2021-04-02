import node_info

children_pools_heard_from = {}

children_pools_sent_to = {}

def check_messages_from_all_children_received(seq_number):
    child_pools = node_info.get_distinct_child_pools()

    if seq_number not in children_pools_heard_from:
        return False

    return child_pools == children_pools_heard_from[seq_number]


def add_children_heard_from(seq_number, sender_id):
    pool_id = node_info.get_pool_for_sender(sender_id)

    if seq_number not in children_pools_heard_from:
        children_pools_heard_from[seq_number] = set()

    children_pools_heard_from[seq_number].add(pool_id)


def add_sent_to_child(seq_number, child_uuid, messages):
    print("Adding sent flag for {} messages to child {} in seq {}".format(len(messages), child_uuid, seq_number))
    if seq_number not in children_pools_sent_to:
        children_pools_sent_to[seq_number] = {}

    if child_uuid not in children_pools_sent_to[seq_number]:
        children_pools_sent_to[seq_number][child_uuid] = set()
    
    for msg in messages:
        children_pools_sent_to[seq_number][child_uuid].add(hash(msg))
    
def check_message_sent_to_child(seq_number, child_uuid, msg):
    if seq_number not in children_pools_sent_to:
        return False
    if child_uuid not in children_pools_sent_to[seq_number]:
        return False
    
    return hash(msg) in children_pools_sent_to[seq_number][child_uuid]
