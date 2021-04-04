import uuid
import hashlib
import random
import string

def hash_two_uuids(uuid1, uuid2):
    m = hashlib.sha256()
    m.update(uuid1.bytes)
    m.update(uuid2.bytes)
    return m.digest()


# input params are lists of uuid's
def get_lists_for_nodes(my_uuid, my_members, target_pool):
    source_pool = [my_uuid] + my_members
    target_pool.sort()

    hashes_for_targets = {t: [] for t in target_pool}

    for t in target_pool:
        for s in source_pool:
            the_hash = hash_two_uuids(t, s)
            hashes_for_targets[t].append((s, the_hash))

    return source_pool, target_pool, hashes_for_targets


def get_closest_node(message, target_nodes, hashes_for_targets):
    msg_hash = hashlib.sha256(message).digest()
    results = []
    
    for t in target_nodes:
        min_ = int(2**256) + 1
        node = -1
        
        result_stack = [(node, min_)]
        
        for source_uuid, hsh in hashes_for_targets[t]:
            val = int.from_bytes(msg_hash, "big") ^ int.from_bytes(hsh, "big")
            result_stack.append((source_uuid, val))
        
        # sort by val
        result_stack.sort(key=lambda x: x[1])
        results.append([(a[0], t) for a in result_stack])
        
    return results


def pick_two_equal_distrib(array, target_nodes, source_nodes):
    results = {t: [] for t in target_nodes}
    times_parent_used = {s: 0 for s in source_nodes}
    
    for child_num, child_id in enumerate(target_nodes):
        num_inserts = 0
        for tpl in array[child_num]:
            if num_inserts == 2:
                break
            if times_parent_used[tpl[0]] < 2:
                if child_num == len(target_nodes) - 2 and num_inserts == 1:
                    zero_entry = [item for item in times_parent_used.items() if item[1]==0]
                    if len(zero_entry) > 0:
                        zero_entry = zero_entry[0]
                        underused_tpl = [a for a in array[child_num] if a[0] == zero_entry[0]][0]
                        results[child_id].append(underused_tpl)
                        times_parent_used[underused_tpl[0]] += 1
                        num_inserts += 1
                        continue
                
                results[child_id].append(tpl)
                times_parent_used[tpl[0]] += 1
                num_inserts += 1
    
    return results


def calculate_allocs(message, target_nodes, source_nodes, hashes_for_targets):
    # special case 1 to n
    if len(source_nodes) == 1:
        return {tn: [(sn, tn)] for tn in target_nodes for sn in source_nodes}
    array = get_closest_node(message, target_nodes, hashes_for_targets)
    return pick_two_equal_distrib(array, target_nodes, source_nodes)


def get_list_by_sender(result_list):
    by_sender_list = {}

    for t, v in result_list.items():
        for sender, _ in v:
            if sender not in by_sender_list:
                by_sender_list[sender] = []
            by_sender_list[sender].append(t)
    
    return by_sender_list




