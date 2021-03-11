import socket
import re

pattern = r"\[([0-9a-f]{8}-?([0-9a-f]{4}-?){3}[0-9a-f]{12}),([0-9]{1,3}(.[0-9]{1,3}){3}),([0-9]{1,5})\]"

graph_change_command_types = r"add_children|add_parents|add_pool"

node_spec_command_types = r"initial_info"

def process_message(recv_msg, handle_add_children=None, handle_add_parents=None,
                    handle_add_pool=None, handle_init=None):
    pool_change_command_match = re.search(graph_change_command_types, recv_msg)
    node_spech_change_match = re.search(node_spec_command_types, recv_msg)
    
    print(recv_msg)

    if pool_change_command_match:

        comm = pool_change_command_match.group(0)
        
        node_list = parse_node_list(recv_msg) 

        if comm == "add_children":
            handle_add_children(node_list)
        elif comm == "add_parents":
            handle_add_parents(node_list)
        elif comm == "add_pool":
            handle_add_pool(node_list)
    elif node_spech_change_match:

        comm = node_spech_change_match.group(0)
        
        node_list = parse_node_list(recv_msg)
        
        print(node_list)

        if len(node_list) > 1:
            raise Exception("Too many nodes specified, can only be one node")

        handle_init(node_list[0])

    else:
        raise Exception("Weird command, maybe UDPs fauls :D")

def parse_node_list(msg):
    results = []

    matches = re.finditer(pattern, msg)
    for match in matches:
        results.append((match.group(1), match.group(3), int(match.group(5))))

    return results


