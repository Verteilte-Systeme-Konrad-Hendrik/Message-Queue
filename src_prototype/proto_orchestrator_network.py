def genereate_node_list_text(node_list):
    result = ""

    for node in node_list:
        result += "["+node.uuid.hex+","+node.ip_address+","+str(node.port)+"]"

    return result

