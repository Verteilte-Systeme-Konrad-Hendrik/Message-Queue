import node_misc

def get_own_msg_for_seq(seq_number):
    return node_misc.get_example_messages()[seq_number]

def has_msg_for_seq(seq_number):
    return seq_number in node_misc.get_example_messages()
    
