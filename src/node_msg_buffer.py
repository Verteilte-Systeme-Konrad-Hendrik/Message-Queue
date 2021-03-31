import node_misc
import node_message
import node_info


def get_msg_for_seq(seq_number):
    msg_content = node_misc.get_example_message_content()
    return node_message.make_queue_message(node_info.getNodeInfo().uuid, seq_number, msg_content)

def has_msg():
    return node_misc.has_message()
    
