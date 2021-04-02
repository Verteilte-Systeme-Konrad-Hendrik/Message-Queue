import node_misc
import node_message
import node_info


msg_content = {}

def add_msg(message, seq_number):
    msg_content[seq_number] = message

def get_msg_for_seq(seq_number):
    # this line right here doesn't make much sense, this function is supposed to be immutable
    # using pop will change the msg_content... Just don't!
    # msg_content = msg_content.pop(0)
    return node_message.make_queue_message(node_info.getNodeInfo().uuid, seq_number, msg_content[seq_number])

def has_msg(seq_number):
    return seq_number in msg_content
    
