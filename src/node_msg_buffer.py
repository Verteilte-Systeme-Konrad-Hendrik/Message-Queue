import node_misc
import node_message
import node_info
import threading


msg_content = {}
last_key = 0
last_key_lock = threading.Lock()


# In general it's only allowed to insert in seq which are not already started

def add_msg(message, seq_number):
    try:
        last_key_lock.acquire(True)
        last_key = max(seq_number, node_info.get_seq_number() + 1, last_key + 1)
        msg_content[last_key] = message

        inserted = last_key
    finally:
        last_key_lock.release()

    return inserted
    
def reg_msg(message):
    try:
        last_key_lock.acquire(True)
        last_key = max(last_key, node_info.get_seq_number())
        last_key += 1
        msg_content[last_key] = message
        
        inserted = last_key
    finally:
        last_key_lock.release()

    return inserted

def get_msg_for_seq(seq_number):
    # this line right here doesn't make much sense, this function is supposed to be immutable
    # using pop will change the msg_content... Just don't!
    # msg_content = msg_content.pop(0)
    print("returning my message for sequence {}: {}".format(seq_number, msg_content[seq_number]))
    return node_message.make_queue_message(node_info.getNodeInfo().uuid, seq_number, msg_content[seq_number])

def has_msg(seq_number):
    return seq_number in msg_content
    
