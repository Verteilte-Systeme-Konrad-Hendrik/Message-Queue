import orchestration_pb2 as orch_pb
import node_info
import node_message

# def make_example_message(sender, seq_number, content):
#     return orch_pb.QueueMessage(sender=orch_pb.NodeId(nodeId=sender.hex), sequence_number=seq_number,
#                                 message_content=content)


my_example_messages = [
        node_message.make_queue_message(node_info.getNodeInfo().uuid, 1, ("This is a test {}".format(node_info.getNodeInfo().uuid.hex)).encode("UTF-8")),
        node_message.make_queue_message(node_info.getNodeInfo().uuid, 2, ("This is a test {}".format(node_info.getNodeInfo().uuid.hex)).encode("UTF-8")),
        node_message.make_queue_message(node_info.getNodeInfo().uuid, 3, ("This is a test {}".format(node_info.getNodeInfo().uuid.hex)).encode("UTF-8")),
        node_message.make_queue_message(node_info.getNodeInfo().uuid, 4, ("This is a test {}".format(node_info.getNodeInfo().uuid.hex)).encode("UTF-8")),
        node_message.make_queue_message(node_info.getNodeInfo().uuid, 5, ("This is a test {}".format(node_info.getNodeInfo().uuid.hex)).encode("UTF-8")),
    ]


def get_example_message():
    global my_example_messages

    return my_example_messages.pop(0)


def has_message():
    global my_example_messages

    return len(my_example_messages) == 0
    
