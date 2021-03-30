import orchestration_pb2 as orch_pb
import node_info
import node_message

# def make_example_message(sender, seq_number, content):
#     return orch_pb.QueueMessage(sender=orch_pb.NodeId(nodeId=sender.hex), sequence_number=seq_number,
#                                 message_content=content)

def get_example_messages():

    my_example_messages = {
        1: [node_message.make_queue_message(node_info.getNodeInfo().uuid, 1, ("This is a test {}".format(node_info.getNodeInfo().uuid.hex)).encode("UTF-8"))],
        2: [node_message.make_queue_message(node_info.getNodeInfo().uuid, 2, ("This is a test {}".format(node_info.getNodeInfo().uuid.hex)).encode("UTF-8"))],
        3: [node_message.make_queue_message(node_info.getNodeInfo().uuid, 3, ("This is a test {}".format(node_info.getNodeInfo().uuid.hex)).encode("UTF-8"))],
        4: [node_message.make_queue_message(node_info.getNodeInfo().uuid, 4, ("This is a test {}".format(node_info.getNodeInfo().uuid.hex)).encode("UTF-8"))],
        5: [node_message.make_queue_message(node_info.getNodeInfo().uuid, 5, ("This is a test {}".format(node_info.getNodeInfo().uuid.hex)).encode("UTF-8"))],

    }

    return my_example_messages
