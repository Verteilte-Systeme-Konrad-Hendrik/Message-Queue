import orchestration_pb2 as orch_pb
import orchestration_pb2_grpc as orch_pb_grpc
import uuid

class NodeMessage:

    def __init__(self, msg):
        self.sender = uuid.UUID(msg.sender.nodeId)
        self.seq_number = msg.sequence_number
        self.message_content = msg.message_content

    def __hash__(self):
        msg_string = str(self.sender.hex) + str(self.seq_number) + str(hash(self.message_content))
        return hash(msg_string)

    def get_pb_queue_message(self):
        return orch_pb.QueueMessage(
            sender=orch_pb.NodeId(
                nodeIf=self.sender.hex
            ),
            sequence_number=self.seq_number,
            message_content=self.message_content
        )
