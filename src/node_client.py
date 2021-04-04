import grpc
import orchestration_pb2 as orch_pb
import orchestration_pb2_grpc as orch_pb_grpc
import node_msg_buffer as nmb
import node_comm

internal_seq_number = 0

class ClientCommServicer(orch_pb_grpc.ClientCommunicationServicer):

    def produceMessage(self, msg, context):
        global internal_seq_number
        nmb.add_msg(msg.message_content, internal_seq_number)
        internal_seq_number += 1
        return orch_pb.Empty()

    def triggerRound(self, round_nr, context):
        node_comm.trigger_round_start(round_nr.round)

        return orch_pb.Empty()
