import grpc
import orchestration_pb2 as orch_pb
import orchestration_pb2_grpc as orch_pb_grpc
import node_msg_buffer as nmb
import node_comm
import node_info
import node_message_store as nms
import threading
import time
import traceback

internal_seq_number = 0

listener_clients = []
current_client_port = 20000

listener_clients_lock = threading.Lock()

client_port_lock = threading.Lock()

def add_client(port):
    time.sleep(0.5)
    stub = try_connect_listener(port)

    try:
        listener_clients_lock.acquire(True)
        listener_clients.append(stub)
    finally:
        listener_clients_lock.release()

def get_client_listeners():
    return listener_clients

def pop_client_port():
    global current_client_port
    try:
        client_port_lock.acquire(True)
        return current_client_port
    finally:
        current_client_port += 1
        client_port_lock.release()

def try_connect_listener(port):
    client_channel = grpc.insecure_channel("localhost:"+str(port))
    client_stub = orch_pb_grpc.ServerFacingClientStub(client_channel)

    repeat = True
    while repeat:
        try:
            # to test connection
            client_stub.NotifyRoundFinished(orch_pb.RoundNumber(round=0))
            repeat = False
        except Exception as e:
            repeat = True
            time.sleep(0.1)

    return client_stub

def notify_all_clients_round_finished(seq_number):
    for s in listener_clients:
        s.NotifyRoundFinished.future(orch_pb.RoundNumber(round=seq_number))

class ClientCommServicer(orch_pb_grpc.ClientCommunicationServicer):

    def produceMessage(self, msg, context):
        global internal_seq_number
        inserted = nmb.add_msg(msg.message_content, internal_seq_number)
        internal_seq_number = inserted + 1
        return orch_pb.RoundNumber(round=inserted)

    def publishMessage(self, msg, context):
        round = nmb.reg_msg(msg.message_content)
        node_comm.trigger_round_start(round)
        return orch_pb.RoundNumber(round=round)

    def triggerRound(self, round_nr, context):
        node_comm.trigger_round_start(round_nr.round)

        return orch_pb.Empty()

    def pullMessageForRound(self, round_nr, context):
        try:
            print("Got message pull")
            if node_comm.check_sequence_all_finished(round_nr.round):
                messages = nms.get_messages_for_round(round_nr.round)

                return orch_pb.QueueMessageBulk(
                    sendingNode=orch_pb.NodeId(nodeId=node_info.getNodeInfo().uuid.hex), 
                    sequence_number=round_nr.round, messages=messages)
            else:
                raise Exception("Sequence has not been completed")
        except Exception as e:
            print(e)
            traceback.print_exc()

    def registerListener(self, _, context):
        future_port = pop_client_port()

        connect_thread = threading.Thread(target=add_client, args=(future_port,))

        connect_thread.start()

        return orch_pb.ClientInfo(port=future_port)
