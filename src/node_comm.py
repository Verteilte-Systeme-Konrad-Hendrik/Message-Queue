import grpc
import orchestration_pb2 as orch_pb
import orchestration_pb2_grpc as orch_pb_grpc
import node_stats
import uuid
import time
from functools import partial
import node_stub
import node_info

# milliseconds
heartbeat_intervall = 5000
heartbeat_timeout = 500.0

# Servicer
class NodeCommunication(orch_pb_grpc.NodeCommunicationServicer):

    def heartBeat(self, request, context):
        source_uuid = uuid.UUID(request.nodeId)
        if node_info.getNodeInfo().get_related_node(source_uuid) is not None:
            node_stats.last_heartbeat[source_uuid] = time.time() * 1000 # store time in ms
            print("heartbeat received")
        else:
            print("Received heartbeat from unknown source")
        return orch_pb.Empty()

# Callbacks

def hearBeatCallback(node_id, future):
    if not future.cancelled() and future.exception() is None:
        node_stats.last_heartbeat[node_id] = time.time() * 1000 # store time in ms
    else:
        on_heartbeat_failed(node_id)
        print("heartbeat failed")


def add_heartbeats(members: []):
    for m in members:
        node_stats.last_heartbeat[m.uuid] = time.time() * 1000 - 10000000 # much earlier time


def on_heartbeat_failed(node_id):
    # what happens if heartbeat fails
    pass

def check_heartbeat():
    my_uuid = node_info.getNodeInfo().uuid
    print("Sending heartbeat")
    for key, value in node_stats.last_heartbeat.items():
        # check all five seconds
        if value < time.time() * 1000 - heartbeat_intervall:
            try:
                stub = node_stub.get_or_create_node_comm_stub(key)
                callback = stub.heartBeat.future(orch_pb.NodeId(nodeId=my_uuid.hex), timeout=heartbeat_timeout/1000.0)
                callback.add_done_callback(partial(hearBeatCallback, key))
            except Exception as e:
                print("Connection failed!", e)
                on_heartbeat_failed(key)

def send_to_all_pool_members_callback(message_hash, future):
    pass

def send_to_all_pool_members(msg):
    my_members = [m.uuid for m in node_info.getNodeInfo().pool_members]

    for mate in my_members:
        stub = node_stub.get_or_create_node_comm_stub(mate)
        callback = stub.pushQueueMessage.future(msg)
        callback.add_done_callback(partial(send_to_all_pool_members_callback, msg))
