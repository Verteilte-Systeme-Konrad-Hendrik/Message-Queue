import grpc
import orchestration_pb2 as orch_pb
import orchestration_pb2_grpc as orch_pb_grpc
import node_stats
import uuid
import time
from functools import partial
import node_stub
import node_info
import node_pool_comm
import node_message_store
import node_message
import node_misc

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

    # for pool level communication
    def pushQueueMessages(self, bulk, context):
        messages = bulk.messages
        sender = uuid.UUID(bulk.sendingNode.nodeId)
        seq_number = bulk.sequence_number

        # Check if sender is a pool member
        if sender not in [nm.uuid for nm in node_info.getNodeInfo().pool_members]:
            # Should not happen as endpoint is only for on pool comm
            pass

        node_message_store.store_messages(messages)
        node_pool_comm.remember_bulk(seq_number, sender)

        if seq_number not in node_pool_comm.did_send_to_nodes:
            node_pool_comm.did_send_to_nodes.add(seq_number)
            send_pool(seq_number, node_misc.get_example_messages().get(seq_number, []))

        if check_pool_comm_complete(seq_number):
            pool_complete_callback(seq_number)
        # TODO: Ack
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

def pool_check_callback(node_id, seq_number, ack):
    print("Got callback")
    node_pool_comm.add_expected_bulk(seq_number, node_id)

    node_pool_comm.add_heard_from(seq_number, node_id)

    if check_pool_comm_complete(seq_number):
            pool_complete_callback(seq_number)

def check_pool_comm_complete(seq_number):
    print("Checking pool complete")
    return node_pool_comm.have_heard_from_all_nodes(seq_number) and node_pool_comm.check_bulks_complete(seq_number)

def pool_complete_callback(seq_number):
    print("pool complete for seq " + str(seq_number))

# check that pool is ready
# TODO: Know which message I have to send
def send_pool(seq_number, msgs):
    print("Sending")
    node_pool_comm.initialize_heard_from(seq_number)
    node_pool_comm.initialize_expected_bulk_dict(seq_number)

    for node in node_info.getNodeInfo().pool_members:
        stub = node_stub.get_or_create_node_comm_stub(node.uuid)
        callback = stub.pushQueueMessages.future(orch_pb.QueueMessageBulk(sendingNode=orch_pb.NodeId(nodeId=node_info.getNodeInfo().uuid.hex), 
                                                                            sequence_number=seq_number, 
                                                                            messages=msgs))
        callback.add_done_callback(partial(pool_check_callback, node.uuid, seq_number))

    node_pool_comm.did_send_to_nodes.add(seq_number)


# TBD
def send_to_all_pool_members_callback(message_sender, message_receiver, future):
    pass

# TODO: Make valid again
def send_to_all_pool_members(msg):
    my_members = [m.uuid for m in node_info.getNodeInfo().pool_members]

    for mate in my_members:
        stub = node_stub.get_or_create_node_comm_stub(mate)
        callback = stub.pushQueueMessage.future(msg.get_pb_queue_message())
        callback.add_done_callback(partial(send_to_all_pool_members_callback, msg.sender, mate))
