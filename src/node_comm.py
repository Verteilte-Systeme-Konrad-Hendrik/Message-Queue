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
import node_child_comm
import node_interpool_comm as nic
import node_message_store as nms
import node_msg_buffer as nmb
import threading


# milliseconds
heartbeat_intervall = 5000
heartbeat_timeout = 500.0
child_receive_timeout = 5000

# child requests running
receiving_children = set()

receiving_children_lock = threading.Lock()

def add_receiving_child(seq_number):
    try:
        receiving_children_lock.acquire(True)
        receiving_children.add(seq_number)
    finally:
        receiving_children_lock.release()


def delete_receiving_child(seq_number):
    try:
        receiving_children_lock.acquire(True)
        receiving_children.remove(seq_number)
    finally:
        receiving_children_lock.release()

def is_in_receiving_childs(seq_number):
    is_in_childs = False
    try:
        receiving_children_lock.acquire(True)
        is_in_childs = seq_number in receiving_children
    finally:
        receiving_children_lock.release()

    return is_in_childs

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

    # for interpool upwards communication
    # DEPRECATED
    def pushMessageToParent(self, bulk, context):
        messages = bulk.messages
        sender = uuid.UUID(bulk.sendingNode.nodeId)
        seq_number = bulk.sequence_number

        node_message_store.store_messages(messages)

        node_child_comm.add_children_heard_from(seq_number, sender)

        if node_child_comm.check_messages_from_all_children_received(seq_number):
            pass
            # trigger own push to parent

        return orch_pb.Empty()

    # this method asks it's assigned children
    # after that it distributes the messages to it's pool
    # than it ensures that it has synced with it's own pool
    # finally it returns the collected messages
    def requestChild(self, round_number, context):
        # only call recursively if we have children
        if len(node_info.getNodeInfo().children) > 0:
            # start thread for child requests / callbacks
            # ensure only one of these threads is running
            # wait until pool complete --> return messages
            # only launch if no other is running and query is not already buffered
            messages = async_child_request(round_number.round)
            if messages is not None:
                return orch_pb.QueueMessageBulk(sendingNode=orch_pb.NodeId(nodeId=node_info.getNodeInfo().uuid.hex), sequence_number=seq_number, messages=messages)
            else:
                context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
                return orch_pb.QueueMessageBulk(sendingNode=orch_pb.NodeId(nodeId="none"), sequence_number=0, messages=[])
        else:
            return orch_pb.QueueMessageBulk(sendingNode=orch_pb.NodeId(nodeId=node_info.getNodeInfo().uuid.hex), sequence_number=round_number.round, messages=[])

    def notifyParent(self, round_number, context):
        async_child_request(round_number.round)
        # find my assigned parent nodes
        # send the notify recusrively
        # check if I have parents before trying to call them
        if len(node_info.getNodeInfo().parents) > 0:
            # calculate my corresponding parents
            src_pool, target_pool, hash_for_target = nic.get_lists_for_nodes(node_info.getNodeInfo().uuid, list(map(lambda x: x.uuid, node_info.getNodeInfo().parents)))
            res_list = nic.calculate_allocs(str(seq_number).encode(), target_pool, src_pool, hash_for_target)
            sender_list = nic.get_list_by_sender(res_list)

            my_targets = sender_list[node_info.getNodeInfo().uuid]

            for target in my_targets:
                comm_stub = node_stub.get_or_create_node_comm_stub(target)
                comm_stub.notifyParent.future(round_number.round)
        
        return orch_pb.Empty()

def async_child_request(seq_number):
    thread = threading.Thread(target=request_children_messages, args=(seq_number,))
    if not is_in_receiving_childs(seq_number) and not node_pool_comm.check_bulks_complete(seq_number):
        add_receiving_child(seq_number)
        thread.start()

    waiting_time = time.time * 1000

    did_receive = False

    while (time.time * 1000 - waiting_time < child_receive_timeout):
        if node_pool_comm.check_bulks_complete(seq_number):
            did_receive = True
            break
        # check every 50ms
        time.sleep(0.05)

    delete_receiving_child(seq_number)

    if did_receive:
        unique_child_pools = node_info.get_distinct_child_pools()

        msgs_for_parent = get_lower_equal_messages_for_sequence(seq_number)
        
        return msgs_for_parent
    else:
        return None

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
    # notify up
    # set flag for waiting endpoints
    node_pool_comm.add_pool_complete(seq_number)
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

# TODO: check if node has children before calling this
# This method is used to query the children for messages
# This should be called asynchronously as this traverses the tree under certain conditions... Thus it can take very very long
def request_children_messages(seq_number):
    # we get the corresponding children based on the seq_number as message
    childs_by_pool = node_info.get_child_uuids_by_pool()

    # request from targets for each pool
    for p, c in childs_by_pool.items():
        src_pool, target_pool, hash_for_target = nic.get_lists_for_nodes(node_info.getNodeInfo().uuid, c)
        res_list = nic.calculate_allocs(str(seq_number).encode(), target_pool, src_pool, hash_for_target)
        sender_list = nic.get_list_by_sender(res_list)

        my_targets = sender_list[node_info.getNodeInfo().uuid]
        
        if node_child_comm.check_messages_from_all_children_received(seq_number):
           
            pass
        else:
            for node in my_targets:
                stub = node_stub.get_or_create_node_comm_stub(node.uuid)
                callback = stub.requestChild.future(orch_pb.Empty())
                callback.add_done_callback(partial(children_request_callback, seq_number, node.uuid))

        # send to my targets
        # Check if we already have msgs for this round
        # TODO: define proto endpoint
        # TODO: use callback function below

def send_to_children_pool(seq_number, pool_uuid, node_messages):
    childs_by_pool = node_info.get_child_uuids_by_pool()

    src_pool, target_pool, hash_for_target = nic.get_lists_for_nodes(node_info.getNodeInfo().uuid, childs_by_pool[pool_uuid])
    res_list = nic.calculate_allocs((str(seq_number)+"s").encode(), target_pool, src_pool, hash_for_target)
    sender_list = nic.get_list_by_sender(res_list)

    my_targets = sender_list[node_info.getNodeInfo().uuid]

    for target in my_targets:
        # filter out messages already sent
        msgs_to_send = [msg for msg in node_messages if node_child_comm.check_message_sent_to_child(seq_number, target, msg)]
        if len(msgs_to_send) > 0:
            queue_messages = node_message.message_array_to_pb_message_array(msgs_to_send)
            stub = node_stub.get_or_create_node_comm_stub(node.uuid)
            callback = stub.pushMessageToChild.future(orch_pb.QueueMessageBulk(sendingNode=orch_pb.NodeId(nodeId=node_info.getNodeInfo().uuid.hex, sequence_number=seq_number, message=queue_messages)))

            callback.add_done_callback(child_send_done_callback)

            node_child_comm.add_sent_to_child(seq_number, target, queue_messages)
        else:
            print("Already sent those messages")

    print("Sent to other children")

def children_request_callback(seq_number, child_id, future):
    # add heard from child
    # parse and store messages QueueMessageBulk
    # check if child comm complete
    # - yes --> communicate to pool
    # - no --> do nothing
    the_result = future.result()

    node_child_comm.add_children_heard_from(seq_number, child_id)
    nms.store_messages(the_result.messages)
    sender_pool = node_info.get_pool_for_sender(child_id)
    nms.store_messages_in_pool(the_result.messages, sender_pool)

    # send to all other pools that didn't send the messagebulk
    send_pools = [pool for pool in node_info.get_distinct_child_pools() if pool not in [sender_pool]]
    for send_pool in send_pools:
        send_to_children_pool(seq_number, send_pool, node_message.message_bulk_to_message_array(the_result.messages))

    if node_child_comm.check_messages_from_all_children_received(seq_number):
        # push the logic down into other function and correct it (previous did not capture all children messages)
        children_receiving_done(seq_number)

# this is called when the children have responded with their messages
def children_receiving_done(seq_number):
    send_pool(seq_number, get_lower_equal_messages_for_sequence(seq_number))

def get_lower_equal_messages_for_sequence(seq_number):
    unique_child_pools = node_info.get_distinct_child_pools()

    messages = []

    for child_pool in unique_child_pools:
        messages += node_message_store.get_message_for_pool_and_seq(child_pool, seq_number)
    # add my message if available
    if nmb.has_msg_for_seq(seq_number):
        messages.append(nmb.get_own_msg_for_seq(seq_number))

# just for debugging
def child_send_done_callback(future):
    print("sent to child")
    pass
