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
import traceback


# milliseconds
heartbeat_intervall = 5000
heartbeat_timeout = 500.0
child_receive_timeout = 5000
pool_receive_timeout = 5000

# child requests running
receiving_children = set()

receiving_children_lock = threading.Lock()

sequences_finished_all = set()

all_finished_sequences_lock = threading.Lock()

def add_all_finished_sequence(seq_number):
    try:
        all_finished_sequences_lock.acquire()
        sequences_finished_all.add(seq_number)
    finally:
        all_finished_sequences_lock.release()

def check_sequence_all_finished(seq_number):
    try:
        all_finished_sequences_lock.acquire()
        return seq_number in sequences_finished_all
    finally:
        all_finished_sequences_lock.release()


sequences_finished_below = set()

finished_sequences_lock = threading.Lock()

def check_sequence_finished(seq_number):
    try:
        receiving_children_lock.acquire(True)
        return seq_number in sequences_finished_below
    finally:
        receiving_children_lock.release()

def add_finished_sequence(seq_number):
    try:
        print("Finished sequence")
        node_info.inc_seq_number()
        receiving_children_lock.acquire(True)
        sequences_finished_below.add(seq_number)
    finally:
        receiving_children_lock.release()

def add_receiving_child(seq_number):
    try:
        receiving_children_lock.acquire(True)
        receiving_children.add(seq_number)
    finally:
        receiving_children_lock.release()


def delete_receiving_child(seq_number):
    try:
        receiving_children_lock.acquire(True)
        if seq_number in receiving_children:
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
            # print("heartbeat received")
        else:
            print("Received heartbeat from unknown source")
        return orch_pb.Empty()

    # for pool level communication
    def pushQueueMessages(self, bulk, context):
        messages = bulk.messages
        sender = uuid.UUID(bulk.sendingNode.nodeId)
        seq_number = bulk.sequence_number

        print("Pool member asking for messages from sequence {}".format(seq_number))

        # Check if sender is a pool member
        if sender not in [nm.uuid for nm in node_info.getNodeInfo().pool_members]:
            # Should not happen as endpoint is only for on pool comm
            pass
        
        if check_sequence_finished(seq_number) and node_message_store.is_a_message_not_already_stored(converted_messages):
            print("===== Illegal message detected! =====")
            print("Seq {} was already finished but {} (from this pool) tried to insert!".format(seq_number, sender))

        # store incoming messages
        node_message_store.store_messages(messages)
        # TODO: store msgs to send them to own children
        node_pool_comm.add_msgs_to_pool(seq_number, messages)
        node_pool_comm.remember_bulk(seq_number, sender)
        # sending my feedback once
        if seq_number not in node_pool_comm.did_send_to_nodes:
            messages_for_pool = get_lower_equal_messages_for_sequence(seq_number)
            if nmb.has_msg(seq_number):
                messages_for_pool.append(nmb.get_msg_for_seq(seq_number))
            send_pool(seq_number, messages_for_pool)
            node_pool_comm.did_send_to_nodes.add(seq_number)

        if check_pool_comm_complete(seq_number):
            # store my own message
            # node_message_store.store_messages([nmb.get_msg_for_seq(seq_number)])
            # TODO: send stored messages to children
            pool_complete_callback(seq_number)
        
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
            messages = synchronized_child_request(round_number.round)
            if messages is not None:
                return orch_pb.QueueMessageBulk(sendingNode=orch_pb.NodeId(nodeId=node_info.getNodeInfo().uuid.hex), sequence_number=round_number.round, messages=messages)
            else:
                context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
                return orch_pb.QueueMessageBulk(sendingNode=orch_pb.NodeId(nodeId="none"), sequence_number=0, messages=[])
        else:
            return orch_pb.QueueMessageBulk(sendingNode=orch_pb.NodeId(nodeId=node_info.getNodeInfo().uuid.hex), sequence_number=round_number.round, messages=get_lower_equal_messages_for_sequence(round_number.round))

    def notifyParent(self, round_number, context):
        try:
            print("notify parent called with round {}".format(round_number.round))
            if len(node_info.getNodeInfo().children) > 0:
                print("parent pulling from children {}".format(node_info.getNodeInfo().uuid))
                synchronized_child_request(round_number.round)
            # find my assigned parent nodes
            # send the notify recusrively
            # check if I have parents before trying to call them
            if len(node_info.getNodeInfo().parents) > 0:
                print("notifying parent, {}".format(node_info.getNodeInfo().uuid))
                # calculate my corresponding parents
                notify_parent(round_number.round)
                
            else:
                print("Message reached the top op the tree! Sending the Ack")
                send_ack(round_number.round)
            
            return orch_pb.Empty()
        except Exception as e:
            print(e)
            traceback.print_exc()

    def pushMessageToChild(self, bulk, context):
        print("receiving messages from above")
        try:
            messages = bulk.messages
            sender = uuid.UUID(bulk.sendingNode.nodeId)
            seq_number = bulk.sequence_number

            node_message_store.store_messages(messages)

            # send recursively to all other children
            print("Sending to my children recursively, number of children {}".format(len(node_info.getNodeInfo().children)))
            msg_objects = node_message.message_bulk_to_message_array(messages)
            for p in node_info.get_distinct_child_pools():
                print("Pushing down to child pool {}".format(p))
                send_to_children_pool(seq_number, p, msg_objects)
        except Exception as e:
            print(e)
            traceback.print_exc()

        return orch_pb.Empty()
    
    def pushAck(self, round_nr, context):
        try:
            print("Received ack for sequence {}".format(round_nr.round))
            send_ack(round_nr.round)
        except Exception as e:
            print(e)
            traceback.print_exc()
        # Done with sequence

def send_ack(seq_number):
    add_all_finished_sequence(seq_number)

    # find my assigned children
    for p in node_info.get_distinct_child_pools():
        my_targets = generate_push_down_targets(seq_number, p)

        for t in my_targets:
            stub = node_stub.get_or_create_node_comm_stub(t)
            callback = stub.pushAck.future(orch_pb.RoundNumber(round=seq_number))
            callback.add_done_callback(round_ack_callback)

def round_ack_callback(future):
    print("Got round ack callback")

def trigger_round_start(seq_number):
    try:
        if len(node_info.getNodeInfo().children) > 0:
            synchronized_child_request(seq_number)
        # special case, if I am leaf I must take care of my pool manually
        if len(node_info.getNodeInfo().children) == 0 and len(node_info.getNodeInfo().pool_members) > 0:
            result = synchronized_pool_exchange(seq_number)
            if not result:
                print("Pool exchange failed to complete in time")
        if len(node_info.getNodeInfo().parents) > 0:
            notify_parent(seq_number)

        # TODO add own message, down -> up works, up -> down not working, set confirmed to True
        print("Round {} started".format(seq_number))
    except Exception as e:
        print(e)
        traceback.print_exc()


def synchronized_pool_exchange(seq_number):
    try:
        thread = threading.Thread(target=send_pool, args=(seq_number, get_lower_equal_messages_for_sequence(seq_number)))

        if not node_pool_comm.check_pool_complete(seq_number):
            thread.start()

        start_time = time.time() * 1000

        did_complete = False

        while time.time() * 1000 - start_time < pool_receive_timeout:
            if node_pool_comm.check_pool_complete(seq_number):
                did_complete = True
                break
        
        if did_complete:
            # Save own message
            node_message_store.store_messages([nmb.get_msg_for_seq(seq_number)])
            
            add_finished_sequence(seq_number)
            return True
        else:
            print("Could not complete pool exchange in time!")
            return False

    except Exception as e:
        print(e)
        traceback.print_exc()

def notify_parent(seq_number):
    src_pool, target_pool, hash_for_target = nic.get_lists_for_nodes(node_info.getNodeInfo().uuid, 
    list(map(lambda x: x.uuid, node_info.getNodeInfo().pool_members)), 
    list(map(lambda x: x.uuid, node_info.getNodeInfo().parents)))
    res_list = nic.calculate_allocs(str(seq_number).encode(), target_pool, src_pool, hash_for_target)
    print(res_list)
    sender_list = nic.get_list_by_sender(res_list)

    my_targets = sender_list[node_info.getNodeInfo().uuid]

    for target in my_targets:
        comm_stub = node_stub.get_or_create_node_comm_stub(target)
        try:
            comm_stub.notifyParent.future(orch_pb.RoundNumber(round=seq_number))
        except Exception as e:
            print(e)
            traceback.print_exc()

def synchronized_child_request(seq_number):
    print("===== begin of child request")
    try:
        thread = threading.Thread(target=request_children_messages, args=(seq_number,))
        if not is_in_receiving_childs(seq_number) and not node_pool_comm.check_bulks_complete(seq_number):
            add_receiving_child(seq_number)
            thread.start()

        waiting_time = time.time() * 1000

        did_receive = False

        while (time.time() * 1000 - waiting_time < child_receive_timeout):
            print("Still running")
            if node_pool_comm.check_pool_complete(seq_number) and node_pool_comm.check_bulks_complete(seq_number):
                did_receive = True
                print("====== All bulks were received")
                break
            # check every 50ms
            time.sleep(0.05)

        delete_receiving_child(seq_number)

        if did_receive:
            print("About to return messges to my parent because of receive from child call messages")
            unique_child_pools = node_info.get_distinct_child_pools()

            msgs_for_parent = get_lower_equal_messages_for_sequence(seq_number)
            
            add_finished_sequence(seq_number)

            return msgs_for_parent
        else:
            print("Could not capture all children messges in time")
            return None
    except Exception as e:
        print(e)
        traceback.print_exc()

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
    # print("Sending heartbeat")
    for key, value in node_stats.last_heartbeat.items():
        # check all five seconds
        if value < time.time() * 1000 - heartbeat_intervall:
            try:
                stub = node_stub.get_or_create_node_comm_stub(key)
                callback = stub.heartBeat.future(orch_pb.NodeId(nodeId=my_uuid.hex), timeout=heartbeat_timeout/1000.0)
                callback.add_done_callback(partial(hearBeatCallback, key))
            except Exception as e:
                print("Connection failed!", e)
                traceback.print_exc()
                on_heartbeat_failed(key)

def pool_check_callback(node_id, seq_number, ack):
    print("Got callback node: {}, seq: {}".format(node_id, seq_number))
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

    if node_pool_comm.check_self_to_pool(seq_number):
        try:
            node_pool_comm.self_seq_lock.acquire(True)
            node_pool_comm.add_msgs_to_pool(seq_number, [nmb.get_msg_for_seq(seq_number)])
            final_msgs = node_pool_comm.get_msgs_for_pool(seq_number)
            # final_msgs = []
            print("Final msgs: " + str(final_msgs))
            node_message_store.store_messages([nmb.get_msg_for_seq(seq_number)])
        
            # Send to children
            send_pools = [pool for pool in node_info.get_distinct_child_pools()]
            for send_pool in send_pools:
                send_to_children_pool(seq_number, send_pool, node_message.message_bulk_to_message_array(final_msgs))

            node_pool_comm.self_seq[seq_number] = True
        finally:
            node_pool_comm.self_seq_lock.release()

    add_finished_sequence(seq_number)
    # TODO: add own message also when no children

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
        src_pool, target_pool, hash_for_target = nic.get_lists_for_nodes(node_info.getNodeInfo().uuid, 
        list(map(lambda x: x.uuid, node_info.getNodeInfo().pool_members)), c)
        res_list = nic.calculate_allocs(str(seq_number).encode(), target_pool, src_pool, hash_for_target)
        sender_list = nic.get_list_by_sender(res_list)

        my_targets = sender_list[node_info.getNodeInfo().uuid]
        
        if node_child_comm.check_messages_from_all_children_received(seq_number):
           
            pass
        else:
            for node in my_targets:
                stub = node_stub.get_or_create_node_comm_stub(node)
                # check this line
                callback = stub.requestChild.future(orch_pb.RoundNumber(round=seq_number))
                callback.add_done_callback(partial(children_request_callback, seq_number, node))

        # send to my targets
        # Check if we already have msgs for this round
        # TODO: define proto endpoint
        # TODO: use callback function below

def generate_push_down_targets(seq_number, pool_uuid):
    childs_by_pool = node_info.get_child_uuids_by_pool()

    src_pool, target_pool, hash_for_target = nic.get_lists_for_nodes(node_info.getNodeInfo().uuid, 
    list(map(lambda x: x.uuid, node_info.getNodeInfo().pool_members)), 
    childs_by_pool[pool_uuid])
    res_list = nic.calculate_allocs((str(seq_number)+"s").encode(), target_pool, src_pool, hash_for_target)
    sender_list = nic.get_list_by_sender(res_list)

    return sender_list[node_info.getNodeInfo().uuid]

def send_to_children_pool(seq_number, pool_uuid, node_messages):
    try:
        my_targets = generate_push_down_targets(seq_number, pool_uuid)

        print("For pushing down I have {} targets".format(len(my_targets)))

        for target in my_targets:
            # filter out messages already sent
            msgs_to_send = [msg for msg in node_messages if not node_child_comm.check_message_sent_to_child(seq_number, target, msg)]
            print("For my target {} I have {} messages out of {} possible messages".format(target, len(msgs_to_send), len(node_messages)))
            if len(msgs_to_send) > 0:
                print("Sending messages to children")
                queue_messages = node_message.message_array_to_pb_message_array(msgs_to_send)
                stub = node_stub.get_or_create_node_comm_stub(target)
                callback = stub.pushMessageToChild.future(orch_pb.QueueMessageBulk(sendingNode=orch_pb.NodeId(nodeId=node_info.getNodeInfo().uuid.hex), sequence_number=seq_number, messages=queue_messages))

                callback.add_done_callback(child_send_done_callback)

                node_child_comm.add_sent_to_child(seq_number, target, msgs_to_send)
            else:
                print("Already sent those messages")
    except Exception as e:
        print(e)
        traceback.print_exc()

    print("Sent to other children")

def children_request_callback(seq_number, child_id, future):
    # add heard from child
    # parse and store messages QueueMessageBulk
    # check if child comm complete
    # - yes --> communicate to pool
    # - no --> do nothing
    try:
        print("Got messages from child {}".format(child_id))
        print(future)
        the_result = future.result()
        node_child_comm.add_children_heard_from(seq_number, child_id)

        converted_messages = node_message.message_bulk_to_message_array(the_result.messages)
        if check_sequence_finished(seq_number) and node_message_store.is_a_message_not_already_stored(converted_messages):
            print("===== Illegal message detected! =====")
            print("Seq {} was already finished but someone below tried to insert!".format(seq_number))

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

    except Exception as e:
        print(e)
        traceback.print_exc()

# this is called when the children have responded with their messages
def children_receiving_done(seq_number):
    print("Done with receiving from children")
    send_pool(seq_number, get_lower_equal_messages_for_sequence(seq_number))

def get_lower_equal_messages_for_sequence(seq_number):
    print("memory call for messages from sequence {}".format(seq_number))
    unique_child_pools = node_info.get_distinct_child_pools()

    messages = []

    for child_pool in unique_child_pools:
        messages += node_message_store.get_message_for_pool_and_seq(child_pool, seq_number)
    # add my message if available
    if nmb.has_msg(seq_number):
        messages.append(nmb.get_msg_for_seq(seq_number))
    print("Returning {} messages".format(len(messages)))
    return messages

# just for debugging
def child_send_done_callback(future):
    print("send down",future)
    pass
