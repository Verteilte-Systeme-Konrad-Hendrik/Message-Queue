import orchestration_pb2 as orch_p2b
import orchestration_pb2_grpc as orch_p2b_grpc
import grpc
from concurrent import futures
import node_info


node_comm_stubs = {}

def get_node_comm_stub_for(node_id):
    return node_comm_stubs.get(node_id, None)

def add_node_comm_stub(node_id):
    the_node = node_info.getNodeInfo().get_related_node(node_id)
    channel = grpc.insecure_channel(the_node.ipAddress+":"+str(the_node.port))
    stub = orch_p2b_grpc.NodeCommunicationStub(channel)

    node_comm_stubs[node_id] = stub

def get_or_create_node_comm_stub(node_id):
    stub = get_node_comm_stub_for(node_id)
    if stub is not None:
        return stub
    else:
        # try reconnecting
        add_node_comm_stub(node_id)
        final_result = get_node_comm_stub_for(node_id)
        if final_result is not None:
            return final_result
        else:
            raise Exception("Node not reachable")
            
