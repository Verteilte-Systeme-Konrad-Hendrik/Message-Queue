import grpc
import orchestration_pb2 as orch_pb
import orchestration_pb2_grpc as orch_pb_grpc
import orchestrator_connection_manager as ocm
import time

def member_to_rpc_node(member):
    return orch_pb.NodeInfo(
        nodeId=orch_pb.NodeId(nodeId=member.uuid.hex),
        nodeIpAddress=member.ip_address,
        port=member.port,
        poolId=orch_pb.PoolId(poolId=member.pool_id.hex)
    )

def member_list_to_rpc_node_list(members: []):
    return orch_pb.NodeList(nodes=[member_to_rpc_node(m) for m in members])

def add_member_callback(future):
    print(future)
    print("Got add member callback")

def add_parents_callback(future):
    print(future)
    print("Got add parents callback")

def add_children_callback(future):
    print(future)
    print("Got add children callback")

def add_members(stub, members: []):
    repeat = True
    while repeat:
        try:
            stub.addPoolMembers(member_list_to_rpc_node_list(members))
            repeat = False
        except Exception as e:
            repeat = True
            time.sleep(0.1)


def add_parents(stub, members: []):
    repeat = True
    while repeat:
        try:
            stub.addParents(member_list_to_rpc_node_list(members))
            repeat = False
        except Exception as a:
            repeat = True
            time.sleep(0.1)


def add_children(stub, members: []):
    repeat = True
    while repeat:
        try:
            stub.addChildren(member_list_to_rpc_node_list(members))
            repeat = False
        except Exception as e:
            repeat = True
            time.sleep(0.1)
