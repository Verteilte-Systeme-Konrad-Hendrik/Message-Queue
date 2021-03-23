import grpc
import orchestration_pb2 as orch_pb
import orchestration_pb2_grpc as orch_pb_grpc
import orchestrator_connection_manager as ocm

def member_to_rpc_node(member):
    print(member.pool_id.hex)
    return orch_pb.NodeInfo(
        nodeId=orch_pb.NodeId(nodeId=member.uuid.hex),
        nodeIpAddress=member.ip_address,
        port=member.port,
        poolId=orch_pb.PoolId(poolId=member.pool_id.hex)
    )

def member_list_to_rpc_node_list(members: []):
    return orch_pb.NodeList(nodes=[member_to_rpc_node(m) for m in members])

def add_member_callback(future):
    print("Got add member callback")

def add_parents_callback(future):
    print("Got add parents callback")

def add_children_callback(future):
    print("Got add children callback")

def add_members(stub, members: []):
    callback = stub.addPoolMembers.future(member_list_to_rpc_node_list(members))
    callback.add_done_callback(add_member_callback)

def add_parents(stub, members: []):
    callback = stub.addParents.future(member_list_to_rpc_node_list(members))
    callback.add_done_callback(add_parents_callback)

def add_children(stub, members: []):
    callback = stub.addChildren.future(member_list_to_rpc_node_list(members))
    callback.add_done_callback(add_children_callback)
