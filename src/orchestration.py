import grpc
import orchestration_pb2 as orch_pb
import orchestration_pb2_grpc as orch_pb_grpc
from orchestrator_objects import node
import uuid
import orchestration_tree as orch_tree
import orchestrator_rpc_functions as orf

class Orchestration(orch_pb_grpc.OrchestratorServicer):
    def __init__(self, initial_port_val):
        self.current_port_val = initial_port_val

    def getInsertEnvironment(self, request, context):
        try:
            orch_tree.tree_lock.acquire(True)
            a_node = node(uuid.uuid4(), "127.0.0.1", self.current_port_val)

            the_pool = orch_tree.get_orchestration_tree().get_insert_pool()

            if the_pool.parent is not None:
                parents = orf.member_list_to_rpc_node_list(the_pool.parent.members)
            else:
                parents = orch_pb.NodeList(nodes=[])
            members = orf.member_list_to_rpc_node_list(the_pool.members)
            all_children = []
            for child_pool in the_pool.children:
                all_children += child_pool.members
            children = orf.member_list_to_rpc_node_list(all_children)
            try:
                the_pool.add_member(a_node)
            except Exception as e:
                print(e)

            nodeInfo = orch_pb.NodeInfo(
                nodeId=orch_pb.NodeId(nodeId=a_node.uuid.hex),
                nodeIpAddress=a_node.ip_address,
                port=a_node.port,
                poolId=orch_pb.PoolId(poolId=a_node.pool_id.hex)
            )

            completeNodeInfo = orch_pb.CompleteNodeInformation(
                selfInfo=nodeInfo,
                poolMembers=members,
                parents=parents,
                children=children
            )

            self.current_port_val += 1

            return completeNodeInfo
        finally:
            orch_tree.tree_lock.release()

