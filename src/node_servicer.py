import grpc
import orchestration_pb2 as orch_pb
import orchestration_pb2_grpc as orch_pb_grcp
import node_info as nf
from node_info import node_info
import node_comm

class NodeOrchestration(orch_pb_grcp.NodeOrchestrationServicer):

    def addParents(self, request, context):
        my_node = nf.getNodeInfo()
        parent_list = nf.rpc_node_list_to_node_info_list(request)
        my_node.parents += parent_list
        node_comm.add_heartbeats(parent_list)
        print("added parents", parent_list)

    def addChildren(self, request, context):
        my_node = nf.getNodeInfo()
        children_list = nf.rpc_node_list_to_node_info_list(request)
        my_node.children += children_list
        node_comm.add_heartbeats(children_list)
        print("added children", children_list)

    def addPoolMembers(self, request, context):
        my_node = nf.getNodeInfo()
        pool_members = nf.rpc_node_list_to_node_info_list(request)
        my_node.pool_members += pool_members
        node_comm.add_heartbeats(pool_members)
        print("pool members", pool_members)
