import grpc
import orchestration_pb2 as orch_pb
import orchestration_pb2_grpc as orch_pb_grpc
import node_servicer as n_serv
import node_info as nf
from concurrent import futures
import node_comm
from threading import Timer

orch_channel = grpc.insecure_channel("localhost:50051")

orch_stub = orch_pb_grpc.OrchestratorStub(orch_channel)

result = orch_stub.getInsertEnvironment(orch_pb.Empty())

my_node = nf.getNodeInfo()

the_node_info = nf.rpc_node_to_node_info(result.selfInfo)

# Init my node info
my_node.uuid = the_node_info.uuid
my_node.ip_address = the_node_info.ipAddress
my_node.port = the_node_info.port

my_node.children = nf.rpc_node_list_to_node_info_list(result.children)
my_node.parents = nf.rpc_node_list_to_node_info_list(result.parents)
my_node.pool_members = nf.rpc_node_list_to_node_info_list(result.poolMembers)

# Add heartbeats
node_comm.add_heartbeats(my_node.children + my_node.parents + my_node.pool_members)

print("Got node info")

# Start own server
node_orch = n_serv.NodeOrchestration()
n_comm = node_comm.NodeCommunication()
node_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
orch_pb_grpc.add_NodeOrchestrationServicer_to_server(node_orch, node_server)
orch_pb_grpc.add_NodeCommunicationServicer_to_server(n_comm, node_server)
node_server.add_insecure_port(my_node.ip_address+":"+str(my_node.port))
node_server.start()

print("Started own server")

heartbeat_trigger = 2.5 # seconds

def repeat_timer():
    Timer(heartbeat_trigger, repeat_timer).start()
    node_comm.check_heartbeat()

# Main loop
Timer(heartbeat_trigger, repeat_timer).start()

print("Waiting...")

node_server.wait_for_termination()