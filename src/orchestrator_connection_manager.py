import grpc
import orchestration_pb2 as orch_pb
import orchestration_pb2_grpc as orch_pb_grpc

class connection_manager:
    def __init__(self):
        self.connections = {}
        self.node_stubs = {}

    def get_connection(self, member):
        if member.uuid not in self.connections:
            self.connections[member.uuid] = grpc.insecure_channel(member.ip_address+":"+str(member.port))
        return self.connections[member.uuid]

    def get_node_stub(self, member):
        if member.uuid not in self.node_stubs:
            self.node_stubs[member.uuid] = orch_pb_grpc.NodeOrchestrationStub(self.get_connection(member))
        return self.node_stubs[member.uuid]


conn_mgr = None

def get_connection_manager():
    global conn_mgr
    if conn_mgr is None:
        conn_mgr = connection_manager()
    return conn_mgr
