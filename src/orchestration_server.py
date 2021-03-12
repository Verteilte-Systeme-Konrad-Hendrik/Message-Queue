import grpc
from concurrent import futures
import orchestration_pb2_grpc as orch_pb_grcp
from orchestration import Orchestration


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    orch_pb_grcp.add_OrchestratorServicer_to_server(Orchestration(5000), server)
    server.add_insecure_port("0.0.0.0:50051")
    server.start()
    print("Started!")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
