import grpc
from concurrent import futures

from pumpkindb_pb2_grpc import PumpkinDBServicer
from pumpkindb_pb2 import GreetResponse
from pumpkindb_pb2_grpc import add_PumpkinDBServicer_to_server

class PumpkinDBServer(PumpkinDBServicer):
    def Greet(self, request, context):
        return GreetResponse(status="ok", msg=f"Hello, {request.clientId}!")
    

if __name__ == '__main__':
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=5),
        maximum_concurrent_rpcs=5)
    add_PumpkinDBServicer_to_server(PumpkinDBServer(), server)
    server.add_insecure_port('localhost:50051')
    server.start()
    print("Server Start")
    server.wait_for_termination()
