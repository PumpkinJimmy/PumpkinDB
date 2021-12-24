import grpc
from concurrent import futures

from pumpkindb_pb2_grpc import PumpkinDBServicer
from pumpkindb_pb2 import GreetResponse, ValueResponse
from pumpkindb_pb2_grpc import add_PumpkinDBServicer_to_server

class PumpkinDBServer(PumpkinDBServicer):
    def __init__(self):
        self.data = {}

    def Greet(self, request, context):
        return GreetResponse(status="ok", msg=f"Hello, {request.clientId}!")

    def Get(self, request, context):
        key = request.key
        if key not in self.data:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f'Key not found: {key}')
            raise Exception(f'Key not found: {key}')
        return ValueResponse(value=self.data[key])
    
    def Put(self, request, context):
        self.data[request.key] = request.value
        return ValueResponse(value=self.data[request.key])


if __name__ == '__main__':
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=5),
        maximum_concurrent_rpcs=5)
    add_PumpkinDBServicer_to_server(PumpkinDBServer(), server)
    server.add_insecure_port('localhost:50051')
    server.start()
    print("Server Start")
    server.wait_for_termination()
