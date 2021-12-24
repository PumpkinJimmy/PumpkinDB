import uuid

import grpc

from pumpkindb_pb2_grpc import PumpkinDBStub
from pumpkindb_pb2 import GreetRequest

if __name__ == '__main__':
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = PumpkinDBStub(channel)
        response = stub.Greet(GreetRequest(clientId=str(uuid.uuid4())))
        print("Message Received")
        print(f"Status: {response.status}\nMessage: {response.msg}")
