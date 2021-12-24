import uuid

import grpc

from pumpkindb_pb2_grpc import PumpkinDBStub
from pumpkindb_pb2 import GreetRequest
import pumpkindb_pb2

if __name__ == '__main__':
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = PumpkinDBStub(channel)
        clientId = str(uuid.uuid4())
        commandId = 0
        print("Send greet")
        print(stub.Greet(GreetRequest(clientId=clientId)))

        print("Get x")
        commandId += 1
        print(stub.Get(pumpkindb_pb2.GetRequest(
            clientId=clientId,
            commandId=commandId,
            key="x",
        )))
        print("Set x = 2077")
        commandId += 1
        print(stub.Put(pumpkindb_pb2.PutRequest(
            clientId=clientId,
            commandId=commandId,
            key="x",
            value="2077")))
        

        print("Get x again")
        commandId += 1
        print(stub.Get(pumpkindb_pb2.GetRequest(
            clientId=clientId,
            commandId=commandId,
            key="x",
        )))
