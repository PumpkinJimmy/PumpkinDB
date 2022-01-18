import uuid
import grpc

from pumpkindb_pb2 import *
from pumpkindb_pb2_grpc import PumpkinDBStub

class DBClient:
    def __init__(self, server_addr=None,clientId=None,retry_number=5):
        if clientId is None:
            clientId = str(uuid.uuid4())
        if server_addr is None:
            server_addr = 'localhost:50051'
        self.clientId = clientId
        self.stub = PumpkinDBStub(grpc.insecure_channel(server_addr))
        self.commandId = 0
        self.retry_count = retry_number
        self.receipts = {}
    def get(self, key):
        self.commandId += 1
        receipt = self.receipts.get(key, (None, None))
        for i in range(self.retry_count):
            try:
                resp = self.stub.Get(GetRequest(
                    clientId=self.clientId,
                    commandId=self.commandId,
                    key=key,
                    receiptTerm=receipt[0],
                    receiptLogIndex=receipt[1],
                ))
            except Exception as e:
                print(e)
                print(f"Retry <GET {key}>")
            else:
                return resp.value
        print("Failed")
        return None
    def put(self, key, value):
        self.commandId += 1
        
        for i in range(self.retry_count):
            try:
                resp = self.stub.Put(PutRequest(
                    clientId=self.clientId,
                    commandId=self.commandId,
                    key=key,
                    value=value))
            except Exception as e:
                print(e)
                print(f"Retry <PUT {key}<-{value}>")
            else:
                self.receipts[key] = (resp.term, resp.logIndex)
                # Return receipt
                return resp.term, resp.logIndex
        print("Failed")
        return (0,0)
        