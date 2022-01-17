from pumpkindb_pb2_grpc import PumpkinDBServicer
from pumpkindb_pb2 import *

class PumpkinDBRPC(PumpkinDBServicer):
    def __init__(self, node):
        self.node = node
        self.logger = self.node.logger

    async def Get(self, request, context):
        'TODO: client consistency'
        self.logger.info(
            f'Request: <[{request.clientId}:{request.commandId}] GET {request.key}>'
            )
        return ValueResponse(
            value=self.node.data.get(request.key, None)
        )


    async def Put(self, request, context):
        'TODO: client consistency'
        self.logger.info(
            f'Request: <[{request.clientId}:{request.commandId}] PUT {request.key}<-{request.value}>'
            )
        key = request.key
        entry = Entry(
            clientId=request.clientId,
            commandId=request.commandId,
            operation='PUT',
            key=key,
            value1=self.node.data.get(key, None),
            value2=request.value
        )
        try:
            term, idx = await self.node.appendEntry(entry)
        except Exception as e:
            self.logger.error(e)
        return ValueResponse(
            term = term,
            logIndex = idx,
            value=self.node.data.get(key,None)
        )
