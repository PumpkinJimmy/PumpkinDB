import asyncio
import json
import sys
import functools
from concurrent.futures import ThreadPoolExecutor

import grpc
import websockets
from pumpkindb_pb2 import MonitorResponse

from pumpkindb_pb2_grpc import MonitorServicer, add_MonitorServicer_to_server

monitor_service = None
class MonitorService(MonitorServicer):
    def __init__(self, conn=None):
        self.conn = conn
    def setConn(self, conn):
        self.conn = conn
    async def SendStatus(self, request, context):
        if self.conn:
            asyncio.create_task(self.conn.send(json.dumps({
                'nodeId': request.nodeId,
                'leaderId': request.leaderId,
                'voteFor': request.voteFor,
                'term': request.term,
                'commitIndex': request.commitIndex,
                'lastApplied': request.lastApplied,
                'lastLogIndex': request.lastLogIndex
            })))
            return MonitorResponse(success=True)
        else:
            return MonitorResponse(success=False)

async def websock_main(rpc_server, websocket):
    print(f"Web frontend connection found: {websocket}")
    monitor_service.setConn(websocket)
    while True:
        try:
            message = await websocket.recv()
        except websockets.ConnectionClosedOK:
            monitor_service.setConn(None)
            break
        print(message)
    

async def main():
    global monitor_service
    server = grpc.aio.server(
        ThreadPoolExecutor(40),
        maximum_concurrent_rpcs=5
    )
    monitor_service = MonitorService()
    add_MonitorServicer_to_server(monitor_service, server)
    # add_MonitorServicer_to_server(MonitorService(), server)
    server.add_insecure_port('localhost:5000')
    await server.start()
    print("Monitor Start")
    try:
        bounded_main = functools.partial(websock_main, server)
        await websockets.serve(bounded_main, 'localhost', 5001)
        await server.wait_for_termination()
    except:
        await server.stop(None)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # loop.run_until_complete(main_task)
    loop.run_until_complete(asyncio.wait([main()]))
    loop.run_forever()