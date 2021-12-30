import asyncio
import platform
import random
from concurrent.futures import ThreadPoolExecutor

import grpc

from pumpkindb_pb2_grpc import RaftServicer, RaftStub, add_RaftServicer_to_server
from pumpkindb_pb2 import *


class RaftRPC(RaftServicer):
    def __init__(self, node):
        self.node = node

    def RequestVote(self, request, context):
        print(f"Get RequestVote from {request.candidateId}")
        if request.term >= self.node.term \
            and (self.node.votedFor is None or self.node.votedFor == request.candidateId) \
            and (request.lastLogTerm >= self.node.lastLogTerm 
                and request.lastLogIndex >= self.node.lastLogIndex):
            self.node.votedFor = request.candidateId
            return RequestVoteResponse(
                term=self.node.term,
                voteGranted=True
            )
            
        else:
            return RequestVoteResponse(
                term=self.node.term,
                voteGranted=False
            )
    
    def AppendEntries(self, request, context):
        return AppendEntriesRequest()

class RaftNode:
    def __init__(self, nodeId, peerIds):
        self.leaderId = None
        self.nodeId = nodeId
        self.nodeNum = len(peerIds) + 1
        self.lastLogIndex = 0
        self.lastLogTerm = 0
        self.term = 0
        self.peerIds = peerIds
        self.channels = list(map(self.getChannel, self.peerIds))
        self.raft_server = grpc.aio.server(
            ThreadPoolExecutor(max_workers=40),
            maximum_concurrent_rpcs=40
        )
        add_RaftServicer_to_server(RaftRPC(self), self.raft_server)
        self.raft_server.add_insecure_port(nodeId)
        self.election_timer_task = None
        self.votedFor = None
    
    def getChannel(self, nodeId):
        return grpc.aio.insecure_channel(nodeId)
    
    def isLeader(self):
        return self.nodeId == self.leaderId
    
    async def start(self):
        await self.raft_server.start()
        print(f"[RaftNode {self.nodeId}] Server start")
        T = 0.5
        self.election_timer_task = asyncio.create_task(self.electionTimer(random.random()*T+T))
        try:
            await self.raft_server.wait_for_termination()
        except KeyboardInterrupt:
            await self.raft_server.stop(None)

    
    async def electionTimer(self, timeout):
        await asyncio.sleep(timeout)
        self.term += 1
        self.votedFor = self.nodeId
        tasks = []
        voteCount = 1
        for channel in self.channels:
            stub = RaftStub(channel)
            tasks.append(stub.RequestVote(RequestVoteRequest(
                term=self.term,
                candidateId=self.nodeId,
                lastLogIndex=self.lastLogIndex,
                lastLogTerm=self.lastLogTerm
            )))
        for task in tasks:
            resp = await task
            if resp.voteGranted:
                voteCount += 1
            if voteCount > self.nodeNum/2:
                self.leaderId = self.nodeId
        print(f'[{self.nodeId}] isLeader: { self.isLeader()}')
        



async def main():
    ids = ['localhost:9000', 'localhost:9002', 'localhost:9004']
    nodes = []
    tasks = []
    for nodeId in ids[:]:
        ids.remove(nodeId)
        nodes.append(RaftNode(nodeId, ids))
        tasks.append(nodes[-1].start())
        ids.append(nodeId)
    
    await asyncio.wait(tasks)

if __name__ == '__main__':
    if platform.system().lower() == 'windows':
        asyncio.set_event_loop_policy(
            asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())