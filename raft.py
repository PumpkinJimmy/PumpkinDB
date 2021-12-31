import asyncio
import platform
import random
import pickle
import struct 
import os
from concurrent.futures import ThreadPoolExecutor

import grpc

from pumpkindb_pb2_grpc import (
    RaftServicer,
    RaftStub,
    MonitorStub,
    add_RaftServicer_to_server
)
from pumpkindb_pb2 import *


class RaftRPC(RaftServicer):
    def __init__(self, node):
        self.node = node

    def RequestVote(self, request, context):
        print(f"[{self.node.nodeId}] Get RequestVote from {request.candidateId}")
        # if self.node.isLeader():
        #     print(f'[{self.node.nodeId}] Reject vote request AS LEADER from {request.candidateId}')
        #     return RequestVoteResponse(
        #         term=self.node.term,
        #         voteGranted=False
        #     )
        if request.term >= self.node.term \
            and (self.node.votedFor is None or self.node.votedFor == request.candidateId) \
            and (request.lastLogTerm >= self.node.logs[-1].term
                and request.lastLogIndex >= self.node.logs[-1].logIndex):
            self.node.votedFor = request.candidateId
            print(f'[{self.node.nodeId}] Vote grant {request.candidateId}')
            return RequestVoteResponse(
                term=self.node.term,
                voteGranted=True
            )
            
        else:
            print(f'[{self.node.nodeId}] Reject vote request from {request.candidateId}')
            return RequestVoteResponse(
                term=self.node.term,
                voteGranted=False
            )
    
    def AppendEntries(self, request, context):
        if self.node.election_timer_task is not None:
            print(f'[{self.node.nodeId}] Heartbeat from {request.leaderId}, become follower')
            self.node.election_timer_task.cancel()
            self.node.election_timer_task = None
        print(f'[{self.node.nodeId}] Heartbeat from {request.leaderId}')
        self.node.leaderId = request.leaderId
        self.node.leader_alive_task.cancel()
        self.node.leader_alive_task = asyncio.create_task(
            self.node.leaderAliveTimer(2)
        )
        return AppendEntriesRequest(
            term=self.node.term,
            success=True
        )

class RaftFile:
    '''
    Log file structure
    4 bytes term
    512 bytes voteFor
    k bytes log entries
    '''
    def __init__(self, log_path):
        self.log_path = log_path
        self.log_file = open(self.log_path, 'ab+')
        self.state_file = open(self.log_path, 'rb+')
        print(log_path)
        if not os.path.exists(self.log_path):
            self.log_file.write(struct.pack('I', 0))
            self.log_file.write(b'\0' * 512)
            self.log_file.flush()
            self.state_file.seek(0)
    
    def getTerm(self):
        res = struct.unpack('I', self.state_file.read(4))
        self.state_file.seek(0)
        return res
    
    def getVoteFor(self):
        self.state_file.seek(4)
        len_id = struct.unpack('I', self.state_file.read(4))
        res = self.state_file.read(len_id)
        self.state_file.seek(0)
        return res
    
    def getLogs(self):
        raise NotImplemented()
    
    def setTerm(self, term):
        self.state_file.write(struct.pack('I', term))
        self.state_file.flush()
        self.state_file.seek(0)
        
    
    def setVoteFor(self, voteFor):
        self.state_file.seek(4)
        if voteFor is None:
            self.state_file.write(struct.pack('I', 0))
            self.state_file.flush()
            self.state_file.seek(0)
            return
        voteFor = voteFor.encode()
        len_id = len(voteFor)
        self.state_file.write(struct.pack('I', len_id))
        self.state_file.write(voteFor)
        self.state_file.flush()
        self.state_file.seek(0)

    def appendEntries(self, entries):
        pass
        # for lentry in entries:
        #     self.log_file.write()

class RaftNode:
    def __init__(self, nodeId, peerIds):
        # Node & Cluster info.
        self.leaderId = None
        self.nodeId = nodeId
        self.nodeNum = len(peerIds) + 1
        self.peerIds = peerIds

        # Log file
        self.log_file = RaftFile(f'./{self.nodeId.split(":")[1]}_raft.binlog')
        
        # Persistent state
        self.votedFor = None
        self.term = 0
        self.logs = [LogEntry(term = 0, logIndex=0)]

        # Volatile state
        self.commitIndex = 0
        self.lastApplied = 0

        # leader only state
        lastLogIndex = self.logs[-1].logIndex
        self.nextIndex = [lastLogIndex+1] * (self.nodeNum - 1)
        self.matchIndex = [0] * (self.nodeNum - 1)
        
        # RPC
        self.channels = list(map(self.getChannel, self.peerIds))
        self.raft_server = grpc.aio.server(
            ThreadPoolExecutor(max_workers=40),
            maximum_concurrent_rpcs=40
        )
        add_RaftServicer_to_server(RaftRPC(self), self.raft_server)
        self.raft_server.add_insecure_port(nodeId)

        # Timers
        self.election_timer_task = None
        self.heartbeat_timer_task = None
        self.leader_alive_task = None

        # Monitor
        self.monitor_stub = MonitorStub(grpc.aio.insecure_channel('localhost:5000'))
    
    def getChannel(self, nodeId):
        return grpc.aio.insecure_channel(nodeId)
    
    def isLeader(self):
        return self.nodeId == self.leaderId

    async def sendMonitor(self):
        await asyncio.sleep(2)

        self.monitor_stub.SendStatus(Status(
            nodeId = self.nodeId,
            leaderId = self.leaderId,
            voteFor = self.votedFor,
            term = self.term,
            commitIndex = self.commitIndex,
            lastApplied = self.lastApplied,
            lastLogIndex = self.logs[-1].logIndex,
        ))

        asyncio.create_task(self.sendMonitor())
    
    async def start(self):
        await self.raft_server.start()
        print(f"[RaftNode {self.nodeId}] Server start")
        asyncio.create_task(self.sendMonitor())
        T = 2
        self.leader_alive_task = asyncio.create_task(
            self.leaderAliveTimer(random.random()*T + T)
        )
        # T = 0.5
        # self.election_timer_task = asyncio.create_task(self.electionTimer(random.random()*T+T))
        # if self.isLeader():
        #     self.heartbeat_timer_task = asyncio.create_task(self.heartbeatTimer(0.1))
        try:
            await self.raft_server.wait_for_termination()
        except KeyboardInterrupt:
            await self.raft_server.stop(None)
    
    async def sendHeartbeat(self):
        
        tasks = []
        for channel in self.channels:
            stub = RaftStub(channel)
            tasks.append(stub.AppendEntries(
                AppendEntriesRequest(
                    term = self.term,
                    leaderId = self.leaderId,
                    prevLogIndex = self.logs[-1].logIndex,
                    prevLogTerm = self.logs[-1].term,
                    leaderCommit = self.commitIndex,
                    entries = ()
                )
            ))
    
    async def leaderAliveTimer(self, timeout):
        await asyncio.sleep(timeout)
        print(f"[{self.nodeId}] Cannot receive heartbeat, become candidate")
        T = 0.5
        self.election_timer_task = asyncio.create_task(self.electionTimer(random.random()*T+T))
    
    async def heartbeatTimer(self, timeout):
        await asyncio.sleep(timeout)
        await self.sendHeartbeat()
        print(f'[{self.nodeId}] send heartbeat')
        self.heartbeat_timer_task = asyncio.create_task(self.heartbeatTimer(timeout))
        
        
        
    
    async def electionTimer(self, timeout):
        await asyncio.sleep(timeout)
        print(f'[{self.nodeId}]: become candidate')
        self.term += 1
        self.log_file.setTerm(self.term)
        self.votedFor = self.nodeId
        self.log_file.setVoteFor(self.nodeId)
        tasks = []
        voteCount = 1
        for channel in self.channels:
            stub = RaftStub(channel)
            tasks.append(stub.RequestVote(RequestVoteRequest(
                term=self.term,
                candidateId=self.nodeId,
                lastLogIndex=self.logs[-1].logIndex,
                lastLogTerm=self.logs[-1].term
            )))
        for task in tasks:
            resp = await task
            if resp.voteGranted:
                voteCount += 1
            if resp.term > self.term:
                self.term = resp.term
                self.log_file.setTerm(self.term)
            if voteCount > self.nodeNum/2:
                self.leaderId = self.nodeId
        
        self.votedFor = None
        self.log_file.setVoteFor(None)

        print(f'[{self.nodeId}] Current Term: {self.term}, Leader: {self.leaderId}')
        print(f'[{self.nodeId}] isLeader: { self.isLeader()}')

        if self.isLeader:
            await self.sendHeartbeat()
            self.heartbeat_timer_task = asyncio.create_task(self.heartbeatTimer(1))

async def main():
    ids = ['localhost:9000', 'localhost:9002', 'localhost:9004', 'localhost:9006', 'localhost:9008']
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
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait([main()]))
    loop.close()
    # asyncio.run(main())