import asyncio
import platform
import random
import pickle
import struct 
import os
import string
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint
import logging
from typing import List
import copy

import grpc

from pumpkindb_pb2_grpc import (
    RaftServicer,
    RaftStub,
    MonitorStub,
    add_RaftServicer_to_server
)
from pumpkindb_pb2 import *

async def setTimer(coro_func, timeout):
    await asyncio.sleep(timeout)
    await coro_func()

async def setInterval(coro_func, timeout):
    await asyncio.sleep(timeout)
    await asyncio.gather(coro_func(), setInterval(coro_func, timeout))


def random_timeout(T):
    return random.random()*T + T

class RaftRPC(RaftServicer):
    def __init__(self, node):
        self.node = node
        self.logger = self.node.logger

    async def RequestVote(self, request, context):
        self.logger.debug(f'Get RequestVote from {request.candidateId}')

        if request.term >= self.node.term \
            and (self.node.votedFor is None or self.node.votedFor == request.candidateId) \
            and (request.lastLogTerm >= self.node.logs[-1].term
                and request.lastLogIndex >= self.node.logs[-1].logIndex):
            self.node.votedFor = request.candidateId
            self.logger.debug(f'Vote grant {request.candidateId}')
            # if self.node.leader_alive_task:
            #     self.node.leader_alive_task.cancel()
            #     self.node.leader_alive_task = None
                
            return RequestVoteResponse(
                term=self.node.term,
                voteGranted=True
            )
            
        else:
            self.logger.debug(f'Reject vote request from {request.candidateId}')
            return RequestVoteResponse(
                term=self.node.term,
                voteGranted=False
            )
    
    async def AppendEntries(self, request, context):
        self.node.leader_alive = True
        self.logger.debug(f'Entries: {request.entries}')

        if request.term >= self.node.term \
            and self.node.checkLogMatch(request):
            if self.node.election_timer_task is not None:
                # switch to follower
                self.logger.info(f'Become follower: heartbeat from {request.leaderId}')
                self.node.election_timer_task.cancel()
                self.node.election_timer_task = None
            
            self.logger.debug(f'Heartbeat from {request.leaderId}')
            self.node.term = request.term
            self.node.leaderId = request.leaderId
            self.node.votedFor = None

            # reset leader alive timer
            if self.node.leader_alive_task is None:
                self.logger.debug('Reset leader alive timer')
                self.node.leader_alive_task = asyncio.create_task(
                    self.node.leaderAliveTimer(random_timeout(2))
                )
            
            # add logs
            for entry in request.entries:
                # write log
                # FIXME: ERROR in older log replication
                # log_entry = LogEntry(
                #     term=self.node.term,
                #     logIndex=len(self.node.logs),
                #     command = entry
                #     )
                self.node.appendLog([entry])

                # apply
                self.node.data[entry.command.key] = entry.command.value2

            if request.leaderCommit > self.node.commitIndex:
                self.node.commitIndex = min(request.leaderCommit, len(self.node.logs)-1)
            self.logger.debug(f'Current data table: {self.node.data}')
            return AppendEntriesResponse(
                term=self.node.term,
                success=True
            )
        # Reject msg from old term
        else:
            self.logger.debug(f'Reject append index: {request.term}, {self.node.term}, {request.prevLogIndex}, {self.node.getLastLogIdx()}, {request.prevLogTerm}, {self.node.getLastLogTerm()}')
            return AppendEntriesResponse(
                term=self.node.term,
                success=False
            )

        

class RaftPersistentStateMachine:
    # log index start from 1 (not 0)
    def __init__(self, log_path):
        self.log_path = log_path
        self._term = 0
        self._votedFor = None
        self.logs = []
        self.reloadPersistentStates()
        

    def getLastLogIdx(self):
        return self.logs[-1].logIndex

    def getLastLogTerm(self):
        return self.logs[-1].term
    
    def getPrevLogIdx(self, cur_idx):
        if cur_idx > 0:
            return self.logs[cur_idx-1].logIndex
        else:
            return 0

    def getPrevLogTerm(self, cur_idx):
        if cur_idx > 0:
            return self.logs[cur_idx - 1].term
        else:
            return 0

    def appendLog(self, entries: List[LogEntry]):
        for le in entries:
            if le.logIndex < len(self.logs):
                # FIXME: undo the op in the thrown logs
                # fix wrong log
                self.logs = self.logs[:le.logIndex] + [le]
            else:
                self.logs.append(le)
        # self.logs.extend(entries)
    
    def getTerm(self):
        return self._term
    
    def setTerm(self, term):
        self.state_file.write(struct.pack('I', term))
        self.state_file.flush()
        self.state_file.seek(0)
        self._term = term

    def getVotedFor(self):
        return self._votedFor

    def setVotedFor(self, votedFor):
        self.state_file.seek(4)
        if votedFor is None:
            self.state_file.write(struct.pack('I', 0))
            self.state_file.flush()
            self.state_file.seek(0)
            self._votedFor = votedFor
        else:
            voteFor = votedFor.encode()
            len_id = len(voteFor)
            self.state_file.write(struct.pack('I', len_id))
            self.state_file.write(voteFor)
            self.state_file.flush()
            self.state_file.seek(0)
            self._votedFor = votedFor
    
    def checkLogMatch(self, req: AppendEntriesRequest):
        if req.prevLogIndex <= self.getLastLogIdx():
            return req.prevLogTerm == self.logs[req.prevLogIndex].term
        else:
            return False

    def reloadPersistentStates(self):
        if not os.path.exists(self.log_path):
            self.log_file = open(self.log_path, 'ab+')
            self.state_file = open(self.log_path, 'rb+')
            self.log_file.write(struct.pack('I', 0))
            self.log_file.write(bytes([0] * 512))
            self.log_file.flush()
            self.state_file.seek(0)
            self.setTerm(0)
            self.setVotedFor(None)
            self.logs = [LogEntry(
                    term=0,
                    logIndex=0
                )]
        else:
            self.log_file = open(self.log_path, 'ab+')
            self.state_file = open(self.log_path, 'rb+')
            self.readTerm()
            self.readVoteFor()
            self.readLogs()
    
    def readTerm(self):
        res = struct.unpack('I', self.state_file.read(4))[0]
        self.state_file.seek(0)
        self.setTerm(res)

    def readVoteFor(self):
        self.state_file.seek(4)
        len_id = struct.unpack('I', self.state_file.read(4))[0]
        res = self.state_file.read(len_id)
        self.state_file.seek(0)
        if not len_id: self.setVotedFor(None)
        else:
            self.setVotedFor(res.decode())

    def readLogs(self):
        logs = [LogEntry(
            term=0,
            logIndex=0
        )]
        self.log_file.seek(4+512)
        len_data = self.log_file.read(4)
        while len_data:
            len_ = struct.unpack('i', len_data)[0]
            entry = LogEntry()
            entry.ParseFromString(self.log_file.read(len_))
            logs.append(copy.copy(entry))
            len_data = self.log_file.read(4)
        self.logs = logs
    
    votedFor = property(getVotedFor, setVotedFor)
    term = property(getTerm, setTerm)

class RaftNode(RaftPersistentStateMachine):
    
    def __init__(self, nodeId, peerIds):
        
        # Node & Cluster info.
        self.nodeId = nodeId
        self.nodeNum = len(peerIds) + 1
        self.peerIds = peerIds
        self.initLogger()

        super().__init__(f'./{self.nodeId.split(":")[1]}_raft.binlog')

        # In-memory DB
        self.data = {}

        self.initState()
        self.resetTimers()
        self.initMonitor()

        self.initRPCClient()
        self.initRPCServer()
    
    def initLogger(self):
        self.logger = logging.getLogger(self.nodeId)
        f = logging.Formatter('[%(name)s] %(msg)s')
        h = logging.StreamHandler()
        h.setFormatter(f)
        h.setLevel(logging.DEBUG)
        self.logger.addHandler(h)
        self.logger.setLevel(logging.DEBUG)

    def initState(self):

        # Volatile state
        self.commitIndex = 0
        self.lastApplied = 0

        # leader only state
        lastLogIndex = self.getLastLogIdx()
        self.nextIndex = [lastLogIndex+1] * (self.nodeNum - 1)
        self.matchIndex = [0] * (self.nodeNum - 1)

        # Convenient state
        self.leader_alive = False
        self.leaderId = None
        self.running_state = 'stopped'
    
    def initRPCServer(self):
        self.raft_server = grpc.aio.server(
            ThreadPoolExecutor(max_workers=40),
            maximum_concurrent_rpcs=40
        )
        add_RaftServicer_to_server(RaftRPC(self), self.raft_server)
        self.raft_server.add_insecure_port(self.nodeId)
    
    def initRPCClient(self):
        self.channels = list(map(self.getChannel, self.peerIds))
    
    def resetTimers(self):
        tmp = getattr(self, 'election_timer_task', None)
        if tmp:
            tmp.cancel()
        tmp = getattr(self, 'heartbeat_timer_task', None)
        if tmp:
            tmp.cancel()
        tmp = getattr(self, 'leader_alive_task', None)
        if tmp:
            tmp.cancel()
        self.election_timer_task = None
        self.heartbeat_timer_task = None
        self.leader_alive_task = None
    
    def initMonitor(self):
        self.monitor_stub = MonitorStub(grpc.aio.insecure_channel('localhost:5000'))

    def getChannel(self, nodeId):
        return grpc.aio.insecure_channel(nodeId)
    
    def isLeader(self):
        return self.nodeId == self.leaderId

    def getNodeInfo(self):
        return {
            'running_state': self.running_state,
            'nodeId': self.nodeId,
            'peerIds': self.peerIds,
            'votedFor': self.votedFor,
            'term': self.term,
            'logs': self.logs,
            'commitIndex': self.commitIndex,
            'lastApplied': self.lastApplied,
            'nextIndex': self.nextIndex,
            'matchIndex': self.matchIndex,
            'channels': self.channels,
            'server': self.raft_server,
            'leader_alive': self.leader_alive,
            'leaderId': self.leaderId,
            'leader_alive_timer': self.leader_alive_task,
            'election_timer': self.election_timer_task,
            'heartbeat_timer': self.heartbeat_timer_task
        }

    async def sendMonitor(self):
        await asyncio.sleep(2)

        self.monitor_stub.SendStatus(Status(
            nodeId = self.nodeId,
            leaderId = self.leaderId,
            voteFor = self.votedFor,
            term = self.term,
            commitIndex = self.commitIndex,
            lastApplied = self.lastApplied,
            lastLogIndex = self.getLastLogIdx(),
        ))

        await self.sendMonitor()
    
    async def _run(self):
        await self.raft_server.start()
        self.logger.info(f"Node start")
        self.running_state = 'running'
        tasks = []
        
        T = 2
        self.leader_alive_task = asyncio.create_task(
            self.leaderAliveTimer(random.random()*T + T)
        )
        tasks.append(self.leader_alive_task)
        tasks.append(asyncio.create_task(self.raft_server.wait_for_termination()))

        try:
            await asyncio.wait(tasks)
        except KeyboardInterrupt:
            await self.raft_server.stop(None)

    async def start(self):
        asyncio.create_task(self.sendMonitor())
        await self._run()
    
    async def sendHeartbeat(self):
        await self.sendRandomMsg(False)

    
    async def leaderAliveTimer(self, timeout):
        await asyncio.sleep(timeout)
        if self.leader_alive:
            self.logger.debug('Leader alive')
            self.leader_alive = False
            await self.leaderAliveTimer(timeout)
        elif self.votedFor:
            print(self.votedFor)
            self.logger.debug("Vote granted, cancel leader alive timer")
            self.leader_alive_task.cancel()
            self.leader_alive_task = None
            return
        else:
            self.logger.info("Become candidate: cannot receive heartbeat")
            T = 2
            self.election_timer_task = asyncio.create_task(self.electionTimer(random_timeout(T)))
            self.leader_alive_task = None
    
    async def heartbeatTimer(self, timeout):
        await asyncio.sleep(timeout)
        await self.sendRandomMsg()
        self.logger.debug('Send heartbeat')
        await self.heartbeatTimer(timeout)
    
    async def electionTimer(self, timeout):
        if self.votedFor != None:
            return
        self.logger.info('Become candidate')
        self.term += 1
        # self.log_file.setTerm(self.term)
        self.votedFor = self.nodeId
        # self.log_file.setVoteFor(self.nodeId)
        tasks = []
        voteCount = 1
        for channel in self.channels:
            self.logger.debug(f"Requesting {channel} for vote")
            stub = RaftStub(channel)
            tasks.append(stub.RequestVote(RequestVoteRequest(
                term=self.term,
                candidateId=self.nodeId,
                lastLogIndex=self.getLastLogIdx(),
                lastLogTerm=self.getLastLogTerm()
            ), timeout=1.5))

        resps, _ = await asyncio.wait(tasks, timeout=2)
        
        for resp in resps:
            try:
                resp = resp.result()
            except Exception as e:
                self.logger.warning(e)
                continue
            if resp.voteGranted:
                voteCount += 1
                self.logger.debug(f'Get Vote, current vote number: {voteCount}')
            if resp.term > self.term:
                self.term = resp.term
                # self.log_file.setTerm(self.term)
            if voteCount > self.nodeNum/2:
                self.leaderId = self.nodeId
                break
        self.votedFor = None
        # self.log_file.setVoteFor(None)

        self.logger.info(f'Become leader. Get vote number: {voteCount}. Current Term: {self.term}')
        self.logger.debug(f'isLeader: { self.isLeader()}')


        if self.isLeader():
            await self.sendHeartbeat()
            self.heartbeat_timer_task = asyncio.create_task(self.heartbeatTimer(1))
        
        else:
            T = 2
            self.leader_alive_task = asyncio.create_task(
                self.leaderAliveTimer(random_timeout(T))
            )
        
        self.election_timer_task = None
    
    async def crash(self):
        self.resetTimers()
        self.running_state = 'stopped'

        await self.raft_server.stop(None)
        for channel in self.channels:
            await channel.close()

    async def resume(self):
        self.reloadPersistentStates()
        self.initState()
        self.resetTimers()
        self.initMonitor()

        self.initRPCClient()
        self.initRPCServer()

        await self._run()
    
    async def sendRandomMsg(self, msg=True):
        if not msg:
            tasks = []
            for channel in self.channels:
                stub = RaftStub(channel)
                tasks.append(stub.AppendEntries(
                    AppendEntriesRequest(
                        term = self.term,
                        leaderId = self.leaderId,
                        prevLogIndex = self.getLastLogIdx(),
                        prevLogTerm = self.getLastLogTerm(),
                        leaderCommit = self.commitIndex,
                    ),
                    timeout=1.5
                ))
            await asyncio.wait(tasks)
        else:
            # tasks = []
            entry = Entry(
                clientId='test',
                commandId=5,
                operation='TEST',
                key=str(random.choice(list(string.ascii_letters))),
                value1=str(random.randint(1, 100)),
                value2=str(random.randint(1,100)),
            )
            await self.appendEntry(entry)
            # log_entry = LogEntry(
            #         term = self.term,
            #         logIndex = self.getLastLogIdx()+1,
            #         command = entry
            #     )
            # # req = AppendEntriesRequest(
            # #         term = self.term,
            # #         leaderId = self.leaderId,
            # #         prevLogIndex = self.getLastLogIdx(),
            # #         prevLogTerm = self.getLastLogTerm(),
            # #         leaderCommit = self.commitIndex,
            # #         entries = (
            # #             entry,
            # #         )
            # #     )
            # self.appendLog((log_entry, ))

            # self.data[entry.key] = entry.value2
            # self.logger.debug(f'Current data: {self.data}')
            # for i in range(len(self.peerIds)):
            #     tasks.append(
            #         self.replicateLog(i)
            #     )
                # stub = RaftStub(channel)
                # tasks.append(stub.AppendEntries(
                #     req,
                #     timeout=1.5
                # ))
            
            # await asyncio.wait(tasks)

    async def appendEntry(self, entry: Entry):
        log_entry = LogEntry(
                term = self.term,
                logIndex = self.getLastLogIdx()+1,
                command = entry
            )
        tasks = []
        self.appendLog((log_entry, ))

        self.data[entry.key] = entry.value2
        self.logger.debug(f'Current data: {self.data}')
        for i in range(len(self.peerIds)):
            self.logger.debug(f'Start copying {self.nextIndex[i]} at Peer {i}')
            tasks.append(
                asyncio.create_task(self.replicateLog(i))
            )
            
        resp_count = 0
        majority = len(self.peerIds) // 2
        while resp_count < majority:
            fin, pend = await asyncio.wait(tasks, timeout=2)
            for task in fin:
                if task.result():
                    resp_count += 1
            self.logger.debug(f'Got {resp_count} response currently')
        self.commitIndex += 1
        self.logger.debug(f'Got {resp_count} response, entry committed')
            # stub = RaftStub(channel)
            # tasks.append(stub.AppendEntries(
            #     req,
            #     timeout=1.5
            # ))
        
        # await asyncio.wait(tasks)
    
    async def replicateLog(self, peerIdx):
        channel = self.channels[peerIdx]
        stub = RaftStub(channel)
        sendLogIdx = self.nextIndex[peerIdx]
        req = AppendEntriesRequest(
                term = self.term,
                leaderId = self.leaderId,
                prevLogIndex = self.getPrevLogIdx(sendLogIdx),
                prevLogTerm = self.getPrevLogTerm(sendLogIdx),
                leaderCommit = self.commitIndex,
                entries = (
                    (self.logs[sendLogIdx],)
                )
            )
        while 1:
            try:
                self.logger.debug(f'Copying {sendLogIdx} at Peer {peerIdx}')
                resp = await stub.AppendEntries(req, timeout=2)
            except Exception as e:
                self.logger.debug(f'Copying at Peer {peerIdx} timeout')
                self.logger.debug(e)
            else:
                if not resp.success:
                    self.logger.debug(f'Copying at Peer {peerIdx} rejected, go back')
                    self.nextIndex[peerIdx] = max(self.nextIndex[peerIdx]-1, 1)
                    
                else:
                    self.logger.debug(f'Copying at Peer {peerIdx} accepted')
                    self.matchIndex[peerIdx] = sendLogIdx
                    self.nextIndex[peerIdx] = sendLogIdx + 1
                    if self.nextIndex[peerIdx] >=  self.getLastLogIdx():
                        self.logger.debug(f'Peer {peerIdx} all up to dated')
                        return True
                sendLogIdx = self.nextIndex[peerIdx]
                req = AppendEntriesRequest(
                        term = self.term,
                        leaderId = self.leaderId,
                        prevLogIndex = self.getPrevLogIdx(sendLogIdx),
                        prevLogTerm = self.getPrevLogTerm(sendLogIdx),
                        leaderCommit = self.commitIndex,
                        entries = (
                            (self.logs[sendLogIdx],)
                        )
                    )
                        
                

async def randomCrash(ids, nodes, tasks, timeout=5):
    await asyncio.sleep(timeout)
    # kill
    idx = random.randint(0, len(ids)-1)
    tasks[idx].cancel()

    asyncio.create_task(nodes[idx].raft_server.stop(None))
    print(f'[Master] kill {ids[idx]}')

    await asyncio.sleep(timeout)
    # recover
    nodeId = ids[idx]
    peers = ids[:]
    peers.remove(ids[idx])
    nodes[idx] = RaftNode(nodeId, peers)
    tasks[idx] = asyncio.create_task(
        nodes[idx].start()
    )
    print(f'[Master] recover {ids[idx]}')

    asyncio.create_task(randomCrash(ids, nodes, tasks, timeout))

async def leaderCrash(ids, nodes, tasks, timeout=5):
    await asyncio.sleep(timeout)
    logger = logging.getLogger('Monitor')

    # kill
    idx = 0
    for node in nodes:
        if node.isLeader():
            idx = nodes.index(node)
            break

    await nodes[idx].crash()
    logger.info(f'kill {ids[idx]}')

    await asyncio.sleep(timeout)
    # recover
    logger.info(f'recover {ids[idx]}')
    await nodes[idx].resume()
    # await asyncio.gather(leaderCrash(ids, nodes, tasks, timeout), nodes[idx].resume())
    

    

async def main():
    # set Monitor logger
    logger = logging.getLogger('Monitor')
    f = logging.Formatter('[%(name)s] %(msg)s')
    h = logging.StreamHandler()
    h.setFormatter(f)
    h.setLevel(logging.DEBUG)
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

    # ids = ['localhost:9000', 'localhost:9002', 'localhost:9004', 'localhost:9006', 'localhost:9008']
    ids = ['localhost:9000', 'localhost:9002', 'localhost:9004']
    nodes = []
    tasks = []
    for nodeId in ids:
        peers = ids[:]
        peers.remove(nodeId)
        nodes.append(RaftNode(nodeId, peers))
        tasks.append(asyncio.create_task(nodes[-1].start()))

    tasks.append(asyncio.create_task(leaderCrash(ids, nodes, tasks, timeout=10)))
    await asyncio.wait(tasks)
    
    

if __name__ == '__main__':
    if platform.system().lower() == 'windows':
        asyncio.set_event_loop_policy(
            asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())