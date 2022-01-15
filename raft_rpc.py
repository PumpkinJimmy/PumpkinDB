import asyncio
import random
from pprint import pprint
from typing import List

from pumpkindb_pb2_grpc import (
    RaftServicer,
)
from pumpkindb_pb2 import *

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
                self.node.appendLog([entry])

                # apply
                if self.node.apply_task is None:
                    self.node.apply_task = asyncio.create_task(
                        self.node.applyEntriesCoro()
                    )
                # self.node.data[entry.command.key] = entry.command.value2

            if request.leaderCommit > self.node.commitIndex:
                self.node.commitIndex = min(request.leaderCommit, self.node.getLastLogIdx())
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
