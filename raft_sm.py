import os
from pprint import pprint
from typing import List

import json

from google.protobuf import json_format

from pumpkindb_pb2 import *

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
            if le.logIndex <= self.getLastLogIdx():
                if le.term != self.logs[le.logIndex]:
                    self.logs = self.logs[0:le.logIndex]
                    self.log_file.seek(0)
                    for old_le in self.logs[1:]:
                        line = json.dumps(json_format.MessageToDict(old_le))
                        self.log_file.write(line)
                        self.log_file.write('\n')
                    # throw the message left
                    self.log_file.truncate()
            line = json.dumps(json_format.MessageToDict(le))
            self.log_file.write(line)
            self.log_file.write('\n')
            self.log_file.flush()
            self.logs.append(le)


    
    def getTerm(self):
        return self._term
    
    def setTerm(self, term):
        self._term = term

    def getVotedFor(self):
        return self._votedFor

    def setVotedFor(self, votedFor):
        self._votedFor = votedFor
    
    def checkLogMatch(self, req: AppendEntriesRequest):
        if req.prevLogIndex <= self.getLastLogIdx():
            return req.prevLogTerm == self.logs[req.prevLogIndex].term
        else:
            return False

    def reloadPersistentStates(self):
        if not os.path.exists(self.log_path):
            self.log_file = open(self.log_path, 'w+')
            self.state_file = open(self.log_path + '.state', 'w+')
            self.setTerm(0)
            self.setVotedFor(None)
            self.logs = [LogEntry(
                    term=0,
                    logIndex=0
                )]
        else:
            self.log_file = open(self.log_path, 'r+')
            self.state_file = open(self.log_path, 'r+')
            self.readTerm()
            self.readVoteFor()
            self.readLogs()
    
    def readTerm(self):
        self._term = 0

    def readVoteFor(self):
        self._votedFor = None

    def readLogs(self):
        logs = [LogEntry(term=0, logIndex=0)]
        self.log_file.seek(0)
        line = self.log_file.readline().strip()
        while line:
            e = LogEntry()
            json_format.Parse(line.encode(), e)
            logs.append(e)
            line = self.log_file.readline().strip()
        self.logs = logs

    
    votedFor = property(getVotedFor, setVotedFor)
    term = property(getTerm, setTerm)
