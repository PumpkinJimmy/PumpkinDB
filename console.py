import uuid
import string

import grpc

from pumpkindb_pb2_grpc import PumpkinDBStub
from pumpkindb_pb2 import GreetRequest
import pumpkindb_pb2
from prompt_toolkit import prompt, PromptSession, HTML
from prompt_toolkit import print_formatted_text as print

def parseToken(line, p):
    while p < len(line) and line[p] in string.whitespace:
        p += 1
    if p >= len(line):
        return None, len(line)
    if line[p] in ('"', "'"):
        end_p = line.find(line[p], p+1)
        if end_p < 0:
            raise Exception(f'Col {p}: Missing {line[p]}')
        return line[p+1:end_p], end_p + 1
    else:
        p2 = p
        while p2 < len(line) and line[p2] not in string.whitespace:
            p2 += 1
        return line[p:p2], p2

if __name__ == '__main__':
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = PumpkinDBStub(channel)
        clientId = str(uuid.uuid4())
        commandId = 0
        session = PromptSession(HTML('<ansiblue>PumpkinDB=> </ansiblue>'))
        while 1:
            line = session.prompt()
            idx = line.find(' ')
            if idx < 0: idx = len(line)
            op = line[:idx]
            if op.lower() == 'quit':
                print("Bye")
                break
            elif op.lower() == 'get':
                key, idx = parseToken(line, idx)
                endpos, _ = parseToken(line, idx)
                if endpos is not None:
                    print(key, endpos)
                    print(HTML('<ansired>Unknown usage</ansired>'))
                    continue
                commandId += 1
                resp = stub.Get(pumpkindb_pb2.GetRequest(
                    clientId=clientId,
                    commandId=commandId,
                    key=key,
                ))
                print(f'{key}: {resp.value}')
            elif op.lower() == 'put':
                commandId += 1
                key, idx = parseToken(line, idx)
                value, idx = parseToken(line, idx)
                endpos, _ = parseToken(line, idx)
                if key is None or value is None or endpos is not None:
                    print(HTML('<ansired>Unknown usage</ansired>'))
                    continue
                resp = stub.Put(pumpkindb_pb2.PutRequest(
                    clientId=clientId,
                    commandId=commandId,
                    key=key,
                    value=value))
                print(f'{key}: {resp.value}')
            elif op.lower() == 'del':
                print(HTML('<ansiyellow>Not implement yet</ansiyellow>'))