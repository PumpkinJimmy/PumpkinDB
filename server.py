import os
import struct
import asyncio

import grpc
from concurrent import futures

from pumpkindb_pb2_grpc import PumpkinDBServicer
from pumpkindb_pb2 import GreetResponse, ValueResponse
from pumpkindb_pb2_grpc import add_PumpkinDBServicer_to_server
import settings

class BinLogger:
    OP_CREATE = 0
    OP_PUT = 1
    OP_DEL = 2
    def __init__(self, binlog_path):
        self.binlog_path = binlog_path
        # if not os.path.exists(self.binlog_path):
        #     self.logfile = open(self.binlog_path, 'wb')
        # else:
        self.logfile = open(self.binlog_path, 'ab')
    def logCreate(self, key, val):
        key = key.encode()
        val = val.encode()
        self.logfile.write(struct.pack('iII', self.OP_CREATE, len(key), len(val)) + key + val)
        self.logfile.flush()
    def logPut(self, key, old_val, new_val):
        key = key.encode()
        old_val = old_val.encode()
        new_val = new_val.encode()
        self.logfile.write(struct.pack('iIII', self.OP_PUT, len(key), len(old_val), len(new_val)) + key + old_val + new_val)
        self.logfile.flush()

    def logDel(self, key):
        pass

class PumpkinDBServer(PumpkinDBServicer):
    def __init__(self):
        super().__init__()
        self.data = {'x': '5'}
        self.current_size = 0
        self.data_path = settings.data_path
        self.binlogger = BinLogger(settings.binlog_path)
        if os.path.exists(self.data_path):
            self.data_file = open(self.data_path, 'rb+')
            self.data = self.deserializeEntrys(self.data_file.read())
            self.data_file.seek(0)
        else:
            self.data_file = open(self.data_path, 'wb')
    
    def getEntryBufSize(self, k, v):
        return 4 + 4 + len(k) + len(v)
    
    def getEntrysBufSize(self, entrys):
        return 4 + sum(map(lambda p: self.getEntryBufSize(*p), entrys.items()))
    
    def serializeEntry(self, k, v):
        k = k.encode()
        v = v.encode()
        return struct.pack('II', len(k), len(v)) + k + v
    
    def serializeEntrys(self, entrys):
        return struct.pack('I', len(entrys)) + b''.join(map(lambda p: self.serializeEntry(*p), entrys.items()))
    
    def deserializeEntry(self, buf, offset=0):
        len_k, len_v = struct.unpack('II', buf[offset:offset+8])
        return (
            buf[offset+8:offset+8+len_k].decode(),
            buf[offset+8+len_k:offset+8+len_k+len_v].decode(),
            offset + 8 + len_k + len_v
        )

    def deserializeEntrys(self, buf):
        len_entries = struct.unpack('I', buf[:4])[0]
        p = 4
        res = {}
        for i in range(len_entries):
            k, v, p = self.deserializeEntry(buf, p)
            res[k] = v
        return dict(res)
    
    def write_file(self):
        self.data_file.write(self.serializeEntrys(self.data))
        self.data_file.flush()
    
    def __del__(self):
        self.write_file()

    def Greet(self, request, context):
        return GreetResponse(status="ok", msg=f"Hello, {request.clientId}!")

    def Get(self, request, context):
        key = request.key
        if key not in self.data:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f'Key not found: {key}')
            raise Exception(f'Key not found: {key}')
        return ValueResponse(value=self.data[key])
    
    def Put(self, request, context):
        # first log
        if request.key in self.data:
            self.binlogger.logPut(request.key, self.data[request.key], request.value)
        else:
            self.binlogger.logCreate(request.key, request.value)
        # then write data
        self.data[request.key] = request.value
        self.current_size = self.getEntrysBufSize(self.data)
        if self.current_size >= 4*1024-8:
            self.data_file.write(self.serializeEntrys(self.data))
            self.data_file.flush()
            self.data_file.seek(0)
        return ValueResponse(value=self.data[request.key])
    
    def Del(self, request, context):
        key = request.key
        if key not in self.data:
            context.set_code(grpc.Status.NOT_FOUND)
            context.set_details(f'Key not found: {key}')
            raise Exception(f'Key not found: {key}')
        del self.data[key]
async def startServer():
    server = grpc.aio.server(
        futures.ThreadPoolExecutor(max_workers=5),
        maximum_concurrent_rpcs=5)
    s = PumpkinDBServer()
    add_PumpkinDBServicer_to_server(s, server)
    server.add_insecure_port('localhost:50051')
    await server.start()
    print("Server Start")
    # FIXME: Never exec finally to persist.
    try:
        await server.wait_for_termination()
    except Exception as e:
        await server.stop(None)
    finally:
        s.write_file()

if __name__ == '__main__':
    # asyncio.run(startServer())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait([startServer()]))
    loop.close()