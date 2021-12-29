import struct
from server import BinLogger
with open('pumpkindb.binlog', 'rb') as f:
    buf = f.read()
    p = 0
    print(f'Type\tKey\tValue 1\t\tValue 2')
    while p < len(buf):
        op = struct.unpack_from('i', buf, p)[0]
        p += struct.calcsize('i')
        if op == BinLogger.OP_CREATE:
            len_key, len_val = struct.unpack_from('II', buf, p)
            p += struct.calcsize('II')
            key = buf[p:p+len_key]
            p += len_key
            val = buf[p:p+len_val]
            p += len_val
            print(f'Create\t{key.decode()}\t{val.decode()}')
        elif op == BinLogger.OP_PUT:
            len_key, len_old_val, len_new_val = struct.unpack_from('III', buf, p)
            p += struct.calcsize('III')
            key = buf[p:p+len_key]
            p += len_key
            old_val = buf[p:p+len_old_val]
            p += len_old_val
            new_val = buf[p:p+len_new_val]
            p += len_new_val
            print(f'Put\t{key.decode()}\t{old_val.decode()}\t\t{new_val.decode()}')
        else:
            print(f"Unknown OP: { op }")