import socket
import threading
import time
import argparse

# Global configuration
config = {
    "role": "master",
    "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
    "master_repl_offset": 0,
    "master_host": None,
    "master_port": None,
    "port": 6379
}

# In-memory database
# Stores: {key: (value, expiry_time)}
data_store = {}
key_versions = {}
# Condition variable for blocking commands
data_condition = threading.Condition()

def mark_modified(key):
    key_versions[key] = key_versions.get(key, 0) + 1

class RedisStream:
    def __init__(self):
        self.entries = [] # List of tuples: (final_id_bytes, fields_dict)
        self.last_id = (0, 0) # (time, seq)
    
    def parse_id(self, id_bytes, is_end=False):
        if id_bytes == b"-": return (0, 0)
        if id_bytes == b"+": return (float('inf'), float('inf'))
        try:
            if b"-" in id_bytes:
                t_part, s_part = id_bytes.split(b"-")
                return (int(t_part), int(s_part))
            else:
                t = int(id_bytes)
                s = float('inf') if is_end else 0
                return (t, s)
        except ValueError: return (0, 0)

    def validate_and_generate_id(self, entry_id_str):
        if entry_id_str == b"*":
            t = int(time.time() * 1000)
            last_t, last_s = self.last_id
            s = last_s + 1 if t == last_t else 0
            final_id = f"{t}-{s}".encode()
            return True, (t, s, final_id)
        try:
            t_part, s_part = entry_id_str.split(b"-")
            t = int(t_part)
        except ValueError: return False, "Invalid ID format"
        last_t, last_s = self.last_id
        if s_part == b"*":
            if t == 0: s = last_s + 1 if last_t == 0 else 1
            elif t == last_t: s = last_s + 1
            elif t > last_t: s = 0
            else: return False, "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        else:
            try: s = int(s_part)
            except ValueError: return False, "Invalid ID format"
            if t == 0 and s == 0: return False, "ERR The ID specified in XADD must be greater than 0-0"
            if t < last_t or (t == last_t and s <= last_s): return False, "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        final_id = f"{t}-{s}".encode()
        return True, (t, s, final_id)

    def add_entry(self, entry_id_str, fields):
        success, result = self.validate_and_generate_id(entry_id_str)
        if not success: return False, result
        t, s, final_id = result
        self.last_id = (t, s)
        self.entries.append((final_id, fields))
        return True, final_id

    def get_range(self, start_id_bytes, end_id_bytes):
        st, ss = self.parse_id(start_id_bytes, is_end=False)
        et, es = self.parse_id(end_id_bytes, is_end=True)
        results = []
        for eid, fields in self.entries:
            t, s = self.parse_id(eid)
            if (t, s) >= (st, ss) and (t, s) <= (et, es): results.append((eid, fields))
        return results

    def get_after(self, start_id_bytes):
        st, ss = self.parse_id(start_id_bytes)
        results = []
        for eid, fields in self.entries:
            t, s = self.parse_id(eid)
            if (t, s) > (st, ss): results.append((eid, fields))
        return results

def encode_stream_entry(eid, fields):
    res = b"*2\r\n"
    res += b"$" + str(len(eid)).encode() + b"\r\n" + eid + b"\r\n"
    res += f"*{len(fields)*2}\r\n".encode()
    for f, v in fields.items():
        res += b"$" + str(len(f)).encode() + b"\r\n" + f + b"\r\n"
        res += b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n"
    return res

def process_command(cmd, args):
    cmd = cmd.upper()
    if cmd == b"PING":
        return b"+PONG\r\n"
    elif cmd == b"REPLCONF":
        return b"+OK\r\n"
    elif cmd == b"PSYNC":
        res_str = f"FULLRESYNC {config['master_replid']} {config['master_repl_offset']}\r\n"
        # Empty RDB file hex representation
        rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000ff8c6e06255a95cb7d"
        rdb_bytes = bytes.fromhex(rdb_hex)
        res_bytes = b"+" + res_str.encode()
        res_bytes += b"$" + str(len(rdb_bytes)).encode() + b"\r\n" + rdb_bytes
        return res_bytes
    elif cmd == b"INFO":
        if args and args[0].upper() == b"REPLICATION":
            res_parts = [
                f"role:{config['role']}",
                f"master_replid:{config['master_replid']}",
                f"master_repl_offset:{config['master_repl_offset']}"
            ]
            res_str = "\n".join(res_parts)
            return b"$" + str(len(res_str)).encode() + b"\r\n" + res_str.encode() + b"\r\n"
        return b"-ERR syntax error\r\n"
    elif cmd == b"ECHO":
        if not args: return b"-ERR wrong number of arguments for 'echo' command\r\n"
        msg = args[0]
        return b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
    elif cmd == b"SET":
        if len(args) < 2: return b"-ERR wrong number of arguments for 'set' command\r\n"
        key, val = args[0], args[1]
        expiry = None
        if len(args) >= 4 and args[2].upper() == b"PX":
            try: expiry = time.time() + (int(args[3])/1000)
            except ValueError: pass
        with data_condition:
            data_store[key] = (val, expiry)
            mark_modified(key)
        return b"+OK\r\n"
    elif cmd == b"GET":
        if not args: return b"-ERR wrong number of arguments for 'get' command\r\n"
        key = args[0]
        entry = data_store.get(key)
        if entry:
            val, exp = entry
            if exp and time.time() > exp:
                with data_condition:
                    del data_store[key]
                    mark_modified(key)
                return b"$-1\r\n"
            else:
                if isinstance(val, (list, RedisStream)): return b"$-1\r\n"
                else: return b"$" + str(len(val)).encode() + b"\r\n" + val + b"\r\n"
        else: return b"$-1\r\n"
    elif cmd == b"INCR":
        if not args: return b"-ERR wrong number of arguments for 'incr' command\r\n"
        key = args[0]
        with data_condition:
            entry = data_store.get(key)
            if entry:
                val, exp = entry
                try:
                    new_val = int(val) + 1
                    data_store[key] = (str(new_val).encode(), exp)
                    mark_modified(key)
                    return f":{new_val}\r\n".encode()
                except (ValueError, TypeError):
                    return b"-ERR value is not an integer or out of range\r\n"
            else:
                data_store[key] = (b"1", None)
                mark_modified(key)
                return b":1\r\n"
    elif cmd == b"TYPE":
        if not args: return b"+none\r\n"
        key = args[0]
        entry = data_store.get(key)
        if not entry: return b"+none\r\n"
        else:
            v = entry[0]
            if isinstance(v, list): return b"+list\r\n"
            elif isinstance(v, RedisStream): return b"+stream\r\n"
            else: return b"+string\r\n"
    elif cmd == b"XADD":
        if len(args) < 4: return b"-ERR wrong number of arguments for 'xadd' command\r\n"
        key, id_str = args[0], args[1]
        f_dict = {}
        for i in range(2, len(args), 2):
            if i+1 < len(args): f_dict[args[i]] = args[i+1]
        with data_condition:
            if key not in data_store: data_store[key] = (RedisStream(), None)
            stream = data_store[key][0]
            if not isinstance(stream, RedisStream): return b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            success, res = stream.add_entry(id_str, f_dict)
            if success:
                mark_modified(key)
                data_condition.notify_all()
                return b"$" + str(len(res)).encode() + b"\r\n" + res + b"\r\n"
            else: return b"-" + res.encode() + b"\r\n"
    elif cmd == b"XRANGE":
        if len(args) < 3: return b"-ERR wrong number of arguments for 'xrange' command\r\n"
        key, start, end = args[0], args[1], args[2]
        entry = data_store.get(key)
        if not entry or not isinstance(entry[0], RedisStream): return b"*0\r\n"
        else:
            stream = entry[0]
            results = stream.get_range(start, end)
            res = f"*{len(results)}\r\n".encode()
            for eid, fields in results: res += encode_stream_entry(eid, fields)
            return res
    elif cmd == b"XREAD":
        block_ms = None
        streams_idx = -1
        for i, arg in enumerate(args):
            if arg.upper() == b"BLOCK" and i+1 < len(args): block_ms = int(args[i+1])
            if arg.upper() == b"STREAMS": streams_idx = i; break
        if streams_idx == -1: return b"-ERR missing STREAMS keyword\r\n"
        remaining = args[streams_idx+1:]
        num_keys = len(remaining) // 2
        actual_keys = remaining[:num_keys]
        raw_ids = remaining[num_keys:]
        actual_ids = []
        for k, tid in zip(actual_keys, raw_ids):
            if tid == b"$":
                entry = data_store.get(k)
                if entry and isinstance(entry[0], RedisStream):
                    lt, ls = entry[0].last_id
                    actual_ids.append(f"{lt}-{ls}".encode())
                else: actual_ids.append(b"0-0")
            else: actual_ids.append(tid)

        def get_results():
            results = []
            for k, tid in zip(actual_keys, actual_ids):
                entry = data_store.get(k)
                if entry and isinstance(entry[0], RedisStream):
                    items = entry[0].get_after(tid)
                    if items: results.append((k, items))
            return results

        with data_condition:
            final_results = get_results()
            if not final_results and block_ms is not None:
                if block_ms == 0:
                    while not final_results:
                        data_condition.wait()
                        final_results = get_results()
                else:
                    data_condition.wait(timeout=block_ms/1000)
                    final_results = get_results()
        if not final_results: return b"*-1\r\n"
        else:
            res = f"*{len(final_results)}\r\n".encode()
            for k, items in final_results:
                res += b"*2\r\n" + b"$" + str(len(k)).encode() + b"\r\n" + k + b"\r\n"
                res += f"*{len(items)}\r\n".encode()
                for eid, fields in items: res += encode_stream_entry(eid, fields)
            return res
    elif cmd == b"RPUSH" or cmd == b"LPUSH":
        if len(args) < 2: return b"-ERR wrong number of arguments\r\n"
        key, new_vals = args[0], args[1:]
        with data_condition:
            if key not in data_store: data_store[key] = ([], None)
            d_list, _ = data_store[key]
            if not isinstance(d_list, list): return b"-ERR WRONGTYPE\r\n"
            for v in new_vals:
                if cmd == b"RPUSH": d_list.append(v)
                else: d_list.insert(0, v)
            mark_modified(key)
            data_condition.notify_all()
        return f":{len(d_list)}\r\n".encode()
    elif cmd == b"LRANGE":
        if len(args) < 3: return b"-ERR wrong number of arguments\r\n"
        key, s, e = args[0], int(args[1]), int(args[2])
        entry = data_store.get(key)
        if not entry or not isinstance(entry[0], list): return b"*0\r\n"
        else:
            l = entry[0]
            def norm(i, n):
                if i < 0: i = n + i
                return max(0, min(i, n-1))
            if not l: return b"*0\r\n"
            si, ei = norm(s, len(l)), norm(e, len(l))
            sub = [] if (s >= len(l) or (s >= 0 and s > e and e >= 0) or (s < 0 and e < 0 and s > e)) else l[si : ei + 1]
            if s >= 0 and e >= 0 and s > e: sub = []
            res = f"*{len(sub)}\r\n".encode()
            for i in sub: res += b"$" + str(len(i)).encode() + b"\r\n" + i + b"\r\n"
            return res
    elif cmd == b"LLEN":
        if not args: return b":0\r\n"
        entry = data_store.get(args[0])
        return f":{len(entry[0])}\r\n".encode() if entry and isinstance(entry[0], list) else b":0\r\n"
    elif cmd == b"LPOP":
        if not args: return b"$-1\r\n"
        key, cnt = args[0], int(args[1]) if len(args) > 1 else None
        with data_condition:
            entry = data_store.get(key)
            if not entry or not isinstance(entry[0], list) or len(entry[0]) == 0: return b"$-1\r\n"
            else:
                l = entry[0]
                if cnt is None:
                    v = l.pop(0)
                    mark_modified(key)
                    return b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n"
                else:
                    p = [l.pop(0) for _ in range(min(cnt, len(l)))]
                    if p: mark_modified(key)
                    res = f"*{len(p)}\r\n".encode()
                    for i in p: res += b"$" + str(len(i)).encode() + b"\r\n" + i + b"\r\n"
                    return res
    elif cmd == b"BLPOP":
        if len(args) < 2: return b"*-1\r\n"
        key, tval = args[0], float(args[-1])
        def get_p():
            ent = data_store.get(key)
            return ent[0].pop(0) if ent and isinstance(ent[0], list) and len(ent[0]) > 0 else None
        with data_condition:
            v = get_p()
            if tval == 0:
                while v is None: data_condition.wait(); v = get_p()
            else:
                if v is None: data_condition.wait(timeout=tval); v = get_p()
            if v is not None:
                mark_modified(key)
        if v is None: return b"*-1\r\n"
        else: return f"*2\r\n${len(key)}\r\n".encode() + key + b"\r\n" + b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n"
    return b"-ERR unknown command\r\n"

def parse_resp(data):
    if not data: return None, b""
    if data[0:1] != b"*": return None, b""
    try:
        lines = data.split(b"\r\n")
        num_args = int(lines[0][1:])
        args = []
        curr = 1
        for _ in range(num_args):
            if lines[curr][0:1] == b"$":
                args.append(lines[curr+1])
                curr += 2
        return args, b"\r\n".join(lines[curr:])
    except: return None, b""

def handle_client(client_connection):
    in_transaction = False
    transaction_queue = []
    watched_keys = {}

    while True:
        try:
            data = client_connection.recv(4096)
            if not data: break
            
            while data:
                args, rest = parse_resp(data)
                if not args: break
                data = rest
                
                cmd = args[0].upper()
                cmd_args = args[1:]
                
                if cmd == b"MULTI":
                    in_transaction = True
                    client_connection.send(b"+OK\r\n")
                elif cmd == b"DISCARD":
                    if not in_transaction:
                        client_connection.send(b"-ERR DISCARD without MULTI\r\n")
                    else:
                        in_transaction = False
                        transaction_queue = []
                        watched_keys = {}
                        client_connection.send(b"+OK\r\n")
                elif cmd == b"EXEC":
                    if not in_transaction:
                        client_connection.send(b"-ERR EXEC without MULTI\r\n")
                    else:
                        abort_transaction = False
                        for k, v in watched_keys.items():
                            if key_versions.get(k, 0) != v:
                                abort_transaction = True
                                break
                        
                        if abort_transaction:
                            client_connection.send(b"*-1\r\n")
                        else:
                            if not transaction_queue:
                                client_connection.send(b"*0\r\n")
                            else:
                                res = f"*{len(transaction_queue)}\r\n".encode()
                                for q_cmd, q_args in transaction_queue:
                                    res += process_command(q_cmd, q_args)
                                client_connection.send(res)
                        
                        in_transaction = False
                        transaction_queue = []
                        watched_keys = {}
                elif cmd == b"WATCH":
                    if in_transaction:
                        client_connection.send(b"-ERR WATCH inside MULTI is not allowed\r\n")
                    else:
                        for k in cmd_args:
                            if k not in watched_keys:
                                watched_keys[k] = key_versions.get(k, 0)
                        client_connection.send(b"+OK\r\n")
                elif cmd == b"UNWATCH":
                    watched_keys = {}
                    client_connection.send(b"+OK\r\n")
                else:
                    if in_transaction:
                        transaction_queue.append((cmd, cmd_args))
                        client_connection.send(b"+QUEUED\r\n")
                    else:
                        resp = process_command(cmd, cmd_args)
                        client_connection.send(resp)
        except (ConnectionResetError, IndexError, ValueError): break
    client_connection.close()

def send_handshake():
    host = config["master_host"]
    port = config["master_port"]
    replica_port = str(config["port"]).encode()
    with socket.create_connection((host, port)) as master_conn:
        # 1. PING
        master_conn.send(b"*1\r\n$4\r\nPING\r\n")
        master_conn.recv(4096)
        
        # 2. REPLCONF listening-port <PORT>
        master_conn.send(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + str(len(replica_port)).encode() + b"\r\n" + replica_port + b"\r\n")
        master_conn.recv(4096)
        
        # 3. REPLCONF capa psync2
        master_conn.send(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        master_conn.recv(4096)
        
        # 4. PSYNC ? -1
        master_conn.send(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        master_conn.recv(4096)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", type=str)
    args = parser.parse_args()
    
    config["port"] = args.port
    
    if args.replicaof:
        config["role"] = "slave"
        m_host, m_port = args.replicaof.split()
        config["master_host"] = m_host
        config["master_port"] = int(m_port)
        
        threading.Thread(target=send_handshake).start()
    
    server_socket = socket.create_server(("localhost", args.port), reuse_port=True)
    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()

if __name__ == "__main__": main()
