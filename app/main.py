import socket
import threading
import time

# In-memory database
# Stores: {key: (value, expiry_time)}
data_store = {}
# Condition variable for blocking commands
data_condition = threading.Condition()

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

def handle_client(client_connection):
    while True:
        try:
            data = client_connection.recv(1024)
            if not data: break
            parts = data.split(b"\r\n")
            if len(parts) < 3: continue
            cmd = parts[2].upper()
            
            if cmd == b"ECHO":
                msg = parts[4]
                client_connection.send(b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n")
            
            elif cmd == b"SET":
                key, val = parts[4], parts[6]
                expiry = time.time() + (int(parts[10])/1000) if len(parts) > 10 and parts[8].upper() == b"PX" else None
                with data_condition: data_store[key] = (val, expiry)
                client_connection.send(b"+OK\r\n")
            
            elif cmd == b"GET":
                entry = data_store.get(parts[4])
                if entry:
                    val, exp = entry
                    if exp and time.time() > exp:
                        with data_condition: del data_store[parts[4]]
                        client_connection.send(b"$-1\r\n")
                    else:
                        if isinstance(val, (list, RedisStream)): client_connection.send(b"$-1\r\n")
                        else: client_connection.send(b"$" + str(len(val)).encode() + b"\r\n" + val + b"\r\n")
                else: client_connection.send(b"$-1\r\n")

            elif cmd == b"TYPE":
                entry = data_store.get(parts[4])
                if not entry: client_connection.send(b"+none\r\n")
                else:
                    v = entry[0]
                    if isinstance(v, list): client_connection.send(b"+list\r\n")
                    elif isinstance(v, RedisStream): client_connection.send(b"+stream\r\n")
                    else: client_connection.send(b"+string\r\n")

            elif cmd == b"XADD":
                key, id_str = parts[4], parts[6]
                raw_f, raw_v = parts[8:-1:4], parts[10:-1:4]
                f_dict = dict(zip(raw_f, raw_v))
                with data_condition:
                    if key not in data_store: data_store[key] = (RedisStream(), None)
                    stream = data_store[key][0]
                    success, res = stream.add_entry(id_str, f_dict)
                    if success:
                        data_condition.notify_all()
                        client_connection.send(b"$" + str(len(res)).encode() + b"\r\n" + res + b"\r\n")
                    else: client_connection.send(b"-" + res.encode() + b"\r\n")

            elif cmd == b"XRANGE":
                key, start, end = parts[4], parts[6], parts[8]
                entry = data_store.get(key)
                if not entry or not isinstance(entry[0], RedisStream): client_connection.send(b"*0\r\n")
                else:
                    stream = entry[0]
                    results = stream.get_range(start, end)
                    res = f"*{len(results)}\r\n".encode()
                    for eid, fields in results: res += encode_stream_entry(eid, fields)
                    client_connection.send(res)

            elif cmd == b"XREAD":
                # XREAD [BLOCK ms] STREAMS key1 key2 id1 id2
                streams_idx = -1
                for i, p in enumerate(parts):
                    if p.upper() == b"STREAMS":
                        streams_idx = i
                        break
                
                # Number of keys = (remaining elements - 1) / 2
                # e.g. STREAMS k1 k2 i1 i2 -> indices 4 5 6 7. (7-4+1) = 4 elements.
                # parts[-1] is empty, so we look at elements between streams_idx and end
                remaining = parts[streams_idx+1 : -1]
                num_keys = len(remaining) // 2
                keys = remaining[:num_keys]
                ids = remaining[num_keys:]
                
                # Only skipping lengths ($len)
                # Wait, STREAMS is at parts[streams_idx].
                # RESP format means every value has a $len before it.
                # Let's use a simpler way: jump by 2
                actual_keys = keys[1::2]
                actual_ids = ids[1::2]
                
                final_results = []
                for k, tid in zip(actual_keys, actual_ids):
                    entry = data_store.get(k)
                    if entry and isinstance(entry[0], RedisStream):
                        items = entry[0].get_after(tid)
                        if items: final_results.append((k, items))
                
                if not final_results:
                    client_connection.send(b"$-1\r\n")
                else:
                    res = f"*{len(final_results)}\r\n".encode()
                    for k, items in final_results:
                        res += b"*2\r\n"
                        res += b"$" + str(len(k)).encode() + b"\r\n" + k + b"\r\n"
                        res += f"*{len(items)}\r\n".encode()
                        for eid, fields in items: res += encode_stream_entry(eid, fields)
                    client_connection.send(res)

            elif cmd == b"RPUSH" or cmd == b"LPUSH":
                key, new_vals = parts[4], parts[6:-1:2]
                # Wait, indices for RPUSH/LPUSH are also affected by $len
                # key is at parts[4], lengths at 3, 5, 7... values at 4, 6, 8...
                # Actually, my previous slice [6:-1:2] was working but I should check
                # parts: [0:*n, 1:$len, 2:RPUSH, 3:$len, 4:key, 5:$len, 6:val1, 7:$len, 8:val2...]
                # Yes, index 6, 8, 10 are the values!
                with data_condition:
                    if key not in data_store: data_store[key] = ([], None)
                    d_list, _ = data_store[key]
                    for v in new_vals:
                        if cmd == b"RPUSH": d_list.append(v)
                        else: d_list.insert(0, v)
                    data_condition.notify_all()
                client_connection.send(f":{len(d_list)}\r\n".encode())

            elif cmd == b"LRANGE":
                key, s, e = parts[4], int(parts[6]), int(parts[8])
                entry = data_store.get(key)
                if not entry or not isinstance(entry[0], list): client_connection.send(b"*0\r\n")
                else:
                    l = entry[0]
                    def norm(i, n):
                        if i < 0: i = n + i
                        return max(0, min(i, n-1))
                    si, ei = norm(s, len(l)), norm(e, len(l))
                    sub = [] if (s >= len(l) or si > ei) else l[si : ei + 1]
                    res = f"*{len(sub)}\r\n".encode()
                    for i in sub: res += b"$" + str(len(i)).encode() + b"\r\n" + i + b"\r\n"
                    client_connection.send(res)

            elif cmd == b"LLEN":
                entry = data_store.get(parts[4])
                client_connection.send(f":{len(entry[0])}\r\n".encode() if entry and isinstance(entry[0], list) else b":0\r\n")

            elif cmd == b"LPOP":
                key = parts[4]
                cnt = int(parts[6]) if len(parts) > 6 else None
                with data_condition:
                    entry = data_store.get(key)
                    if not entry or not isinstance(entry[0], list) or len(entry[0]) == 0: client_connection.send(b"$-1\r\n")
                    else:
                        l = entry[0]
                        if cnt is None:
                            v = l.pop(0)
                            client_connection.send(b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n")
                        else:
                            p = [l.pop(0) for _ in range(min(cnt, len(l)))]
                            res = f"*{len(p)}\r\n".encode()
                            for i in p: res += b"$" + str(len(i)).encode() + b"\r\n" + i + b"\r\n"
                            client_connection.send(res)

            elif cmd == b"BLPOP":
                key, tval = parts[4], float(parts[-2])
                def get_p():
                    ent = data_store.get(key)
                    return ent[0].pop(0) if ent and isinstance(ent[0], list) and len(ent[0]) > 0 else None
                with data_condition:
                    v = get_p()
                    if tval == 0:
                        while v is None: data_condition.wait(); v = get_p()
                    else:
                        if v is None: data_condition.wait(timeout=tval); v = get_p()
                if v is None: client_connection.send(b"*-1\r\n")
                else: client_connection.send(f"*2\r\n${len(key)}\r\n".encode() + key + b"\r\n" + b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n")
                    
            elif cmd == b"PING": client_connection.send(b"+PONG\r\n")
        except (ConnectionResetError, IndexError, ValueError): break
    client_connection.close()

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()

if __name__ == "__main__": main()
