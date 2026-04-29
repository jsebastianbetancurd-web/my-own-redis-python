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
        # Handle special symbols
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
        except ValueError:
            return (0, 0)

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
        except ValueError:
            return False, "Invalid ID format"

        last_t, last_s = self.last_id

        if s_part == b"*":
            if t == 0:
                s = last_s + 1 if last_t == 0 else 1
            elif t == last_t:
                s = last_s + 1
            elif t > last_t:
                s = 0
            else:
                return False, "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        else:
            try:
                s = int(s_part)
            except ValueError:
                return False, "Invalid ID format"

            if t == 0 and s == 0:
                return False, "ERR The ID specified in XADD must be greater than 0-0"
            if t < last_t or (t == last_t and s <= last_s):
                return False, "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        
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
        start_t, start_s = self.parse_id(start_id_bytes, is_end=False)
        end_t, end_s = self.parse_id(end_id_bytes, is_end=True)
        
        results = []
        for entry_id, fields in self.entries:
            # Parse the entry_id for comparison
            et, es = self.parse_id(entry_id)
            
            # Check if start <= entry <= end
            # Tuple comparison (t, s) works perfectly for Redis IDs
            if (et, es) >= (start_t, start_s) and (et, es) <= (end_t, end_s):
                results.append((entry_id, fields))
        return results

def handle_client(client_connection):
    while True:
        try:
            data = client_connection.recv(1024)
            if not data: break
            parts = data.split(b"\r\n")
            if len(parts) < 3: continue
            command = parts[2].upper()
            
            if command == b"ECHO":
                msg = parts[4]
                client_connection.send(b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n")
            
            elif command == b"SET":
                key, val = parts[4], parts[6]
                expiry = time.time() + (int(parts[10])/1000) if len(parts) > 10 and parts[8].upper() == b"PX" else None
                with data_condition: data_store[key] = (val, expiry)
                client_connection.send(b"+OK\r\n")
            
            elif command == b"GET":
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

            elif command == b"TYPE":
                entry = data_store.get(parts[4])
                if not entry: client_connection.send(b"+none\r\n")
                else:
                    v = entry[0]
                    if isinstance(v, list): client_connection.send(b"+list\r\n")
                    elif isinstance(v, RedisStream): client_connection.send(b"+stream\r\n")
                    else: client_connection.send(b"+string\r\n")

            elif command == b"XADD":
                key, id_str = parts[4], parts[6]
                # parts contains: [..., id, $len, field1, $len, value1, ...]
                # field1 is at index 8, field2 at 12... (jump by 4)
                # value1 is at index 10, value2 at 14... (jump by 4)
                raw_fields = parts[8:-1:4]
                raw_values = parts[10:-1:4]
                f_dict = dict(zip(raw_fields, raw_values))
                with data_condition:
                    if key not in data_store: data_store[key] = (RedisStream(), None)
                    stream = data_store[key][0]
                    success, res = stream.add_entry(id_str, f_dict)
                    if success:
                        data_condition.notify_all()
                        client_connection.send(b"$" + str(len(res)).encode() + b"\r\n" + res + b"\r\n")
                    else: client_connection.send(b"-" + res.encode() + b"\r\n")

            elif command == b"XRANGE":
                key, start, end = parts[4], parts[6], parts[8]
                entry = data_store.get(key)
                if not entry or not isinstance(entry[0], RedisStream):
                    client_connection.send(b"*0\r\n")
                else:
                    stream = entry[0]
                    results = stream.get_range(start, end)
                    # RESP Array of results
                    res = f"*{len(results)}\r\n".encode()
                    for eid, fields in results:
                        # Each result is an array of 2: [ID, [fields]]
                        res += b"*2\r\n"
                        res += b"$" + str(len(eid)).encode() + b"\r\n" + eid + b"\r\n"
                        # Fields array: [f1, v1, f2, v2...]
                        res += f"*{len(fields)*2}\r\n".encode()
                        for f, v in fields.items():
                            res += b"$" + str(len(f)).encode() + b"\r\n" + f + b"\r\n"
                            res += b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n"
                    client_connection.send(res)

            elif command == b"RPUSH" or command == b"LPUSH":
                key = parts[4]
                new_vals = parts[6:-1:2]
                with data_condition:
                    if key not in data_store: data_store[key] = ([], None)
                    d_list, _ = data_store[key]
                    for v in new_vals:
                        if command == b"RPUSH": d_list.append(v)
                        else: d_list.insert(0, v)
                    data_condition.notify_all()
                client_connection.send(f":{len(d_list)}\r\n".encode())

            elif command == b"LRANGE":
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

            elif command == b"LLEN":
                entry = data_store.get(parts[4])
                client_connection.send(f":{len(entry[0])}\r\n".encode() if entry and isinstance(entry[0], list) else b":0\r\n")

            elif command == b"LPOP":
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

            elif command == b"BLPOP":
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
                    
            elif command == b"PING": client_connection.send(b"+PONG\r\n")
        except (ConnectionResetError, IndexError, ValueError): break
    client_connection.close()

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()

if __name__ == "__main__": main()
