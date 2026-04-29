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
        self.entries = [] # List of tuples: (id, {fields})
        self.last_id = (0, 0) # (time, seq)
    
    def validate_and_generate_id(self, entry_id_str):
        # Handle full auto-generation "*"
        if entry_id_str == b"*":
            t = int(time.time() * 1000)
            last_t, last_s = self.last_id
            if t == last_t:
                s = last_s + 1
            else:
                s = 0
            final_id = f"{t}-{s}".encode()
            return True, (t, s, final_id)

        # entry_id_str is b"time-seq" or b"time-*"
        try:
            t_part, s_part = entry_id_str.split(b"-")
            t = int(t_part)
        except ValueError:
            return False, "Invalid ID format"

        last_t, last_s = self.last_id

        if s_part == b"*":
            # Auto-generate sequence
            if t == 0:
                if last_t == 0: s = last_s + 1
                else: s = 1
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
        if not success:
            return False, result
        
        t, s, final_id = result
        self.last_id = (t, s)
        self.entries.append((final_id, fields))
        return True, final_id

def handle_client(client_connection):
    while True:
        try:
            data = client_connection.recv(1024)
            if not data:
                break
            
            parts = data.split(b"\r\n")
            if len(parts) < 3:
                continue
                
            command = parts[2].upper()
            
            if command == b"ECHO":
                message = parts[4]
                response = b"$" + str(len(message)).encode() + b"\r\n" + message + b"\r\n"
                client_connection.send(response)
            
            elif command == b"SET":
                key = parts[4]
                value = parts[6]
                expiry_time = None
                if len(parts) > 10 and parts[8].upper() == b"PX":
                    ms_to_live = int(parts[10])
                    expiry_time = time.time() + (ms_to_live / 1000)
                
                with data_condition:
                    data_store[key] = (value, expiry_time)
                client_connection.send(b"+OK\r\n")
            
            elif command == b"GET":
                key = parts[4]
                entry = data_store.get(key)
                if entry:
                    value, expiry_time = entry
                    if expiry_time and time.time() > expiry_time:
                        with data_condition:
                            del data_store[key]
                        client_connection.send(b"$-1\r\n")
                    else:
                        if isinstance(value, (list, RedisStream)):
                            client_connection.send(b"$-1\r\n")
                        else:
                            val_to_send = value
                            response = b"$" + str(len(val_to_send)).encode() + b"\r\n" + val_to_send + b"\r\n"
                            client_connection.send(response)
                else:
                    client_connection.send(b"$-1\r\n")

            elif command == b"TYPE":
                key = parts[4]
                entry = data_store.get(key)
                if not entry:
                    client_connection.send(b"+none\r\n")
                else:
                    value = entry[0]
                    if isinstance(value, list):
                        client_connection.send(b"+list\r\n")
                    elif isinstance(value, RedisStream):
                        client_connection.send(b"+stream\r\n")
                    else:
                        client_connection.send(b"+string\r\n")

            elif command == b"XADD":
                key = parts[4]
                entry_id_str = parts[6]
                raw_fields = parts[8:-1:2]
                raw_values = parts[9:-1:2]
                fields_dict = dict(zip(raw_fields, raw_values))
                
                with data_condition:
                    if key not in data_store:
                        data_store[key] = (RedisStream(), None)
                    
                    stream = data_store[key][0]
                    if not isinstance(stream, RedisStream):
                        stream = RedisStream()
                        data_store[key] = (stream, None)
                    
                    success, result = stream.add_entry(entry_id_str, fields_dict)
                    if success:
                        data_condition.notify_all()
                        response = b"$" + str(len(result)).encode() + b"\r\n" + result + b"\r\n"
                    else:
                        response = b"-" + result.encode() + b"\r\n"
                
                client_connection.send(response)

            elif command == b"RPUSH" or command == b"LPUSH":
                key = parts[4]
                new_values = parts[6:-1:2]
                with data_condition:
                    if key not in data_store:
                        data_store[key] = ([], None)
                    data_list, expiry = data_store[key]
                    if not isinstance(data_list, list):
                        data_list = []
                        data_store[key] = (data_list, None)
                    for v in new_values:
                        if command == b"RPUSH": data_list.append(v)
                        else: data_list.insert(0, v)
                    data_condition.notify_all()
                client_connection.send(f":{len(data_list)}\r\n".encode())

            elif command == b"LRANGE":
                key = parts[4]
                start, stop = int(parts[6]), int(parts[8])
                entry = data_store.get(key)
                if not entry or not isinstance(entry[0], list):
                    client_connection.send(b"*0\r\n")
                    continue
                data_list = entry[0]
                length = len(data_list)
                def normalize(idx, length):
                    if idx < 0: idx = length + idx
                    if idx < 0: idx = 0
                    if idx >= length: idx = length - 1
                    return idx
                s_idx, e_idx = normalize(start, length), normalize(stop, length)
                sub_list = [] if (start >= length or s_idx > e_idx) else data_list[s_idx : e_idx + 1]
                response = f"*{len(sub_list)}\r\n".encode()
                for item in sub_list:
                    response += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
                client_connection.send(response)

            elif command == b"LLEN":
                entry = data_store.get(parts[4])
                client_connection.send(f":{len(entry[0])}\r\n".encode() if entry and isinstance(entry[0], list) else b":0\r\n")

            elif command == b"LPOP":
                key = parts[4]
                count = int(parts[6]) if len(parts) > 6 else None
                with data_condition:
                    entry = data_store.get(key)
                    if not entry or not isinstance(entry[0], list) or len(entry[0]) == 0:
                        client_connection.send(b"$-1\r\n")
                    else:
                        data_list = entry[0]
                        if count is None:
                            val = data_list.pop(0)
                            client_connection.send(b"$" + str(len(val)).encode() + b"\r\n" + val + b"\r\n")
                        else:
                            popped = [data_list.pop(0) for _ in range(min(count, len(data_list)))]
                            res = f"*{len(popped)}\r\n".encode()
                            for i in popped: res += b"$" + str(len(i)).encode() + b"\r\n" + i + b"\r\n"
                            client_connection.send(res)

            elif command == b"BLPOP":
                key = parts[4]
                timeout_val = float(parts[-2])
                def get_popped_item():
                    entry = data_store.get(key)
                    return entry[0].pop(0) if entry and isinstance(entry[0], list) and len(entry[0]) > 0 else None
                with data_condition:
                    val = get_popped_item()
                    if timeout_val == 0:
                        while val is None:
                            data_condition.wait()
                            val = get_popped_item()
                    else:
                        if val is None:
                            data_condition.wait(timeout=timeout_val)
                            val = get_popped_item()
                if val is None: client_connection.send(b"*-1\r\n")
                else:
                    res = f"*2\r\n${len(key)}\r\n".encode() + key + b"\r\n" + b"$" + str(len(val)).encode() + b"\r\n" + val + b"\r\n"
                    client_connection.send(res)
                    
            elif command == b"PING":
                client_connection.send(b"+PONG\r\n")
                
        except (ConnectionResetError, IndexError, ValueError):
            break
    client_connection.close()

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        client_connection, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(client_connection,)).start()

if __name__ == "__main__":
    main()
