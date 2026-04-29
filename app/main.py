import socket
import threading
import time

# In-memory database
# Stores: {key: (value, expiry_time)}
data_store = {}
# Condition variable for blocking commands
data_condition = threading.Condition()

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
                        if isinstance(value, list):
                            val_to_send = value[0]
                        else:
                            val_to_send = value
                        response = b"$" + str(len(val_to_send)).encode() + b"\r\n" + val_to_send + b"\r\n"
                        client_connection.send(response)
                else:
                    client_connection.send(b"$-1\r\n")

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
                        if command == b"RPUSH":
                            data_list.append(v)
                        else:
                            data_list.insert(0, v)
                    
                    list_len = len(data_list)
                    # Notify any blocking BLPOP threads
                    data_condition.notify_all()
                
                client_connection.send(f":{list_len}\r\n".encode())

            elif command == b"LRANGE":
                key = parts[4]
                start = int(parts[6])
                stop = int(parts[8])
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
                s_idx = normalize(start, length)
                e_idx = normalize(stop, length)
                if start >= length or s_idx > e_idx:
                    sub_list = []
                else:
                    sub_list = data_list[s_idx : e_idx + 1]
                response = f"*{len(sub_list)}\r\n".encode()
                for item in sub_list:
                    response += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
                client_connection.send(response)

            elif command == b"LLEN":
                key = parts[4]
                entry = data_store.get(key)
                if not entry or not isinstance(entry[0], list):
                    client_connection.send(b":0\r\n")
                else:
                    client_connection.send(f":{len(entry[0])}\r\n".encode())

            elif command == b"TYPE":
                # TYPE key
                key = parts[4]
                entry = data_store.get(key)
                
                if not entry:
                    client_connection.send(b"+none\r\n")
                else:
                    value = entry[0]
                    if isinstance(value, list):
                        client_connection.send(b"+list\r\n")
                    else:
                        client_connection.send(b"+string\r\n")

            elif command == b"LPOP":
                key = parts[4]
                count = None
                if len(parts) > 6:
                    try: count = int(parts[6])
                    except ValueError: pass
                
                with data_condition:
                    entry = data_store.get(key)
                    if not entry or not isinstance(entry[0], list) or len(entry[0]) == 0:
                        client_connection.send(b"$-1\r\n")
                    else:
                        data_list = entry[0]
                        if count is None:
                            removed_val = data_list.pop(0)
                            client_connection.send(b"$" + str(len(removed_val)).encode() + b"\r\n" + removed_val + b"\r\n")
                        else:
                            popped = [data_list.pop(0) for _ in range(min(count, len(data_list)))]
                            response = f"*{len(popped)}\r\n".encode()
                            for item in popped:
                                response += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
                            client_connection.send(response)

            elif command == b"BLPOP":
                # BLPOP key1 [key2 ...] timeout
                key = parts[4]
                raw_timeout = parts[-2]
                timeout_val = float(raw_timeout)
                
                def get_popped_item():
                    entry = data_store.get(key)
                    if entry and isinstance(entry[0], list) and len(entry[0]) > 0:
                        return entry[0].pop(0)
                    return None

                with data_condition:
                    val = get_popped_item()
                    if timeout_val == 0:
                        # Indefinite wait: must use while loop to ensure we never return Null
                        while val is None:
                            data_condition.wait()
                            val = get_popped_item()
                    else:
                        # Timed wait: wait once and return what we find
                        if val is None:
                            data_condition.wait(timeout=timeout_val)
                            val = get_popped_item()
                
                if val is None:
                    # Only possible for non-zero timeouts
                    client_connection.send(b"*-1\r\n")
                else:
                    response = f"*2\r\n${len(key)}\r\n".encode() + key + b"\r\n"
                    response += b"$" + str(len(val)).encode() + b"\r\n" + val + b"\r\n"
                    client_connection.send(response)
                    
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
