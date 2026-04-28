import socket
import threading
import time

# In-memory database
# Stores: {key: (value, expiry_time)}
data_store = {}

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
                # SET key value [PX ms]
                key = parts[4]
                value = parts[6]
                expiry_time = None
                
                # Check for PX argument (index 8) and its value (index 10)
                if len(parts) > 10 and parts[8].upper() == b"PX":
                    ms_to_live = int(parts[10])
                    expiry_time = time.time() + (ms_to_live / 1000)
                
                data_store[key] = (value, expiry_time)
                client_connection.send(b"+OK\r\n")
            
            elif command == b"GET":
                # GET key
                key = parts[4]
                entry = data_store.get(key)
                
                if entry:
                    value, expiry_time = entry
                    # Check if the key has expired
                    if expiry_time and time.time() > expiry_time:
                        del data_store[key]
                        client_connection.send(b"$-1\r\n")
                    else:
                        # Handle both strings and lists for GET (though GET usually only returns strings)
                        if isinstance(value, list):
                            # For now, if someone GETs a list, we'll just return the first element or a simple string
                            # Real Redis would return an error, but let's keep it simple.
                            val_to_send = value[0]
                        else:
                            val_to_send = value
                            
                        response = b"$" + str(len(val_to_send)).encode() + b"\r\n" + val_to_send + b"\r\n"
                        client_connection.send(response)
                else:
                    client_connection.send(b"$-1\r\n")

            elif command == b"RPUSH":
                # RPUSH key value1 [value2 ...]
                key = parts[4]
                # All values start at index 6 and appear at every other index (6, 8, 10...)
                # We slice from 6 to the end, jumping by 2
                new_values = parts[6:-1:2] 
                
                if key not in data_store:
                    data_store[key] = ([], None)
                
                data_list, expiry = data_store[key]
                if not isinstance(data_list, list):
                    # Overwrite if it wasn't a list
                    data_list = []
                    data_store[key] = (data_list, None)
                
                for v in new_values:
                    data_list.append(v)
                
                # Return the new length as a RESP Integer
                response = f":{len(data_list)}\r\n".encode()
                client_connection.send(response)

            elif command == b"LPUSH":
                # LPUSH key value1 [value2 ...]
                key = parts[4]
                new_values = parts[6:-1:2]
                
                if key not in data_store:
                    data_store[key] = ([], None)
                
                data_list, expiry = data_store[key]
                if not isinstance(data_list, list):
                    data_list = []
                    data_store[key] = (data_list, None)
                
                # Prepend each value one by one to the start (index 0)
                for v in new_values:
                    data_list.insert(0, v)
                
                response = f":{len(data_list)}\r\n".encode()
                client_connection.send(response)

            elif command == b"LRANGE":
                # LRANGE key start stop
                key = parts[4]
                start = int(parts[6])
                stop = int(parts[8])
                
                entry = data_store.get(key)
                if not entry or not isinstance(entry[0], list):
                    client_connection.send(b"*0\r\n")
                    continue
                
                data_list = entry[0]
                length = len(data_list)
                
                # Normalize indices (handle negative and out of bounds)
                def normalize(idx, length):
                    if idx < 0: idx = length + idx
                    if idx < 0: idx = 0
                    if idx >= length: idx = length - 1
                    return idx

                s_idx = normalize(start, length)
                e_idx = normalize(stop, length)
                
                # Check for empty range cases
                if start >= length or s_idx > e_idx:
                    sub_list = []
                else:
                    # e_idx + 1 because Python slicing is exclusive on the end
                    sub_list = data_list[s_idx : e_idx + 1]
                
                # Build RESP Array: *<count>\r\n + each element as Bulk String
                response = f"*{len(sub_list)}\r\n".encode()
                for item in sub_list:
                    response += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
                
                client_connection.send(response)
                    
            elif command == b"PING":
                client_connection.send(b"+PONG\r\n")
                
        except (ConnectionResetError, IndexError, ValueError):
            break
    client_connection.close()

def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
    while True:
        client_connection, client_address = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(client_connection,))
        client_thread.start()

if __name__ == "__main__":
    main()
