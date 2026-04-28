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
                # RPUSH key value
                key = parts[4]
                value = parts[6]
                
                if key not in data_store:
                    data_store[key] = ([value], None)
                else:
                    data_list, expiry = data_store[key]
                    if isinstance(data_list, list):
                        data_list.append(value)
                    else:
                        # Overwrite if it wasn't a list
                        data_store[key] = ([value], None)
                
                list_len = len(data_store[key][0])
                # Return RESP Integer: :<length>\r\n
                response = f":{list_len}\r\n".encode()
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
