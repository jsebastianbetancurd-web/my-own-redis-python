import socket
import threading

# In-memory database
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
                # SET key value
                key = parts[4]
                value = parts[6]
                data_store[key] = value
                client_connection.send(b"+OK\r\n")
            
            elif command == b"GET":
                # GET key
                key = parts[4]
                value = data_store.get(key)
                if value:
                    response = b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
                    client_connection.send(response)
                else:
                    client_connection.send(b"$-1\r\n") # Null Bulk String
                    
            elif command == b"PING":
                client_connection.send(b"+PONG\r\n")
                
        except (ConnectionResetError, IndexError):
            break
    client_connection.close()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
    while True:
        client_connection, client_address = server_socket.accept() # wait for client
        # Create a new thread for each connection
        client_thread = threading.Thread(target=handle_client, args=(client_connection,))
        client_thread.start()

if __name__ == "__main__":
    main()
