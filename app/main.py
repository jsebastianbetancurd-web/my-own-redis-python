import socket
import threading

def handle_client(client_connection):
    while True:
        try:
            data = client_connection.recv(1024)
            if not data:
                break
            
            # Basic RESP Parser for ECHO
            # Example incoming: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
            parts = data.split(b"\r\n")
            
            if len(parts) > 4 and b"ECHO" in parts[2].upper():
                # Extract the message (e.g., b"hey")
                message = parts[4]
                # Format as Bulk String: $<length>\r\n<message>\r\n
                response = b"$" + str(len(message)).encode() + b"\r\n" + message + b"\r\n"
                client_connection.send(response)
            else:
                # Default back to PONG for simple PING commands
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
