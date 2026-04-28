import socket
import threading

def handle_client(client_connection):
    while True:
        try:
            data = client_connection.recv(1024)
            if not data:
                break
            client_connection.send(b"+PONG\r\n")
        except ConnectionResetError:
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
