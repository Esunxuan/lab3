import socket
import threading
import time

tuple_space = {}
operation_count = 0
put_count = 0
read_count = 0
get_count = 0
error_count = 0
client_count = 0

def handle_client(client_socket):
    global operation_count, put_count, read_count, get_count, error_count
    try:
        while True:
            data = client_socket.recv(1024).decode('utf-8')
            if not data:
                break
            operation_count += 1
            parts = data.split()
            if len(parts) < 3:
                error_count += 1
                response = f"{parts[0]} ERR Invalid request"
                client_socket.send(response.encode('utf-8'))
                continue
            command = parts[1]
            key = parts[2]
            if command == 'R':
                read_count += 1
                if key in tuple_space:
                    value = tuple_space[key]
                    response = f"{parts[0]} OK ({key}, {value}) read"
                else:
                    response = f"{parts[0]} ERR No such tuple"
            elif command == 'G':
                get_count += 1
                if key in tuple_space:
                    value = tuple_space.pop(key)
                    response = f"{parts[0]} OK ({key}, {value}) get"
                else:
                    response = f"{parts[0]} ERR No such tuple"
            elif command == 'P':
                put_count += 1
                if len(parts) < 4:
                    error_count += 1
                    response = f"{parts[0]} ERR Invalid PUT request"
                else:
                    value = parts[3]
                    tuple_space[key] = value
                    response = f"{parts[0]} OK ({key}, {value}) put"
            else:
                error_count += 1
                response = f"{parts[0]} ERR Invalid command"
            client_socket.send(response.encode('utf-8'))
    except Exception as e:
        error_count += 1
        print(f"Error handling client: {e}")
    finally:
        client_socket.close()

def print_stats():
    global tuple_space, operation_count, put_count, read_count, get_count, error_count, client_count
