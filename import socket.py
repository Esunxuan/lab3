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
    while True:
        time.sleep(10)
        tuple_count = len(tuple_space)
        total_tuple_size = sum(len(key) + len(value) for key, value in tuple_space.items())
        if tuple_count > 0:
            avg_tuple_size = total_tuple_size / tuple_count
        else:
            avg_tuple_size = 0
        total_key_size = sum(len(key) for key in tuple_space.keys())
        if tuple_count > 0:
            avg_key_size = total_key_size / tuple_count
        else:
            avg_key_size = 0
        total_value_size = sum(len(value) for value in tuple_space.values())
        if tuple_count > 0:
            avg_value_size = total_value_size / tuple_count
        else:
            avg_value_size = 0
        print(f"Tuple count: {tuple_count}")
        print(f"Average tuple size: {avg_tuple_size}")
        print(f"Average key size: {avg_key_size}")
        print(f"Average value size: {avg_value_size}")
        print(f"Connected clients: {client_count}")
        print(f"Total operations: {operation_count}")
        print(f"PUT operations: {put_count}")
        print(f"READ operations: {read_count}")
        print(f"GET operations: {get_count}")
        print(f"Error count: {error_count}")

def start_server(port):
    global client_count
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', port))
    server_socket.listen(5)
    print(f"Server listening on port {port}")
    stats_thread = threading.Thread(target=print_stats)
    stats_thread.daemon = True
    stats_thread.start()
    while True:
        client_socket, addr = server_socket.accept()
        client_count += 1
        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.start()