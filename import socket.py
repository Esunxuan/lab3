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