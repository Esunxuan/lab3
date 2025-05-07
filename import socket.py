import socket
import threading
import time

# 全局变量
tuple_space = {}
operation_count = 0
put_count = 0
read_count = 0
get_count = 0
error_count = 0
client_count = 0