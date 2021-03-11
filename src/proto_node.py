import socket
import network_protocol as netp

orchestrator_ip = "127.0.0.1"
orchestrator_port = 5005

# node_variables

pool = []
parents = []
children = []

node_ip = None
node_port = None
node_uuid = None

node_sock = None

# message handlers

def handle_pool_addition(node_list):
    print("Received pool info")
    global pool
    pool += node_list

def handle_parent_addition(node_list):
    print("Received parent info")
    global parents
    parents += node_list

def handle_children_addition(node_list):
    print("Received children info")
    global children
    children += node_list

def handle_init_data(node_info):
    print("Received general init info")
    global node_ip, node_port, node_uuid, node_sock
    node_ip = node_info[1]
    node_port = node_info[2]
    node_uuid = node_info[0]

    node_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    node_sock.bind((node_ip, node_port))

def handle_message(msg):
    netp.process_message(msg, handle_add_children=handle_children_addition, handle_add_parents=handle_parent_addition, handle_add_pool=handle_pool_addition,
                         handle_init=handle_init_data)


def recv_message(sock="ORCH"):
    print("waiting for data")
    if sock == "ORCH":
        data, adr = orchestrator_sock.recvfrom(1024)
    elif sock == "DEFAULT":
         data, adr = node_sock.recvfrom(1024)

    msg = data.decode('UTF-8')
    handle_message(msg)

# startup process

# 1. Ask for initial information (my_UUID, my_ip, my_port)
orchestrator_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
orchestrator_sock.sendto(b"INIT", (orchestrator_ip, orchestrator_port))

# 2. receive parents, children, pool

# initial_info
recv_message()
# pool / children / parents
recv_message(sock="DEFAULT")
recv_message(sock="DEFAULT")
recv_message(sock="DEFAULT")

# main loop
while True:
    recv_message(sock="DEFAULT")
   

print("node done")

