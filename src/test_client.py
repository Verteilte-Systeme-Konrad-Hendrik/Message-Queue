import socket

UDP_IP = "127.0.0.1"
UDP_PORT = 5005
msg = b"Hello, World!"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock.sendto(msg, (UDP_IP, UDP_PORT))

