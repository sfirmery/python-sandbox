import socket

MCAST_GRP = "239.18.0.10"
UDP_PORT = 9000

# create udp socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# define multicast params
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

try:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
except AttributeError:
    pass # Some systems don't support SO_REUSEPORT
sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_TTL, 1)
sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_LOOP, 1)

# bind
sock.bind(('', UDP_PORT))

# Set some more multicast options
sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF, socket.inet_aton("0.0.0.0"))
sock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(MCAST_GRP) + socket.inet_aton("0.0.0.0"))

while 1:
    data, addr = sock.recvfrom(1024)
    print "received message:", data, "from:", addr

    sock.sendto("Re:" + data, addr)