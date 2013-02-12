import socket
import time

MCAST_GRP = "239.18.0.10"
UDP_PORT = 9000
MESSAGE = "Multicast sandbox"

# create udp socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# define multicast params
sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# set timeout
sock.settimeout(.2)

# send data many times
while 1:
    # send data to multicast group
    sock.sendto(MESSAGE, (MCAST_GRP, UDP_PORT))

    print "send message:", MESSAGE, "to:", MCAST_GRP
    print "waiting reply..."

    # listen reply
    while 1:
        try:
            data, addr = sock.recvfrom(12)
        except socket.timeout:
            print 'timed out, no more responses'
            break
        else:
            print "received message:", data, "from:", addr

        time.sleep(1)