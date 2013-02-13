import socket
import time

TCP_PORT = 9000

# create udp socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# bind
sock.bind(('', TCP_PORT))

sock.listen(1)

while 1:
    pair = sock.accept()
    if pair is not None:
        conn, addr = pair
        print 'Incoming connection from %s' % repr(addr)
        while 1:
            data = conn.recv(8192)
            if data:
                print "received message:", repr(data), "from:", repr(addr)
                conn.send("OK")
            else:
                break
        print "connection closed."
        conn.close()
