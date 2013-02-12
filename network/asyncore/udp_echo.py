import asyncore
import socket

UDP_PORT = 9000

class UDPEchoServer(asyncore.dispatcher):
    def __init__(self, addr, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.bind((addr, port))

    def handle_connect(self):
        print "Server Started..." 

    def handle_read(self):
        data, addr = self.recvfrom(1024)
        print 'Incoming connection from %s' % repr(addr)
        print 'Received data: %s' % repr(data)
        self.sendto("OK", addr)

server = UDPEchoServer('', UDP_PORT)
asyncore.loop()