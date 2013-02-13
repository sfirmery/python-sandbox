import asyncore
import socket
import time

TCP_PORT = 9000

class EchoHandler(asyncore.dispatcher_with_send):

    def handle_write(self):
        print "handle_write()"

    def handle_read(self):
        data = self.recv(8192)
        if data:
            print "received message:", repr(data)
            self.send("OK")

    def handle_close(self):
        print "Connection closed."
        self.close()

class TCPEchoServer(asyncore.dispatcher):

    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(1024)

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock, addr = pair
            print 'Incoming connection from %s' % repr(addr)
            handler = EchoHandler(sock)

server = TCPEchoServer('', TCP_PORT)
asyncore.loop()