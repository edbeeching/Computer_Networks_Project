import socket
import threading

"""
Created by Arslen REMACI

Conductor file, send IPs to the member connecting to him

IPs saved in a file named "IPs.txt" (may change later)

"""


class MemberThread(threading.Thread):
    def __init__(self, ip, port, membersocket):
        threading.Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.membersocket = membersocket
        print("+++ New thread for %s on %s" % (self.ip, self.port,))

    def run(self):
        print("%s %s is connecting" % (self.ip, self.port,))

        print("=== Sending IPs ===")
        fp = open("data/IPs.txt", 'rb')
        self.membersocket.sendall(fp.read())

        print("=== Finished sending, member disconnected ===")
        self.membersocket.shutdown(socket.SHUT_RDWR)
        self.membersocket.close()


tcpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcpsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
tcpsock.bind(('0.0.0.0', 9999))

while True:
    tcpsock.listen(10)
    print("Listening")
    (membersocket, (ip, port)) = tcpsock.accept()
    newthread = MemberThread(ip, port, membersocket)
    newthread.start()