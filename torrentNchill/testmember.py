from threading import Thread
import queue
import string
import socket
import connection
import argparse

"""
 Just for testing ideas of the interaction between a Member and its connetions
 Status: Incomplete

"""


class TestMember:
    _connections = {}

    def __init__(self, port):
        self._queue = queue.Queue()
        self._port = port

    def connect(self, ip, port):
        sock = socket.create_connection((ip, port))
        conn = connection.Connection(sock, self)
        self._connections[ip + ':' + str(port)] = conn
        conn.start()

    def listen(self):
        # Bind, Listen for a connection
        mbr_socket = socket.socket()
        mbr_socket.bind(('localhost', self._port))
        mbr_socket.listen()

        def handle_connection():
            # Create the Connection
            # Add to the list of connections
            while True:
                sock, address = mbr_socket.accept()
                ip, port = sock.getpeername()

                # Verify address and port in the dictionary
                conn = connection.Connection(sock, self)
                conn.start()
                self._connections[ip + ':' + str(port)] = conn

        Thread(target=handle_connection).start()

    # Add a command to the processing queue
    def add_command(self, element):
        self._queue.put(element)

    # Add a command for sending
    def add_command_to_send_queue(self, ip, port, cmd):
        self._connections[ip + ':' + str(port)].put_message(cmd)

    def get_num_of_bytes(self, filename, part):
        return 10

if __name__ == "__main__":
    # Creates a number of messages to send to the member
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", help="Listen to an incomming connection",
                        action="store_true")
    parser.add_argument("--connect", help="Connect to another extreme",
                        action="store_true")
    parser.add_argument('n', nargs=1)
    args = parser.parse_args()
    if args.listen:
        print("Listening...")

        M = TestMember(int(args.n[0]))
        M.listen()

    if args.connect:
        print("Connecting...")
        M = TestMember(0)
        adr = args.n[0].split(':')
        ip = adr[0]
        port = int(adr[1])
        M.connect(ip, port)
        # Some commands to send to the other extreme
        cmd = {'msg': 'DOWN', 'filename': 'maxresults.jpg', 'checksum': '2953289a34e0cc2bf776decc3f8b86622d66b705'}
        M.add_command_to_send_queue(ip, port, cmd)



