from threading import Thread
import queue
import string
import socket
import connection
import argparse
import member

"""
 Just for testing ideas of the interaction between a Member and its connetions
 Status: Incomplete

"""


class TestMember:
    def __init__(self, port, orch_filename):
        self._queue = queue.Queue()
        self._port = port
        self._dictionary = member.Member._get_orch_parameters(orch_filename)
        self._queue = queue.Queue()
        self._connections = {}
        self._file_parts = {}

    def connect(self, ip, port):
        sock = socket.create_connection((ip, port))
        send_queue = queue.Queue()
        conn = connection.Connection(sock, self._dictionary, send_queue, self._queue)
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
                send_queue = queue.Queue()
                conn = connection.Connection(sock, self._dictionary, send_queue, self._queue)
                conn.start()
                self._connections[ip + ':' + str(port)] = conn

        Thread(target=handle_connection).start()

    def read_whole_file(self):


    def process_queue(self):
        None

    def run_forever(self):
        r = Thread(target=self.process_queue)
        r.start()

if __name__ == "__main__":
    # Creates a number of messages to send to the member
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", help="Listen to an incomming connection",
                        action="store_true")
    parser.add_argument("--connect", help="Connect to another extreme",
                        action="store_true")
    parser.add_argument('n', nargs=1)
    args = parser.parse_args()

    orch_filename = 'maxresdefault.jpg.orch'


    if args.listen:
        print("Listening...")
        M = TestMember(int(args.n[0]), orch_filename)
        M.listen()

    if args.connect:
        print("Connecting...")
        M = TestMember(0, orch_filename)

        # Commands to send







        adr = args.n[0].split(':')
        ip = adr[0]
        port = int(adr[1])
        M.connect(ip, port)
        # Some commands to send to the other extreme
        cmd = {'msg': 'DOWN', 'filename': 'maxresults.jpg', 'checksum': '2953289a34e0cc2bf776decc3f8b86622d66b705'}
        M.add_command_to_send_queue(ip, port, cmd)



