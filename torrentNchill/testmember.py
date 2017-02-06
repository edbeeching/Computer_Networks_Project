from threading import Thread
import queue
import socket
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
        None

    def listen(self):
        # Bind, Listen for a connection
        mbr_socket = socket.socket()
        mbr_socket.bind(('localhost', self._port))
        mbr_socket.listen()

        def handle_connection():
            # Create the Connection
            # Add to the list of connections
            while True:
                socket, address = mbr_socket.accept()
                # Verify address and port in the dictionary
                connection = Connection(socket, self)
                connection.start()
                self._connections['address'] = connection

        Thread(target=handle_connection).start()

    def put(self, element):
        self._queue.put(element)
