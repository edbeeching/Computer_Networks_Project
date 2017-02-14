from threading import Thread
import socket
import queue


class ConnectionHandler(Thread):

    def __init__(self, in_queue, out_queue):
        # Initialise the thread
        Thread.__init__(self)
        self.in_queue = in_queue
        self.out_queue = out_queue

        self.listener = Thread(target=self._connection_listener, args=(self.out_queue,))

    def run(self):
        self.listener.start()
        while True:
            message = self.in_queue.get()

            if message['msg'] == 'CRTCON':
                ip = message['ip']
                port = int(message['port'])
                print('Connection to ip:', ip, 'port', port)

                clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    clientsocket.connect((ip, port))
                    message = {'msg': 'NEWCON', 'sock': clientsocket}

                    self.out_queue.put(message)

                except:
                    print('exception')

                finally:
                    print("Exception connecting to", ip, port)

            elif message['msg'] == 'KILL':
                break
            else:
                print("Message is not understood")

    def _connection_listener(self, out_queue):
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind(('localhost', 10001))
            server_socket.listen(5)

            while True:
                (clientsocket, addr) = server_socket.accept()

                message = {'msg': 'NEWCON', 'sock': clientsocket}

                out_queue.put(message)

        finally:
            print("Exception in connection listener")





