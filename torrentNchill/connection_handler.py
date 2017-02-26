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
            print('CON HANDLER:', 'Looking at connection handler queue')
            message = self.in_queue.get()

            if message['msg'] == 'CRTCON':
                ip = message['ip']
                port = int(message['port'])
                print('CON HANDLER:', 'Connection to ip:', ip, 'port', port)

                clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # Set a short timeout to ensure we poll through the IPs quickly
                clientsocket.settimeout(1)
                try:
                    clientsocket.connect((ip, port))
                    (ip, _) = clientsocket.getsockname()
                    (ip2, _) = clientsocket.getpeername()
                    print('CON HANDLER:', 'ip1', ip, 'ip2', ip2)
                    if ip == ip2:
                        print('CON HANDLER:', 'CON HANDLER trying to connect to self')
                        continue
                    message = {'msg': 'NEWCON', 'sock': clientsocket}
                    # Set the timeout to blocking for recieving data
                    clientsocket.settimeout(None)
                    self.out_queue.put(message)

                except socket.error as er:
                    print('CON HANDLER:', er)

                finally:
                    print('CON HANDLER:', "Exception connecting to", ip, port)

            elif message['msg'] == 'KILL':
                break
            else:
                print('CON HANDLER:', "Message is not understood")

    def _connection_listener(self, out_queue):
        print('CON HANDLER:', 'Starting listener')
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind(('0.0.0.0', 10001))
            server_socket.listen(5)

            while True:
                print('CON HANDLER:', 'Trying to accept')
                (clientsocket, addr) = server_socket.accept()
                print('Client connected at', addr)
                # Check socket send and recv addresses are not the same
                (ip, _) = clientsocket.getsockname()
                (ip2, _) = clientsocket.getpeername()
                print('CON HANDLER:', 'ip1', ip, 'ip2', ip2)
                if ip == ip2:
                    print('CON HANDLER:', 'CON HANDLER trying to connect to self')
                    continue
                message = {'msg': 'NEWCON', 'sock': clientsocket}

                out_queue.put(message)
        except socket.error as er:
            print('CON HANDLER:', er)
        finally:
            print('CON HANDLER:', "Exception in connection listener")





