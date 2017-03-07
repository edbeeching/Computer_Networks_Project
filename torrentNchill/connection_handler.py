from threading import Thread
import socket
import queue
import logging

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
            logging.info('CON HANDLER: Looking at connection handler queue')
            message = self.in_queue.get()

            if message['msg'] == 'CRTCON':
                ip = message['ip']
                port = int(message['port'])
                logging.info('CON HANDLER: %s %s %s %s', 'Connection to ip:', ip, 'port', port)

                clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # Set a short timeout to ensure we poll through the IPs quickly
                clientsocket.settimeout(1)
                try:
                    clientsocket.connect((ip, port))
                    (ip, _) = clientsocket.getsockname()
                    (ip2, _) = clientsocket.getpeername()
                    logging.info('CON HANDLER: ip1 %s %s %s', ip, 'ip2', ip2)
                    if ip == ip2:
                        logging.warning('CON HANDLER:', 'CON HANDLER trying to connect to self')
                        continue
                    message = {'msg': 'NEWCON', 'sock': clientsocket}
                    # Set the timeout to blocking for recieving data
                    clientsocket.settimeout(None)
                    self.out_queue.put(message)

                except socket.error as er:
                    logging.warning('CON HANDLER: %s', er)

                finally:
                    logging.info('CON HANDLER: Exception connecting to %s %i', ip, port)

            elif message['msg'] == 'KILL':
                break
            else:
                logging.info('CON HANDLER: Message is not understood')

    def _connection_listener(self, out_queue):
        logging.info('CON HANDLER: Starting listener')
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind(('0.0.0.0', 10001))
            server_socket.listen(5)

            while True:
                logging.info('CON HANDLER: Trying to accept')
                (clientsocket, addr) = server_socket.accept()
                logging.info('Client connected at %s', addr)
                # Check socket send and recv addresses are not the same
                (ip, _) = clientsocket.getsockname()
                (ip2, _) = clientsocket.getpeername()
                logging.info('CON HANDLER: ip1 %s %s %s', ip, 'ip2', ip2)
                if ip == ip2:
                    logging.info('CON HANDLER: CON HANDLER trying to connect to self')
                    continue
                message = {'msg': 'NEWCON', 'sock': clientsocket}

                out_queue.put(message)
        except socket.error as er:
            logging.warning('CON HANDLER:', er)
        finally:
            logging.info('CON HANDLER: Exception in connection listener')





