from threading import Thread
import socket
import threading
import queue
import logging

"""
    Created by Jorge Chong 14/02/2017
    Class Connection, manage the Receiver and Sending threads over a socket
    and interact with the Member that created the corresponding connection

    Status: Incomplete, Send and Receive Threads
"""


class Connection:
    # Can send or receive?
    # This flag is set to false after an socket i/o error
    # socket: a reference to the socket created before the connection is instantiated
    # member: a reference to the member
    def __init__(self, sock, dictionary, send_queue, member_queue):
        # Initialize Connection logging
        logging.basicConfig(format='%(asctime)s %(message)s', filename='logs/connections.log', filemode='w',
                            level=logging.DEBUG)
        # Socket object to send and receive in a connection
        self._socket = sock
        # Member dictionary referring to the file metadata (parts, checksum, etc)
        self._dictionary = dictionary
        # Queue of the Member in which the commands are inserted
        self._member_queue = member_queue
        # This flag is set to false after a socket i/o error
        self._ready = True
        # This flage tells the protocol state machine that it has read a command (4 letters)
        # and proceed to read the rest of a message
        self._cmd_read = False
        # Default buffer size of a receive thread
        self._recv_buffer_size = 1024
        # Ip and Port of the other extreme (for future reference and messages)
        self._ip, self._port = self._socket.getpeername()
        # Queue for sending messages through a socket
        self._send_queue = send_queue
        self._last_cmd = ''

    # Read from the queue, build the message according to protocol.md
    # and send it through socket
    def send(self):
        # Always waiting for something to send
        while self._ready:
            # Wait in the queue for something to send
            logging.info("CONN ST: Waiting for something to send...")
            # print("CONN ST: Waiting for something to send...")
            cmd = self._send_queue.get()
            # print("CONN ST: Sending Command: {}".format(cmd['msg']))
            logging.info("CONN ST: Sending Command: {}".format(cmd['msg']))
            # print("CONN ST: cmd from member: {}".format(str(cmd)))
            logging.info("CONN ST: cmd from member: {}".format(str(cmd)))

            # Request List of Parts: DOWN \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n
            if cmd['msg'] == 'REQUEST_PARTS_LIST':
                self._protocol_msg_DOWN()
            # Send List of parts: SEND \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n
            elif cmd['msg'] == 'SEND_PARTS_LIST':
                self._protocol_msg_SEND(str(cmd['parts_list']))
            # Request a Part: PART \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n
            elif cmd['msg'] == 'REQUEST_PART':
                self._protocol_msg_PART(str(cmd['part']))
            # Send the actual data of a part: STRT \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n <DATA>
            elif cmd['msg'] == 'SEND_PART':
                self._protocol_msg_STRT(str(cmd['part']), cmd['data'])
            # File not found: NONE \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n 0 \r\n
            elif cmd['msg'] == 'FILE_NOT_FOUND':
                self._protocol_msg_NONE('0')
            # Invalid Checksum: NONE \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n 0 \r\n
            elif cmd['msg'] == 'INVALID_CHECKSUM':
                self._protocol_msg_NONE('0')
            # Part not Found: NONE \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n
            elif cmd['msg'] == 'PART_NOT_FOUND':
                self._protocol_msg_NONE(str(cmd['part']))
            elif cmd['msg'] == 'CLOSE':
                try:
                    self._socket.shutdown(socket.SHUT_RDWR)
                    self._socket.close()
                except socket.error as er:
                    logging.warning('Exception when closing socket %s', er)
                    try:
                        self._socket.close()
                    except socket.error as er:
                        logging.warning('Exception when closing socket %s', er)

            else:
                # print('--- Unrecognized Command')
                logging.warning('--- Unrecognized Command')

    # Read from a socket and interpret the protocol
    # See protocol.md
    # The protocol is text based and line terminated in \r\n
    def receive(self):
        self._last_cmd = ''
        # While there is no error in the socket communication
        while self._ready:
            # print("Receiving Thread...")
            # Reading:
            # Phase 0: Read a Command Line (4 Letters followed by a \r\n)
            if not self._cmd_read:
                self._protocol_read_command()
            else:
                # 2. Read the rest of the request
                # Cmd = DOWN, read 2 lines
                # DOWN \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n

                # Cmd = SEND, read 3 lines
                # SEND \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n

                # Cmd = NONE, read 3 lines
                # Can be a reply to a DOWN command:
                # NONE \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n 0 \r\n
                # Or a reply to a STRT command:
                # NONE \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n

                # Cmd = PART, read 3 lines
                # PART \r\n < FILENAME > \r\n < FULL_FILE_CHECKSUM > \r\n < INTEGER > \r\n

                # Cmd = STRT, read 3 lines + data
                # STRT \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n <DATA>
                filename = ''
                checksum = ''
                err_msg = ''
                error = False
                try:
                    # print('CONN RT: Last command received:{}, IP:PORT = {}:{}'.format(self._last_cmd, self._ip, str(self._port)))
                    logging.info('CONN RT: Last command received:{}, IP:PORT = {}:{}'.format(self._last_cmd, self._ip, str(self._port)))
                    filename = Connection._read_line(self._socket)
                    checksum = Connection._read_line(self._socket)
                    word_3rd = b""
                    # In these commands, there is an additional parameter to read
                    if self._last_cmd in ['NONE', 'SEND', 'PART', 'STRT']:
                        word_3rd = Connection._read_line(self._socket)
                except socket.error as socket_error_msg:
                    err_msg = socket_error_msg
                    error = True
                except:
                    err_msg = 'Unexpected Error'
                    error = True
                finally:
                    if error:
                        cmd = {'msg': 'SOCKET_ERROR', 'conn': self, 'error': err_msg}
                        # Put the error in the Member's queue
                        self._member_queue.put(cmd)
                        # Set _ready to False to avoid reading over a faulty socket
                        self._ready = False
                        # Close the socket
                        self._socket.close()
                        # print('CONN RT: ---- Error receiving through socket: {}'.format(cmd))
                        logging.error('CONN RT: ---- Error receiving through socket: {}'.format(cmd))

                if not error:
                    # Command Interpretation:
                    # print('CONN RT: Processing command received: {}'.format(self._last_cmd))
                    logging.info('CONN RT: Processing command received: {}'.format(self._last_cmd))
                    if self._last_cmd == 'DOWN':
                        self._handle_receive_DOWN(filename, checksum)
                    elif self._last_cmd == 'SEND':
                        self._handle_receive_SEND(word_3rd)
                    elif self._last_cmd == 'NONE':
                        self._handle_receive_NONE(filename, checksum, word_3rd)
                    elif self._last_cmd == 'PART':
                        self._handle_receive_PART(word_3rd)
                    elif self._last_cmd == 'STRT':
                        self._handle_receive_STRT(word_3rd)
                    else:
                        # The command is supposed to be filtered before this section
                        pass

                    self._cmd_read = False

    # Read a line from socket
    # Taken from the practical sessions
    @staticmethod
    def _read_line(sock):
        sock.setblocking(1)
        res = b""
        was_r = False
        while True:
            b = sock.recv(1)
            if len(b) == 0:
                return None
            if b == b"\n" and was_r:
                break
            if was_r:
                res += b"\r"
            if b == b"\r":
                was_r = True
            else:
                was_r = False
                res += b
        return res.decode("utf-8")

    # Read n bytes from socket
    @staticmethod
    def _read_n_bytes(sock, n_bytes, buffer_size):
        # print('CONN RT: Reading {} bytes: '.format(str(n_bytes)))
        logging.info('CONN RT: Reading {} bytes: '.format(str(n_bytes)))
        sock.setblocking(1)
        bytes_read = bytearray()
        to_read = n_bytes
        data = bytearray()
        while to_read > 0:
            if to_read <= buffer_size:
                data = sock.recv(to_read)
            else:
                data = sock.recv(buffer_size)

            to_read -= len(data)
            bytes_read.extend(data)
        # print('CONN RT: Total bytes read: {}'.format(str(len(bytes_read))))
        logging.info('CONN RT: Total bytes read: {}'.format(str(len(bytes_read))))
        return bytes_read

    # Protocol Management functions
    def _protocol_read_command(self):
        error = False
        err_msg = ''
        try:
            # Read the first Line of any stream
            # if the stream is well formed according to the protocol
            # everything is going to be fine
            self._last_cmd = Connection._read_line(self._socket)
        except socket.error as socket_error_msg:
            err_msg = socket_error_msg
            error = True
        except:
            err_msg = 'Unexpected Error'
            error = True
        finally:
            if error:
                cmd = {'msg': 'SOCKET_ERROR', 'conn': self, 'error': err_msg}
                # Put the error in the Member's queue
                self._member_queue.put(cmd)
                # Set _ready to False to avoid reading over a faulty socket
                self._ready = False
                # Close the socket and with self._ready = False ends the thread
                self._socket.close()
                # print('---- Error receiving through socket: {}'.format(cmd))
                logging.error('---- Error receiving through socket: {}'.format(cmd))

        # 1. Interpret command, if no errors in the socket connection:
        if not error:
            if self._last_cmd in ['DOWN', 'NONE', 'SEND', 'STRT', 'PART']:
                self._cmd_read = True
            else:
                # Error in the message received, notify to
                # the member to take action
                self._member_queue.put({'msg': 'CMD_ERROR',
                                        'conn': self,
                                        'error': 'Bad Command Received',
                                        'cmd': self._last_cmd
                                        })

    def _protocol_read_request(self):
        pass

    def _handle_receive_DOWN(self, filename, checksum):
        # DOWN: Validate <FILENAME> and <FULL_FILE_CHECKSUM>
        # If there is an error, answer directly inserting a response in
        # the sender's queue notifies to the member
        if filename != self._dictionary['composition_name']:
            self._send_queue.put({'msg': 'FILE_NOT_FOUND'})
            self._member_queue.put({'msg': 'BAD_FILE_REQUEST'})
        elif checksum != self._dictionary['full_checksum']:
            self._send_queue.put({'msg': 'INVALID_CHECKSUM'})
            self._member_queue.put({'msg': 'BAD_FILE_REQUEST'})
        else:
            self._member_queue.put({'msg': 'PARTS_LIST_REQUEST',
                                    'conn': self})
        # print(str(self._member_queue))

    def _handle_receive_SEND(self, parts):
        self._member_queue.put({'msg': 'RECEIVED_PARTS_LIST',
                                'conn': self,
                                'parts_list': int(parts)})

    def _handle_receive_NONE(self, filename, checksum, part_number):
        # Put a BAD_FILE_REQUEST into the members queue to notify that the other extreme
        # is not sharing a file or part list
        self._member_queue.put({'msg': 'BAD_FILE_REQUEST',
                                'conn': self,
                                'filename': filename,
                                'checksum': checksum,
                                'part': int(part_number)
                                })

    def _handle_receive_PART(self, part_number):
        # See the type of the word_3rd for debugging only
        # print('** Type of the part encoded {}'.format(type(part_number)))
        self._member_queue.put({'msg': 'PART_REQUEST',
                                'conn': self,
                                'part': int(part_number)
                                })

    def _handle_receive_STRT(self, part_number):
        n_bytes = b""
        # To read the binary data, we need to know how many bytes to read from the
        # socket therefore, we have to query the member about the number of bytes
        # of a specific part
        part_number = int(part_number)
        num_of_parts = self._dictionary['num_parts']
        bytes_per_part = self._dictionary['bytes_per_part']
        total_bytes = self._dictionary['total_bytes']
        num_of_bytes = 0
        if part_number < num_of_parts:
            num_of_bytes = bytes_per_part
        else:
            num_of_bytes = total_bytes - (num_of_parts - 1) * bytes_per_part

        # print("CONN RT: Receiving ...{} bytes".format(str(num_of_bytes)))
        logging.info("CONN RT: Receiving ...{} bytes".format(str(num_of_bytes)))

        err_msg = ''
        error = False
        try:
            n_bytes = Connection._read_n_bytes(self._socket, num_of_bytes,
                                               self._recv_buffer_size)
        except socket.error as socket_error_msg:
            err_msg = socket_error_msg
            error = True
        except:
            err_msg = 'Unexpected Error'
            error = True
        finally:
            if error:
                cmd = {'msg': 'SOCKET_ERROR', 'conn': self, 'error': err_msg}
                # Put the error in the Member's queue
                self._member_queue.put(cmd)
                # Set _ready to False to avoid reading over a faulty socket
                self._ready = False
                # Close the socket
                self._socket.close()
                # print('---- Error receiving through socket: {}'.format(cmd))
                logging.error('---- Error receiving through socket: {}'.format(cmd))

        if err_msg == '':
            # Insert Part readed in the Member's queue
            self._member_queue.put({'msg': 'RECEIVED_PART',
                                    'conn': self,
                                    'part': part_number,
                                    'data': n_bytes
                                    })

    def _protocol_send_text(self, text_message):
        send_buffer = bytearray()
        send_buffer.extend(text_message.encode('ascii'))
        # If there is an error in the socket (the other end disconnects, etc)
        # shutdown the socket an inform the Member, and set self._ready to False
        error = False
        err_msg = ''
        try:
            # print('CONN ST: Text Buffer content: {}'.format(send_buffer.decode('utf-8')))
            logging.info('CONN ST: Text Buffer content: {}'.format(send_buffer.decode('utf-8')))
            self._socket.sendall(send_buffer)
        except socket.error as socket_error_msg:
            err_msg = socket_error_msg
            error = True
        except:
            err_msg = 'Unexpected Error'
            error = True
        finally:
            if error:
                cmd = {'msg': 'SOCKET_ERROR', 'conn': self, 'error': err_msg}
                # Put the error in the Member's queue
                # print('CONN ST: ---- Error sending text through socket: {}'.format(cmd))
                logging.error('CONN ST: ---- Error sending text through socket: {}'.format(cmd))
                self._member_queue.put(cmd)
                # Set _ready to False to avoid reading over a faulty socket
                self._ready = False
                # Close the socket
                self._socket.close()

    def _protocol_send_bytes(self, data):
        # data is a byte array
        # print type to verify
        bytes = len(data)
        # print('CONN ST: Send Bytes, type of data to send: {}, bytes: {}'.format(type(data), str(bytes)))
        logging.info('CONN ST: Send Bytes, type of data to send: {}, bytes: {}'.format(type(data), str(bytes)))
        error = False
        err_msg = ''
        try:
            self._socket.sendall(data)
        except socket.error as socket_error_msg:
            err_msg = socket_error_msg
            error = True
        except:
            err_msg = 'Unexpected Error'
            error = True
        finally:
            if error:
                cmd = {'msg': 'SOCKET_ERROR', 'conn': self, 'error': err_msg}
                # Put the error in the Member's queue
                # print('CONN ST: ---- Error sending bytes through socket: {}'.format(cmd))
                logging.error('CONN ST: ---- Error sending bytes through socket: {}'.format(cmd))
                self._member_queue.put(cmd)
                # Set _ready to False to avoid reading over a faulty socket
                self._ready = False
                # Close the socket
                self._socket.close()



    def _protocol_msg_DOWN(self):
        msg = '{}\r\n{}\r\n{}\r\n'.format('DOWN',
                                          self._dictionary['composition_name'],
                                          self._dictionary['full_checksum'])
        # print('CONN ST: Sending message through socket: {}'.format(msg))
        logging.info('CONN ST: Sending message through socket: {}'.format(msg))
        self._protocol_send_text(msg)

    def _protocol_msg_SEND(self, parts_list):
        msg = '{}\r\n{}\r\n{}\r\n{}\r\n'.format('SEND',
                                                self._dictionary['composition_name'],
                                                self._dictionary['full_checksum'],
                                                parts_list)
        # print('CONN ST: Sending message through socket: {}'.format(msg))
        logging.info('CONN ST: Sending message through socket: {}'.format(msg))
        self._protocol_send_text(msg)

    def _protocol_msg_PART(self, part_number):
        msg = '{}\r\n{}\r\n{}\r\n{}\r\n'.format('PART',
                                                self._dictionary['composition_name'],
                                                self._dictionary['full_checksum'],
                                                part_number)
        # print('CONN ST: Sending message through socket: {}'.format(msg))
        logging.info('CONN ST: Sending message through socket: {}'.format(msg))
        self._protocol_send_text(msg)

    def _protocol_msg_NONE(self, part_number):
        msg = '{}\r\n{}\r\n{}\r\n{}\r\n'.format('NONE',
                                                self._dictionary['composition_name'],
                                                self._dictionary['full_checksum'],
                                                part_number)
        # print('CONN ST: Sending message through socket: {}'.format(msg))
        logging.info('CONN ST: Sending message through socket: {}'.format(msg))
        self._protocol_send_text(msg)

    def _protocol_msg_STRT(self, part_number, data):
        msg = '{}\r\n{}\r\n{}\r\n{}\r\n'.format('STRT', self._dictionary['composition_name'],
                                                self._dictionary['full_checksum'],
                                                part_number)
        # print('CONN ST: Sending message through socket: {}'.format(msg))
        logging.info('CONN ST: Sending message through socket: {}'.format(msg))
        self._protocol_send_text(msg)

        # print('CONN ST: Bytes to send...')
        # print('Type of data: {}'.format(type(data)))
        self._protocol_send_bytes(data)

    def start(self):
        rt = Thread(target=self.receive)
        st = Thread(target=self.send)
        rt.start()
        st.start()
