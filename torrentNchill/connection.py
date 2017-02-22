from threading import Thread
import socket
import threading
import queue

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

    # Read from the queue, build the message according to protocol.md
    # and send it through socket
    def send(self):
        # Always waiting for something to send
        while True:
            # If there was no error in the socket communication
            if not self._ready:
                break

            # Wait in the queue for something to send
            print("Sending Thread...")
            print("+ Waiting for something to send...")
            cmd = self._send_queue.get()
            print("++ Sending Command: {}", cmd['msg'])

            # Request List of Parts: DOWN \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n
            if cmd['msg'] == 'REQUEST_PARTS_LIST':
                msg = '{}\r\n{}\r\n{}\r\n'.format('DOWN', self._dictionary['composition_name'],
                                                  self._dictionary['full_checksum'])
                print('+++ Sending message through socket: {}'.format(msg))
            # Send List of parts: SEND \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n
            elif cmd['msg'] == 'SEND_PARTS_LIST':
                msg = '{}\r\n{}\r\n{}\r\n{}\r\n'.format('SEND', self._dictionary['composition_name'],
                                                        self._dictionary['full_checksum'], str(cmd['parts_list']))
                print('+++ Sending message through socket: {}'.format(msg))
            # Request a Part: PART \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n
            elif cmd['msg'] == 'REQUEST_PART':
                msg = '{}\r\n{}\r\n{}\r\n{}\r\n'.format('PART', self._dictionary['composition_name'],
                                                        self._dictionary['full_checksum'], str(cmd['part']))
                print('+++ Sending message through socket: {}'.format(msg))
            # Send the actual data of a part: STRT \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n <DATA>
            elif cmd['msg'] == 'SEND_PART':
                msg = '{}\r\n{}\r\n{}\r\n{}\r\n{}'.format('STRT', self._dictionary['composition_name'],
                                                        self._dictionary['full_checksum'], str(cmd['part']), str(cmd['data']))
                print('+++ Sending message through socket: {}'.format(msg))
            # File not found: NONE \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n 0 \r\n
            elif cmd['msg'] == 'FILE_NOT_FOUND':
                msg = '{}\r\n{}\r\n{}\r\n{}\r\n'.format('NONE', self._dictionary['composition_name'],
                                                        self._dictionary['full_checksum'], '0')
                print('+++ Sending message through socket: {}'.format(msg))
            # Invalid Checksum: NONE \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n 0 \r\n
            elif cmd['msg'] == 'INVALID_CHECKSUM':
                msg = '{}\r\n{}\r\n{}\r\n{}\r\n'.format('NONE', self._dictionary['composition_name'],
                                                        self._dictionary['full_checksum'], '0')
                print('+++ Sending message through socket: {}'.format(msg))
            # Part not Found: NONE \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n
            elif cmd['msg'] == 'PART_NOT_FOUND':
                msg = '{}\r\n{}\r\n{}\r\n{}\r\n'.format('NONE', self._dictionary['composition_name'],
                                                        self._dictionary['full_checksum'], str(cmd['part']))
                print('+++ Sending message through socket: {}'.format(msg))
            else:
                print('--- Unrecognized Command')

            send_buffer = bytearray()
            send_buffer.extend(msg.encode('ascii'))
            # If there is an error in the socket (the other end disconnects, etc)
            # shutdown the socket an inform the Member, and set self._ready to False
            err_msg = ''
            try:
                print('++++ Send buffer content: {}'.format(send_buffer.decode('utf-8')))
                self._socket.sendall(send_buffer)
            except socket.error as se:
                err_msg = se.strerror
            except:
                err_msg = 'Unexpected Error'
            finally:
                if err_msg != '':
                    cmd = {'msg': 'SOCKET_ERROR', 'conn': self, 'error': err_msg}
                    # Put the error in the Member's queue
                    print('---- Error sending through socket: {}'.format(cmd))
                    self._member_queue.put(cmd)
                    # Set _ready to False to avoid reading over a faulty socket
                    self._ready = False
                    # Close the socket
                    self._socket.close()

    # Read from a socket and interpret the protocol
    # See protocol.md
    # The protocol is text based and line terminated in \r\n
    def receive(self):
        last_cmd = ''
        while True:
            # If there was no error in the socket communication
            if not self._ready:
                break
            print("Receiving Thread...")
            # Reading:
            # Phase 0: Read a Command Line (4 Letters followed by a \r\n)
            if not self._cmd_read:
                err_msg = ''
                try:
                    last_cmd = Connection._read_line(self._socket)
                except socket.error as se:
                    err_msg = se.strerror
                except:
                    err_msg = 'Unexpected Error'
                finally:
                    if err_msg != '':
                        cmd = {'msg': 'SOCKET_ERROR', 'conn': self, 'error': err_msg}
                        # Put the error in the Member's queue
                        self._member_queue.put(cmd)
                        # Set _ready to False to avoid reading over a faulty socket
                        self._ready = False
                        # Close the socket and with self._ready = False ends the thread
                        self._socket.close()

                # 1. Interpret command, if no errors in the socket connection:
                if err_msg == '':
                    if last_cmd in ['DOWN', 'NONE', 'SEND', 'STRT', 'PART']:
                        self._cmd_read = True
                    else:
                        # Error in the message received, raise an exception and notify to
                        # the member to take action
                        self._member_queue.put({'msg': 'CMD_ERROR',
                                                'conn': self,
                                                'error': 'Bad Command Received',
                                                'cmd': last_cmd})
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
                try:
                    print('* Last command received:{}, IP:PORT = {}:{}'.format(last_cmd, self._ip, str(self._port)))
                    filename = Connection._read_line(self._socket)
                    checksum = Connection._read_line(self._socket)
                    word_3rd = b""
                    # In these commands, there is an additional parameter to read
                    if last_cmd in ['NONE', 'SEND', 'PART', 'STRT']:
                        word_3rd = Connection._read_line(self._socket)
                except socket.error as se:
                    err_msg = se.strerror
                except:
                    err_msg = 'Unexpected Error'
                finally:
                    if err_msg != '':
                        cmd = {'msg': 'SOCKET_ERROR', 'conn': self, 'error': err_msg}
                        # Put the error in the Member's queue
                        self._member_queue.put(cmd)
                        # Set _ready to False to avoid reading over a faulty socket
                        self._ready = False
                        # Close the socket
                        self._socket.close()

                if err_msg == '':
                    # Command Interpretation:
                    if last_cmd == 'DOWN':
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

                    elif last_cmd == 'SEND':
                        self._member_queue.put({'msg': 'RECEIVED_PARTS_LIST',
                                                'conn': self,
                                                'parts_list': int(word_3rd)})
                    elif last_cmd == 'NONE':
                        # Put a BAD_FILE_REQUEST into the members queue to notify that the other extreme
                        # is not sharing a file or part list
                        self._member_queue.put({'msg': 'BAD_FILE_REQUEST', 'conn': self, 'filename':})
                        pass
                    elif last_cmd == 'PART':
                        # See the type of the word_3rd
                        print('** Type of the part encoded {}'.format(type(word_3rd)))
                        self._member_queue.put({'msg': 'PART_REQUEST', 'conn': self, 'part': int(word_3rd)})
                    elif last_cmd == 'STRT':
                        n_bytes = b""
                        # To read the binary data, we need to know how many bytes to read from the
                        # socket therefore, we have to query the member about the number of bytes
                        # of a specific part
                        part_number = int(word_3rd)
                        num_of_parts = self._dictionary['num_parts']
                        bytes_per_part = self._dictionary['bytes_per_part']
                        total_bytes = self._dictionary['total_bytes']
                        if part_number < num_of_parts:
                            num_of_bytes = bytes_per_part
                        else:
                            num_of_bytes = total_bytes - num_of_parts * bytes_per_part

                        err_msg = ''
                        try:
                            n_bytes = Connection._read_n_bytes(self._socket, num_of_bytes,
                                                               self._recv_buffer_size)
                        except socket.error as se:
                            err_msg = se.strerror
                        except:
                            err_msg = 'Unexpected Error'
                        finally:
                            if err_msg != '':
                                cmd = {'msg': 'SOCKET_ERROR', 'conn': self, 'error': err_msg}
                                # Put the error in the Member's queue
                                self._member_queue.put(cmd)
                                # Set _ready to False to avoid reading over a faulty socket
                                self._ready = False
                                # Close the socket
                                self._socket.close()

                        if err_msg == '':
                            # Insert Part readed in the Member's queue
                            self._member_queue.put({'msg': 'RECEIVED_PART',
                                                    'conn': self,
                                                    'part': part_number,
                                                    'data': n_bytes})
                    else:
                        pass
                    self._cmd_read = False

    # Read a line from socket
    # Taken from the practical sessions
    @staticmethod
    def _read_line(sock):
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
        bytes_read = bytearray()
        to_read = n_bytes
        data = b""
        while to_read > 0:
            if to_read <= buffer_size:
                data = sock.recv(to_read)
            else:
                data = sock.recv(buffer_size)
            to_read -= len(data)
            bytes_read.extend(data)
        return bytes_read

    def start(self):
        rt = Thread(target=self.receive)
        st = Thread(target=self.send)
        rt.start()
        st.start()
