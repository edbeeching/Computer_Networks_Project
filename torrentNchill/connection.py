from threading import Thread
import socket
import threading
import queue

"""
    Created by Jorge Chong 14/02/2017
    Class Connection, manage the Receiver and Sending threads over a socket
    and interact with the Member that created the corresponding connection

    Status: Incomplete
"""


class Connection:
    # Can send or receive?
    # This flag is set to false after an socket i/o error

    # socket: a reference to the socket created before the connection is instantiated
    # member: a reference to the member
    def __init__(self, sock, dictionary, send_queue, member_queue):
        self._socket = sock
        self._dictionary = dictionary
        self._member_queue = member_queue
        self._ready = True
        self._cmd_read = False
        self._recv_buffer_size = 1024
        self._ip, self._port = self._socket.getpeername()
        self._send_queue = send_queue

    # Read from the queue, build the message according to protocol.md
    # and send it through socket
    def send(self):
        # If there was no error in the socket communication
        if self._ready:
            # Always waiting for something to send
            while True:
                #if not self._send_queue.empty():
                cmd = self._send_queue.get()
                if cmd['msg'] == 'REQUEST_PARTS_LIST':
                    msg = '{}\r\n{}\r\n{}\r\n'.format('DOWN', self._dictionary['composition_name'],
                                                      self._dictionary['full_checksum'])
                    print('Sending message: {}'.format(msg))
                elif cmd['msg'] == 'SEND_PARTS_LIST':
                    msg = '{}\r\n{}\r\n{}\r\n{}\r\n'.format('SEND', self._dictionary['composition_name'],
                                                      self._dictionary['full_checksum'], str(cmd['parts_list']))
                else:
                    pass

                # Build the message to be sent over socket
                #data = ''
                #if 'data' in cmd:
                #    data = cmd['data']
                #msg = '{}\r\n{}\r\n{}\r\n'.format(cmd['msg'], cmd['filename'], cmd['checksum'])
                #if 'part' in cmd:
                #    msg += '{}\r\n'.format(cmd['part'])

                # print(msg)

                send_buffer = bytearray()
                send_buffer.extend(msg.encode('ascii'))
                #for byte in data:
                #    send_buffer.extend(byte)

                # If there is an error in the socket (the other end disconnects, etc)
                # shutdown the socket an inform the Member, and set self._ready to False
                err_msg = ''
                try:
                    print(send_buffer)
                    self._socket.sendall(send_buffer)
                except socket.error as se:
                    err_msg = se.strerror
                except:
                    err_msg = 'Unexpected Error'
                finally:
                    if err_msg != '':
                        cmd = {'msg': 'ERROR', 'conn': self, 'error': err_msg}
                        # Put the error in the Member's queue
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
        if self._ready:
            while True:
                print("Receive Thread...")
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
                            cmd = {'msg': 'ERROR', 'conn': self, 'error': err_msg}
                            # Put the error in the Member's queue
                            self._member_queue.put(cmd)
                            # Set _ready to False to avoid reading over a faulty socket
                            self._ready = False
                            # Close the socket
                            self._socket.close()

                    # 1. Interpret command, if no errors in the socket connection:
                    if err_msg == '':
                        if last_cmd in ['DOWN', 'NONE', 'SEND', 'STRT', 'PART']:
                            self._cmd_read = True
                        else:
                            # Error in the message received, raise an exception and notify to
                            # the member to take action
                            self._member_queue.put({'msg': 'ERROR', 'error': 'Bad Command Received'})
                else:
                    # 2. Read the rest of the request
                    # Cmd = SEND, read 3 lines
                    # SEND \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n

                    # Cmd = NONE, read 3 lines
                    # Can be a reply to a DOWN command:
                    # NONE \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n 0 \r\n
                    # Or a reply to a STRT command:
                    # NONE \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n

                    # Cmd = STRT, read 3 lines + data
                    # STRT \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n <DATA>
                    err_msg = ''
                    try:
                        print('Last command received:{}'.format(last_cmd))
                        filename = Connection._read_line(self._socket)
                        checksum = Connection._read_line(self._socket)
                        part = ''
                        if last_cmd in ['NONE', 'SEND', 'PART', 'STRT']:
                            part = Connection._read_line(self._socket)
                    except socket.error as se:
                        err_msg = se.strerror
                    except:
                        err_msg = 'Unexpected Error'
                    finally:
                        if err_msg != '':
                            cmd = {'msg': 'ERROR', 'conn': self, 'error': err_msg}
                            # Put the error in the Member's queue
                            self._member_queue.put(cmd)
                            # Set _ready to False to avoid reading over a faulty socket
                            self._ready = False
                            # Close the socket
                            self._socket.close()
                        else:
                            n_bytes = ''
                            command = {'msg': last_cmd,
                                       'filename': filename,
                                       'checksum': checksum
                                       }
                            if part != '':
                                command['part'] = part
                            if last_cmd == 'STRT':
                                # To read the bynary data, we need to know how many bytes to read from the
                                # socket therefore, we have to query the member about the number of bytes
                                # of a specific part
                                part_number = int(part)
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
                                        cmd = {'msg': 'ERROR', 'conn': self, 'error': err_msg}
                                        # Put the error in the Member's queue
                                        self._member_queue.put(cmd)
                                        # Set _ready to False to avoid reading over a faulty socket
                                        self._ready = False
                                        # Close the socket
                                        self._socket.close()

                            self._cmd_read = False
                            command['data'] = n_bytes
                            # Add the request to the Member's queue
                            if command['msg'] == 'DOWN':
                                self._member_queue.put({'msg': 'PARTS_LIST_REQUEST', 'conn': self})
                            elif command['msg'] == 'SEND':
                                self._member_queue.put({'msg': 'RECEIVED_PARTS_LIST', 'conn': self, 'parts_list': part})
                            else:
                                None
                            print(command)

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
