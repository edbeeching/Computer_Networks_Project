from threading import Thread
import socket
import threading
import testmember
import queue

"""
    Created by Jorge Chong 31/01/2017
    Class Connection, manage the Receiver and Sending threads over a socket
    and interact with the Member that created the corresponding connection

    Status: Incomplete
"""


class Connection:
    # Can send and receive?
    # This flag is set to false after an socket i/o error
    _ready = True
    _cmd_read = False
    _recv_buffer_size = 1024

    # socket: a reference to the socket created before the connection is instantiated
    # member: a reference to the member
    def __init__(self, sock, member):
        self._socket = sock
        self._member = member
        self._send_queue = queue.Queue()

    # Read from the queue, build the message according to protocol.md
    # and send it through socket
    def send(self):
        # If there was no error in the socket communication
        if self._ready:
            # Always waiting for something to send
            while True:
                if not self._send_queue.empty():
                    cmd = self._send_queue.get()
                    # Build the message to be sent over socket
                    data = ''
                    if 'data' in cmd:
                        data = cmd['data']
                    msg = '{}\r\n{}\r\n{}\r\n'.format(cmd['msg'], cmd['filename'], cmd['checksum'])
                    if 'part' in cmd:
                        msg += '{}\r\n'.format(cmd['part'])

                    # print(msg)

                    send_buffer = bytearray()
                    send_buffer.extend(msg.encode('ascii'))
                    for byte in data:
                        send_buffer.extend(byte)

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
                            cmd = {'msg': 'ERROR', 'error': err_msg}
                            # Put the error in the Member's queue
                            self._member.add_command(cmd)
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
                            cmd = {'msg': 'ERROR', 'error': err_msg}
                            # Put the error in the Member's queue
                            self._member.add_command(cmd)
                            # Set _ready to False to avoid reading over a faulty socket
                            self._ready = False
                            # Close the socket
                            self._socket.close()

                    # 1. Interpret command, if no errors in the socket connection:
                    if err_msg == '':
                        if last_cmd in ['DOWN', 'NONE', 'SEND', 'STRT']:
                            self._cmd_read = True
                        else:
                            # Error in the message received, raise an exception and notify to
                            # the member to take action
                            self._member.add_command({'msg': 'ERROR', 'error': 'Bad Command Received'})
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
                        filename = Connection._read_line(self._socket)
                        checksum = Connection._read_line(self._socket)
                        part = Connection._read_line(self._socket)
                    except socket.error as se:
                        err_msg = se.strerror
                    except:
                        err_msg = 'Unexpected Error'
                    finally:
                        if err_msg != '':
                            cmd = {'msg': 'ERROR', 'error': err_msg}
                            # Put the error in the Member's queue
                            self._member.add_command(cmd)
                            # Set _ready to False to avoid reading over a faulty socket
                            self._ready = False
                            # Close the socket
                            self._socket.close()
                        else:
                            n_bytes = ''
                            command = {'msg': last_cmd,
                                       'filename': filename,
                                       'checksum': checksum,
                                       'part': part
                                       }
                            if last_cmd == 'STRT':
                                # To read the bynary data, we need to know how many bytes to read from the
                                # socket therefore, we have to query the member about the number of bytes
                                # of a specific part
                                num_of_bytes = self._member.get_num_of_bytes(filename, part)
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
                                        cmd = {'msg': 'ERROR', 'error': err_msg}
                                        # Put the error in the Member's queue
                                        self._member.add_command(cmd)
                                        # Set _ready to False to avoid reading over a faulty socket
                                        self._ready = False
                                        # Close the socket
                                        self._socket.close()

                            self._cmd_read = False
                            command['data'] = n_bytes
                            # Add the request to the Member's queue
                            self._member.add_command(command)
                            print(command)

    # Put a message in the queue
    # Message has the format: dictionary of key, value pairs, example:
    # {'msg': 'SEND', 'other': 'OTHER STUFF'}
    def put_message(self, message):
        self._send_queue.put(message)

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


if __name__ == "__main__":
    # A connection has to be created outside of connection, typically by a Member
    # since there is the possibility to be listening in a port PORT.
    # Therefore a connection can be created actively when the Member wants to
    # connect to another Member or passively when listening for a connection
    # The Member has to manage this interaction and create an instance of the
    # Connection class

    # Examples:

    # 1. Listening for a connection in port PORT
    None
    #MBR_ADDRESS = 'localhost'
    #MBR_PORT = 7666
    #mbr_socket = socket.socket()
    #mbr_socket.bind((MBR_ADDRESS, MBR_PORT))
    #mbr_socket.listen()