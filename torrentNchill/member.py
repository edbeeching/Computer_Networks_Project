import sys
import os
import hashlib
import time
import copy
import random
import queue
import socket
import logging
from threading import Thread, Timer

import connection_handler
import connection
import netutils
import file_handler
import monitor

'''
    Created By Edward Beeching, last update 03/03/2017

        The Member class acts as a Director who organises the activities of:
        1. A ConnectionHandler who receives and makes connections
        2. A FileHandler who handles reading and writing parts from disk
        3. A Set of Connections which send and receive requests / data from other Members on the network

        # Remaining tasks
            Improved exception handling
            Clean exit, list for keyboard input?


    The member communicates with instances of the Connection Class with the following messages:
        # DIRECTOR RECEIVES
            message = {'msg': 'RECEIVED_PART', 'conn':Connection, 'part': number, 'data': data}
            message = {'msg': 'RECEIVED_PARTS_LIST', 'conn':Connection, 'parts_list': parts_list}
            message = {'msg': 'DISCONNECTED','conn':Connection }
            message = {'msg': 'PART_REQUEST','conn':Connection,'part': number}
            message = {'msg': 'PARTS_LIST_REQUEST','conn':Connection}
            message = {'msg': 'BAD_FILE_REQUEST',,'conn':Connection 'filename': filename, 'checksum': checksum, 'part': number} # Don't PEP8 me!
            message = {'msg': 'SOCKET_ERROR', 'conn': connection, 'error': err_msg}

        # DIRECTOR SENDS
            message = {'msg': 'SEND_PART', 'part': number, 'data': data}
            message = {'msg': 'SEND_PARTS_LIST', 'parts_list': number}
            message = {'msg': 'DISCONNECT'}
            message = {'msg': 'REQUEST_PART', 'part': number}
            message = {'msg': 'REQUEST_PARTS_LIST'}


    The Member communicates with a ConnectionHandler in order to receive and make connections
        # DIRECTOR RECEIVES
            message = {'msg': 'COND_IPS', 'ip_list': ip_list}
            message = {'msg': 'NEWCON', 'sock': clientsocket}

        # DIRECTOR SENDS
            message = {'msg': 'CRTCON', 'ip': ip, 'port': port}
            message = {'msg': 'COND_IPS'}


    The Member communicates with a FileHandler in order to get parts for disk /  a buffer. This is abstracted

        # DIRECTOR RECEIVES
            message = {'msg': 'GOT_PART', 'conn':Connection, 'part': number, 'data': data}
            message = {'msg': 'PART_NOT_FOUND', 'conn':Connection, 'part': number}

        # DIRECTOR SENDS
            message = {'msg': 'GIVE_PART', 'conn':Connection, 'part': number}
            message = {'msg': 'WRITE_PART', 'conn':Connection, 'part': number, 'data': data}


'''


class Member(Thread):
    def __init__(self, orch_filename):
        logging.basicConfig(format='%(asctime)s %(message)s', filename='logs/log1.log', filemode='w',
                            level=logging.DEBUG)
        # Initialise the thread
        Thread.__init__(self)

        # load orch file info and print it

        # queue holding messages from connections
        # queue to hold messages from CEO ? two constructors for the messages
        # dict of IP:PORT: AVAILABLE PARTS
        # dict of Connections: IP:PORT are dicts mutable?
        # filename of file
        # load orch into member
        # dict of parts / checksums
        # listener to find new connections from other peers
        self.orch_dict = Member._get_orch_parameters(orch_filename)
        logging.info('MEMBER: Orch Dict %s', self.orch_dict)

        Member._write_dummy_composition(self.orch_dict.get('composition_name'),
                                        self.orch_dict.get('total_bytes'))

        self.parts_dict = Member._get_parts_dict(self.orch_dict.get('composition_name'),
                                                 self.orch_dict.get('bytes_per_part'),
                                                 self.orch_dict.get('parts_checksum_dict'))
        self.connections_ip_dict = {}
        # self.ip_connections_dict = {}
        self.connections_parts_dict = {}
        self.active_transfers = {}
        self.connections_queue_dict = {}
        self.list_of_orch_ips = {}
        self.director_queue = queue.Queue()
        message = {'msg': 'CONDUCTOR'}
        self.director_queue.put(message)

        # Start thread that handles connections
        self.connect_queue = queue.Queue()
        self.con_handler = connection_handler.ConnectionHandler(self.connect_queue, self.director_queue, self.orch_dict)
        # Starts a thread that handles reading & writing parts to disk
        self.file_queue = queue.Queue()
        self.f_handler = file_handler.FileHandler(self.orch_dict, self.file_queue, self.director_queue)
        # Starts a thread that handles monitoring progress for output in terminal
        # First count to see if we have any parts already
        has_parts = len([i for i in self.parts_dict.values() if i is True])

        self.monitor_queue = queue.Queue()
        self.mon = monitor.Monitor(has_parts, self.orch_dict['num_parts'], self.monitor_queue, self.director_queue)


    def run(self):
        self.con_handler.start()
        self.f_handler.start()
        self.mon.start()
        while True:

            # Poll queue
            message = self.director_queue.get()
            # Respond to messages
            # print('MEMBER: Received message:', message['msg'])
            if self._handle_director_connection_msg(message):
                pass
                # print('MEMBER:', message, ' was handled by DIR - CONN')

            elif self._handle_director_con_handle_msg(message):
                pass
                # print('MEMBER:', message,  'was handled by DIR - CONN HANDLER')

            elif self._handle_director_file_handle_msg(message):
                pass
                # print('MEMBER:', message, ' was handled by DIR- FILE HANDLER')
            elif message['msg'] == 'CLOSE':
                print('Trying to exit gracefully')
                logging.info('MEMBER: Trying to exit gracefully')
                self._clean_exit() # Try and exit gracefully
                logging.info('MEMBER: Returning')
                return
                # break
            elif message['msg'] == 'OTHER':
                pass
                # print('MEMBER: Message is other')

            else:
                logging.warning('MEMBER: Could not read message', message)
                # # TODO REMOVE this sleep in the final version
                # time.sleep(0.1)

    def _handle_director_connection_msg(self, message):
        """
            handles the director connection message communications
        :param message: The message to handle


        # DIRECTOR RECEIVES
            message = {'msg': 'RECEIVED_PART', 'conn':Connection, 'part': number, 'data': data} # DONE
            message = {'msg': 'RECEIVED_PARTS_LIST', 'conn':Connection, 'parts_list': parts_list} # DONE
            message = {'msg': 'DISCONNECTED','conn':Connection }
            message = {'msg': 'PART_REQUEST','conn':Connection,'part': number} #DONE
            message = {'msg': 'PARTS_LIST_REQUEST','conn':Connection} # DONE
            message = {'msg': 'BAD_FILE_REQUEST',,'conn':Connection 'filename': filename, 'checksum': checksum, 'part': number} # Don't PEP8 me!

        # DIRECTOR SENDS
            message = {'msg': 'SEND_PART', 'part': number, 'data': data}
            message = {'msg': 'SEND_PARTS_LIST', 'parts_list': number}
            message = {'msg': 'DISCONNECT'}
            message = {'msg': 'REQUEST_PART', 'part': number}
            message = {'msg': 'REQUEST_PARTS_LIST'}
        """

        if message['msg'] == 'PARTS_LIST_REQUEST':
            # print('MEMBER: PARTS LIST REQUEST')
            self._handle_parts_list_request(message)
            return True

        elif message['msg'] == 'RECEIVED_PARTS_LIST':
            self._handle_received_parts_list(message)
            return True

        elif message['msg'] == 'RECEIVED_PART':
            # Send to filehandler message = {'msg': 'WRITE_PART', 'conn': Connection, 'part': number, 'data': data}
            if not self._checksum_part(message):
                logging.warning('MEMBER: Invalid part received, part number:%s %s %s', message['part'], 'Connection:',
                                message['conn'])
                self._assign_parts_request(message['conn'])
                return

            logging.info('MEMBER: Valid part received, part number:%s %s %s', message['part'], 'Connection:',
                         message['conn'])
            out_message = {'msg': 'WRITE_PART', 'conn': message['conn'], 'part': message['part'],
                           'data': message['data']}
            tot_parts = len([i for i in self.parts_dict.values() if i is True])
            self.monitor_queue.put({'msg': 'PARTS', 'parts': tot_parts})
            self.file_queue.put(out_message)
            self.parts_dict[message['part']] = True
            if message['conn'] in self.active_transfers:
                self.active_transfers.pop(message['conn'])

            self._assign_parts_request(message['conn'])

            return True

        elif message['msg'] == 'PART_REQUEST':
            # Send to filehandler message = {'msg': 'GIVE_PART', 'conn': Connection, 'part': number}
            out_message = {'msg': 'GIVE_PART', 'conn': message['conn'], 'part': message['part']}
            self.file_queue.put(out_message)
            return True

        elif message['msg'] == 'ERROR':
            conn = message['conn']
            self._remove_connection(conn)
            return True
        else:
            return False

    def _handle_director_con_handle_msg(self, message):
        """
        # DIRECTOR RECEIVES
            message = {'msg': 'COND_IPS', 'ip_list': ip_list}
            message = {'msg': 'NEWCON', 'sock': clientsocket}

        # DIRECTOR SENDS
            message = {'msg': 'CRTCON', 'ip': ip, 'port': port}
            message = {'msg': 'COND_IPS'}
        """
        if message['msg'] == 'CONDUCTOR':

            self.connect_queue.put({'msg': 'COND_IPS'})
            return True
        elif message['msg'] == 'COND_IPS':
            self._get_ips_from_conductor(message['ip_list'])
            return True
        elif message['msg'] == 'POLL':
            self._poll_ips()
            return True

        elif message['msg'] == 'NEWCON':
            socket = message['sock']
            self._create_connection(socket)
            return True
        else:
            return False

    def _handle_director_file_handle_msg(self, message):
        """
        # DIRECTOR RECEIVES
            message = {'msg': 'GOT_PART', 'conn':Connection, 'part': number, 'data': data}
            message = {'msg': 'PART_NOT_FOUND', 'conn':Connection, 'part': number}


        # DIRECTOR SENDS
            message = {'msg': 'GIVE_PART', 'conn':Connection, 'part': number}
            message = {'msg': 'WRITE_PART', 'conn':Connection, 'part': number, 'data': data}
        """
        if message['msg'] == 'GOT_PART':
            conn = message['conn']
            # if conn not in self.connections_queue_dict[conn]:
            #     return
            con_queue = self.connections_queue_dict[conn]
            # message = {'msg': 'SEND_PART', 'part': number, 'data': data}
            out_message = {'msg': 'SEND_PART', 'part': message['part'], 'data': message['data']}
            con_queue.put(out_message)
            return True

        elif message['msg'] == 'PART_NOT_FOUND':
            logging.info('MEMBER: A part has not been found! %s', message)
            return True

        else:
            return False

    def _remove_connection(self, conn):

        # Removes entries of conn for all the connection dicts
        logging.info('MEMBER: Connection closing %s', conn)
        if conn in self.connections_ip_dict:
            # ip = self.connections_ip_dict
            del self.connections_ip_dict[conn]
            # if ip in self.ip_connections_dict:
            #     del self.ip_connections_dict[ip]
        if conn in self.connections_parts_dict:
            del self.connections_parts_dict[conn]
        if conn in self.connections_queue_dict:
            del self.connections_queue_dict[conn]
        if conn in self.active_transfers:
            del self.active_transfers[conn]

    def _clean_exit(self):
        # Try and make a clean exit by passing all handlers the CLOSE message
        self.connect_queue.put({'msg': 'CLOSE'})
        self.file_queue.put({'msg': 'CLOSE'})
        for q in self.connections_queue_dict.values():
            q.put({'msg': 'CLOSE'})
        # Give them half a second to close gracefully
        time.sleep(0.5)
        logging.info('MEMBER: Joining ConnectionHandler')
        self.con_handler.join()
        logging.info('MEMBER: Joining FileHandler')
        self.f_handler.join()
        # for con in self.connections_queue_dict.keys():
        #     logging.info('MEMBER: Joining Connections')
        #     con.join()
        logging.info('MEMBER: Joining Monitor')
        self.mon.join()
        logging.info('MEMBER: All threads joined, exiting')


    def _handle_parts_list_request(self, message):
        parts_int = Member._get_parts_int(self.parts_dict)

        conn = message['conn']
        if conn not in self.connections_queue_dict:
            return
        con_queue = self.connections_queue_dict[conn]

        message = {'msg': 'SEND_PARTS_LIST', 'parts_list': parts_int}
        con_queue.put(message)

    def _handle_received_parts_list(self, message):
        con_parts_list = Member._get_con_parts_dict(message['parts_list'], self.orch_dict['num_parts'])
        self.connections_parts_dict[message['conn']] = con_parts_list
        logging.info("MEMBER: parts list received %s %s", message['parts_list'], con_parts_list)

        self._assign_parts_request(message['conn'])

    def _assign_parts_request(self, conn):
        if conn in self.active_transfers:
            logging.info(
                'MEMBER: Note that a connection with an active transfer is being assigned another part, returning')
            return

        # The most Pythonic statements I've ever writen
        unassigned_parts = [i for i in self.parts_dict.keys() if self.parts_dict.get(i) is False]
        parts_needed = [i for i in unassigned_parts if i not in set(self.active_transfers.keys())]

        if len(parts_needed) > 0:
            # Get a random index and that part will be assigned for this connection
            rand_int = random.randrange(0, len(parts_needed))
            message = {'msg': 'REQUEST_PART', 'part': parts_needed[rand_int]}
            # if conn not in self.connections_queue_dict[conn]:
            #     return
            self.connections_queue_dict[conn].put(message)
            self.active_transfers[conn] = parts_needed[rand_int]
        else:
            # There are no parts available for this connection to retrieve (perhaps send a timer here
            # TODO Add a timer here
            logging.info('MEMBER: Connection has no parts available to retrieve')

    def _get_ips_from_conductor(self, ip_list):

        # Updating with the protocol
        # TODO if this blocks is will hang the whole program

        for ip in ip_list:
            if self.list_of_orch_ips.get(ip):
                # We already have this in the list
                pass
            else:
                self.list_of_orch_ips[ip] = 1  # IPs can have ratings in case they behave badly
                logging.info('MEMBER: list of ips is: %s', self.list_of_orch_ips)
        message = {'msg': 'POLL'}
        self.director_queue.put(message)

    def _poll_ips(self):
        if len(self.list_of_orch_ips) is 0:
            delayed_message = {'msg': 'CONDUCTOR'}
            # Create a Timer that will resend a conductor query message in 5 seconds
            timer = Timer(5, Member._delayed_message, (self.director_queue, delayed_message))
            timer.start()
            return
        for ip in self.list_of_orch_ips.keys():
            # print('poll ips trying to connect to', ip)
            if self.connections_ip_dict.get(ip):
                # We are already connected to this IP
                pass
            else:
                [ip, port] = str(ip).split(':')
                # print('POLL Connecting', ip, port)
                message = {'msg': 'CRTCON', 'ip': ip, 'port': port}
                self.connect_queue.put(message)

        # Create a Timer that will resend a conductor query message in 10 seconds
        delayed_message = {'msg': 'CONDUCTOR'}
        timer = Timer(10, Member._delayed_message, (self.director_queue, delayed_message))
        timer.start()

    def _create_connection(self, sock):
        # socket, dict, directors queue, sending queue

        send_queue = queue.Queue()
        message = {'msg': 'REQUEST_PARTS_LIST'}

        send_queue.put(message)
        # Using deep copy to ensure there are no race conditions on orch dict
        con = connection.Connection(sock, copy.deepcopy(self.orch_dict), send_queue, self.director_queue)
        con.start()
        # Add to up the dictionaries

        ip, port = sock.getpeername()
        self.connections_ip_dict[con] = ip
        # self.ip_connections_dict[ip] = con
        self.connections_parts_dict[con] = 0
        self.connections_queue_dict[con] = send_queue

    def _checksum_part(self, message):
        hasher = hashlib.sha1()
        hasher.update(message['data'])
        if hasher.hexdigest() == self.orch_dict['parts_checksum_dict'][message['part']]:
            return True
        else:
            return False

    @staticmethod
    def _delayed_message(q, message):
        q.put(message)

    @staticmethod
    def _get_orch_parameters(orch_filename):
        assert os.path.isfile(orch_filename)
        orch_dict = {}
        with open(orch_filename, 'r') as file:
            try:
                # Read in the orch file, note the rstrip is used to remove the trailing newline characters
                orch_dict['conductor_ip'] = str(file.readline()).rstrip()
                orch_dict['composition_name'] = str(file.readline()).rstrip()
                orch_dict['full_checksum'] = str(file.readline()).rstrip()
                orch_dict['total_bytes'] = int(file.readline())
                orch_dict['bytes_per_part'] = int(file.readline())
                orch_dict['num_parts'] = int(file.readline())

                orch_dict['parts_checksum_dict'] = {}

                for i in range(orch_dict.get('num_parts')):
                    orch_dict.get('parts_checksum_dict')[i + 1] = str(file.readline()).rstrip()

            finally:
                file.close()
        return orch_dict

    @staticmethod
    def _write_dummy_composition(composition_name, file_size):

        # check filesize is less than 1 GB, to avoid writing massive files to disk by accident
        assert file_size < 1024 * 1024 * 1024

        if os.path.isfile(composition_name):
            assert os.path.getsize(composition_name) == file_size
        else:
            # TODO Allocate less memory by writing smaller chunks of data, if this was a 1GB file, 1GB would be alloc.
            byte_buffer = bytearray(file_size)

            with open(composition_name, "wb") as file:
                try:
                    file.write(byte_buffer)
                finally:
                    file.close()
                    assert os.path.getsize(composition_name) == file_size

    @staticmethod
    def _get_parts_dict(composition_name, bytes_per_part, parts_checksum_dict):
        parts_dict = {}
        with open(composition_name, 'rb') as file:
            try:
                part_num = 1
                bytes_read = file.read(bytes_per_part)
                while bytes_read:
                    hasher = hashlib.sha1()
                    hasher.update(bytes_read)

                    if hasher.hexdigest() == parts_checksum_dict[part_num]:

                        parts_dict[part_num] = True
                    else:
                        parts_dict[part_num] = False

                    part_num += 1
                    bytes_read = file.read(bytes_per_part)
            finally:
                file.close()
        return parts_dict

    @staticmethod
    def _get_parts_int(parts_dict):
        # print(parts_dict)
        parts_int = 1 << len(parts_dict.keys())
        # print('MEMBER: parts list length', len(parts_dict.keys()))
        for i in range(0, len(parts_dict)):
            if parts_dict[i + 1] is True:
                parts_int += 1 << i
        return parts_int

    @staticmethod
    def _get_con_parts_dict(parts_int, total_parts):
        con_parts_dict = {}
        for i in range(0, total_parts):
            if parts_int & 1 << i > 0:
                con_parts_dict[i + 1] = True
            else:
                con_parts_dict[i + 1] = False
        return con_parts_dict


if __name__ == "__main__":
    print('number of arguments', len(sys.argv))
    print('arguments', str(sys.argv))

    if len(sys.argv) > 1:
        member = Member(sys.argv[1])
    else:
        # orch = 'maxresdefault.jpg.orch'
        #orch = 'ATJ.jpg.orch'
        orch = 'Sciences.M1ML.complete.zip.orch'
        member = Member(orch)

    member.start()
    member.join()
    print('Member joined')
    sys.exit()
    # test_dict = {1: True, 2: False, 3: True, 4: False, 5: True, 6: False, 7: True, 8: False, 9: True}
    # parts_int = Member._get_parts_int(test_dict)
    #
    # check_dict = Member._get_con_parts_dict(parts_int, 9)
    # print(check_dict)
