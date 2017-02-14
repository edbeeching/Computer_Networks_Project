import sys
import os
import hashlib
import time
from threading import Thread
import queue
import socket
import netutils
import connection_handler


class Member(Thread):

    def __init__(self, orch_filename):
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

        Member._write_dummy_composition(self.orch_dict.get('composition_name'),
                                        self.orch_dict.get('total_bytes'))

        self.parts_dict = Member._get_parts_dict(self.orch_dict.get('composition_name'),
                                                 self.orch_dict.get('bytes_per_part'),
                                                 self.orch_dict.get('parts_checksum_dict'))
        self.connections_ip_dict = {}
        self.connections_parts_dict = {}
        self.connections_queue_dict = {}
        self.list_of_orch_ips = {}
        self.director_queue = queue.Queue()
        message = {'msg': 'CONDUCTOR'}
        self.director_queue.put(message)

        # Start thread that handles connections
        self.connect_queue = queue.Queue()
        con_handler = connection_handler.ConnectionHandler(self.connect_queue, self.director_queue)
        con_handler.start()

        # Thread for file IO
        # dict of

    def run(self):
        while True:

            # Poll queue
            message = self.director_queue.get()
            # Respond to messages
            if message['msg'] == 'CONDUCTOR':
                self._get_ips_from_conductor()
            elif message['msg'] == 'POLL':
                self._poll_ips()
                print('Message is poll')
            elif message['msg'] == 'OTHER':
                print('Message is other')
            else:
                print('Could not read message')









            print("Hello World!")
            time.sleep(0.1)

    def _get_ips_from_conductor(self):
        ip_list = []
        cond_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            ip = str.split(self.orch_dict['conductor_ip'], ':')[0]
            port = str.split(self.orch_dict['conductor_ip'], ':')[1]
            print(ip, port)
            if ip != 'localhost':
                ip = int(ip)
            cond_socket.connect((ip, int(port)))
            print('Getting IPs from conductor')
            msg = netutils.read_line(cond_socket)
            while msg:
                ip_list.append(msg)
                print(msg)
                msg = netutils.read_line(cond_socket)
        finally:

            print('closing connection')
            cond_socket.shutdown(socket.SHUT_RDWR)
            cond_socket.close()

        for ip in ip_list:
            if ip in self.list_of_orch_ips:
                pass
            else:
                self.list_of_orch_ips[ip] = 1  # IPs can have ratings in case they behave badly
        message = {'msg': 'POLL'}
        self.director_queue.put(message)

    def _poll_ips(self):
        for ip in self.list_of_orch_ips.keys():
            if ip in self.connections_ip_dict:
                pass
            else:
                self._create_connection(ip)
    def _create_connection(self, ip):
        

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
                    orch_dict.get('parts_checksum_dict')[i+1] = str(file.readline()).rstrip()

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
                    assert os.path.getsize(composition_name) == file_size
                    file.close()

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



if __name__ == "__main__":

    print('number of arguments', len(sys.argv))
    print('arguments', str(sys.argv))

    orch = 'maxresdefault.jpg.orch'
    member = Member(orch)
    member.start()

    member.join()

