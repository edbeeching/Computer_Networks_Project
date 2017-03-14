import queue
import time
import hashlib
"""

    @author: Sejal
    Created on: Tue, Feb 21
    Last updated: Tue, Mar 7

    The file handler is responsible for reading/writing a file.
    This can be done either directly to/from the file or to/from the memory (buffer).

    Messages received from member:

    # DIRECTOR RECEIVES
        message = {'msg': 'GOT_PART', 'conn':Connection, 'part': number, 'data': data}
        TO DO : message = {'msg': 'PART_NOT_FOUND', 'conn':Connection, 'part': number}


    # DIRECTOR SENDS
        message = {'msg': 'GIVE_PART', 'conn':Connection, 'part': number}
        message = {'msg': 'WRITE_PART', 'conn':Connection, 'part': number, 'data': data}

"""

from threading import Thread
import logging

class FileHandler(Thread):

    def __init__(self, dictionary, in_queue, out_queue):
        Thread.__init__(self)
        self.dictionary = dictionary
        self.in_queue = in_queue
        self.out_queue = out_queue

        self.memory = {}
        self.recently_used = []


    @staticmethod
    def write_part(composition_name, bytes_per_part, part, data):
            position = bytes_per_part * (part - 1)
            try:
                with open(composition_name, "rb+") as file:
                    file.seek(position)
                    file.write(data)
            except IOError as er:
                logging.warning('FILE HANDLER: IO exception', er)
            finally:
                file.close()
            return

    @staticmethod
    def read_part(composition_name, bytes_per_part, part):
        position = bytes_per_part * (part - 1)
        with open(composition_name, 'rb') as file:
            try:
                #with open(composition_name, 'rb') as file:
                file.seek(position)
                data_part = file.read(bytes_per_part)
            except IOError as er:
                 logging.warning('FILE HANDLER: IO exception %s', er)
            finally:
                file.close()
        return data_part


    def run(self):
        while True:
            logging.info('FILE HANDLER: Looking for commands in the in_queue')
            command = self.in_queue.get()

            composition_name = self.dictionary['composition_name']
            bytes_per_part = self.dictionary['bytes_per_part']

            # Read the dictionary containing meta-data of the parts: _get_parts_dict [member.py]
            if command['msg'] == 'WRITE_PART':
                part = command['part']
                data = command['data']
                FileHandler.write_part(composition_name, bytes_per_part, part, data)

                if len(self.memory) <= 6000:
                    self.memory[part] = data
                    self.recently_used.append(part)
                else:
                    oldest = self.recently_used.pop(0)  # returns the key to the oldest item
                    del self.memory[oldest]
                    self.memory[part] = data
                    self.recently_used.append(part)



            elif command['msg'] == 'GIVE_PART':

                part = command['part']
                Connection = command['conn']

                #For now, fetch directly from the file rather than in buffer.
                part_requested = FileHandler.read_part(composition_name, bytes_per_part, part)
                message = {'msg': 'GOT_PART', 'conn': Connection, 'part': part, 'data': part_requested}
                self.out_queue.put(message)

                #First check in dictionary, if it exists in memory
                #If yes, fetch from memory

                logging.info('FILE HANDLER: Checking for part in the memory.')
                if part in self.memory:
                    logging.info('FILE HANDLER: Looking in the memory.')
                    part_requested = self.memory[part]
                    message = {'msg': 'GOT_PART', 'conn': Connection, 'part': part, 'data': part_requested}
                    self.out_queue.put(message)
                else:
                    part_requested = FileHandler.read_part(composition_name, bytes_per_part, part)
                    if len(self.memory) <= 6000:
                        self.memory[part] = part_requested
                        self.recently_used.append(part)
                    else:
                        oldest = self.recently_used.pop(0)  # returns the key to the oldest item
                        del self.memory[oldest]
                        self.memory[part] = part_requested
                        self.recently_used.append(part)
                    message = {'msg': 'GOT_PART', 'conn': Connection, 'part': part, 'data': part_requested}
                    self.out_queue.put(message)

                #To manange incase the fetch was unsuccesful
            elif command['msg'] == 'CLOSE':
                # TODO Check this does not cause other issues
                logging.info('FILE HANDLER: Closing')
                return
            else:
                logging.warning('FILE HANDLER: Message is not understood %s', message)

# Idea: For the memory,implement a linked list of five/ten recently used itemset. Once the 10 items are full,
# simply delete the least recently link and add a new one. Then keep a dictionary
# which gives the name, detail of the file part, etc stored in the linked list and its link number.
# Whenever a part is to be checked for, then we simply need to access this dictionary and if present
# fetch the part from the linked list.


if __name__ == "__main__":
    print('Testing')
    orch_dict = {'num_parts': 9, 'full_checksum': '2953289a34e0cc2bf776decc3f8b86622d66b705',
                'total_bytes': 142044, 'parts_checksum_dict':   {
                                                                    1: 'd53bff7979a4ac6f56da2f7085e6c2dff49656eb', 2: 'a36d78065883e2b2cf4b02f61ebbdc3b5dca7a26',
                                                                    3: '6dc13d0429aea3e39979a0191ca0aa80b6ab55d4', 4: 'a9ff34e937e3bf554046072fa489339c3df550fc',
                                                                    5: '0d597842e3d47c1d32c529d03f9f89054dcf3c76', 6: '2d1fc8ff5acdf2e63a694f1b99cae4a173933430',
                                                                    7: '1d9de0a99e38435b7001f3c85020e388d73529a8', 8: '32fa02ffdbcde31deb1290a24beafcf5554f35b7',
                                                                    9: 'c0a63314f9a0e677ecdd5bebeb5b746024deabba'
                                                                },

                'composition_name': 'maxresdefault.jpg', 'bytes_per_part': 16384, 'conductor_ip': '172.20.10.3:9999'}

    in_queue = queue.Queue()
    out_queue = queue.Queue()

    fileHandler = FileHandler(orch_dict, in_queue, out_queue)
    fileHandler.start()
    # message = {'msg': 'GIVE_PART', 'conn': Connection, 'part': number}
    # message = {'msg': 'WRITE_PART', 'conn': Connection, 'part': number, 'data': data}
    message = {'msg': 'GIVE_PART', 'conn': None, 'part': 3}
    #print('In queue message:',message)
    in_queue.put(message)

    new_message = out_queue.get()
    print('Out queue message:', new_message)

    hasher = hashlib.sha1()
    hasher.update(new_message['data'])
    print(hasher.hexdigest())

    #data2 = bytearray(16*1024)
    #in_queue.put({'msg': 'WRITE_PART', 'conn':None, 'part': 1, 'data': data2})

    message = {'msg': 'GIVE_PART', 'conn': None, 'part': 3}
    in_queue.put(message)

    new_message_2 = out_queue.get()
    print('Out queue message 2:', new_message_2)

    print(fileHandler.memory)


