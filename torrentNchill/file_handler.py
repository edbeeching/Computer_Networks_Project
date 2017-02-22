"""

    @author: Sejal
    Created on: Tue, Feb 21
    Last updated: Tue, Feb 21

    The file handler is responsible for reading/writing a file.
    This can be done either directly to/from the file or to/from the memory (buffer).

    To do: Implement buffer fetch.

"""

from threading import Thread
import socket


class FileHandler:

    def __init__(self, dictionary, in_queue, out_queue):
        Thread.__init__(self)
        self.dictionary = dictionary
        self.in_queue = in_queue
        self.out_queue = out_queue
        #Use later to fetch from Memory rather than the Disc
        self.buffer_size = 1024


    def read_in(self):
        while True:
            print('Looking for commands in the in_queue')
            command = self.in_queue.get()

            if command['msg'] == 'WRITE':
                #Read the dictionary containing meta-data of the parts: _get_parts_dict [member.py]

            elif command['msg'] == 'FETCH':
                #For now, fetch directly from the file rather than in buffer.
            else:
                print("Message is not understood")


