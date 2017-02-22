"""

    @author: Sejal
    Created on: Tue, Feb 21
    Last updated: Tue, Feb 22

    The file handler is responsible for reading/writing a file.
    This can be done either directly to/from the file or to/from the memory (buffer).

    TO DO: Implement buffer fetch.

    Messages received from member:

    # DIRECTOR RECEIVES
        message = {'msg': 'GOT_PART', 'conn':Connection, 'part': number, 'data': data}
        TO DO : message = {'msg': 'PART_NOT_FOUND', 'conn':Connection, 'part': number}


    # DIRECTOR SENDS
        message = {'msg': 'GIVE_PART', 'conn':Connection, 'part': number}
        message = {'msg': 'WRITE_PART', 'conn':Connection, 'part': number, 'data': data}

"""

from threading import Thread


class FileHandler(Thread):

    def __init__(self, dictionary, in_queue, out_queue):
        Thread.__init__(self)
        self.dictionary = dictionary
        self.in_queue = in_queue
        self.out_queue = out_queue
        #Use later to fetch from Memory rather than the Disc
        #self.buffer_size = 1024
        FileHandler.read_in()

    def write_part(composition_name, bytes_per_part, part, data):
            position = bytes_per_part * (part - 1)
            try:
                with open(composition_name, "wb") as file:
                    file.seek(position)
                    file.write(data)
            except IOError:
                print('IO exception')
            finally:
                file.close()
            return

    def read_part(composition_name, bytes_per_part, part):
        position = bytes_per_part * (part - 1)
        try:
            with open(composition_name, 'rb') as file:
                file.seek(position)
                data_part = file.read(bytes_per_part)
        except IOError:
                print('IO exception')
        finally:
            file.close()
        return data_part

    def read_in(self):
        while True:
            print('Looking for commands in the in_queue')
            command = self.in_queue.get()

            composition_name = self.dictionary['composition_name']
            bytes_per_part = self.dictionary['bytes_per_part']

            # Read the dictionary containing meta-data of the parts: _get_parts_dict [member.py]
            if command['msg'] == 'WRITE_PART':
                part = command['part']
                data = command['data']
                FileHandler.write_part(composition_name, bytes_per_part, part, data)

            elif command['msg'] == 'GIVE_PART':
                part = command['part']
                Connection = command['conn']

                #For now, fetch directly from the file rather than in buffer.
                part_requested = FileHandler.read_part(composition_name, bytes_per_part, part)
                message = {'msg': 'GOT_PART', 'conn': Connection, 'part': part, 'data': part_requested}
                self.out_queue.put(message)
                #To manange incase the fetch was unsuccesful

            else:
                print("Message is not understood")