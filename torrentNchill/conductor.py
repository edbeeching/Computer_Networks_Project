import socket
import threading
import netutils

# import pprint

"""
Created by Arslen REMACI

Conductor file

Accept connections from the members, save what they ask for, and send IPs if the file is ready to download from some members.

"""


def handler(s, ip):
    try:
        command = netutils.read_line(s)
        filename = netutils.read_line(s)
        checksum = netutils.read_line(s)
        port = netutils.read_line(s)
        
        # Read the command sent by the member

        namecheck = "(" + filename + "," + checksum + ")"
        listips = ""

        if command == "DOWN":
            if namecheck in dictionary: # If the file is found
                for i in dictionary[namecheck]:
                    listips = listips + i + '\r\n'  # Prepare the list of IPs

                s.sendall(bytes(
                    'SEND\r\n' + filename + '\r\n' + checksum + '\r\n' + str(len(dictionary[namecheck])) + '\r\n' + listips,
                    encoding="ascii")) 
                # Send everything as presented in the protocol

                if (ip + ":" + port) not in dictionary[namecheck]:  # If the member was not in the dictionary, add him
                    dictionary[namecheck].append(ip + ":" + port)
            else:
                s.sendall(bytes('NONE\r\n' + filename + '\r\n' + checksum + '\r\n', encoding="ascii")) 
                # File not found, send the command to the member
                
                dictionary[namecheck] = [ip + ":" + port]
                # Add the member to the dictionary

        print("=== Finished sending, member disconnected ===")
        s.close()
    except socket.error as er:
        print('Exception on socket', er)


tcpsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

tcpsocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

tcpsocket.bind(('', 9999))
tcpsocket.listen(5)

dictionary = {}

while True:
    s, (ip, port) = tcpsocket.accept()
    print(dictionary)
    print("+++ New thread for %s on %s +++" % (ip, port,))

    threading.Thread(target=handler, args=(s, ip,)).start()
