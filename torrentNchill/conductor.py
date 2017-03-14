import socket
import threading
import netutils
#import pprint

"""
Created by Arslen REMACI

Conductor file, send IPs to the member connecting to him

IPs saved in a file named "IPs.txt" (may change later)

"""

# dict{(filename,checksum),[IP:Port,IP:Port,...]}

def handler(s, ip):
    command = netutils.read_line(s)
    filename = netutils.read_line(s)
    checksum = netutils.read_line(s)
    port = netutils.read_line(s)

    namecheck = "(" + filename + "," + checksum + ")"
    listips = ""

    if(command == "DOWN"):
        if namecheck in dictionary:
            for i in dictionary[namecheck]:
                listips = listips + i + '\r\n'

            s.sendall(bytes('SEND\r\n' + filename + '\r\n' + checksum + '\r\n' + str(len(dictionary[namecheck])) + '\r\n' + listips, encoding="ascii"))

            if (ip+":"+port) not in dictionary[namecheck]:
                dictionary[namecheck].append(ip+":"+port)
        else:
            s.sendall(bytes('NONE\r\n' + filename + '\r\n' + checksum + '\r\n', encoding="ascii"))
            dictionary[namecheck] = [ip+":"+port]

    #else if(command == "UPLD")

    #pp = pprint.PrettyPrinter()
    #pp.pprint(dictionary)

    print("=== Finished sending, member disconnected ===")
    s.close()

tcpsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

tcpsocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

tcpsocket.bind(('', 9999))
tcpsocket.listen(5)

dictionary = {}

while True:
    s, (ip, port) = tcpsocket.accept()
    print(dictionary)
    print("+++ New thread for %s on %s +++" % (ip, port,))

    threading.Thread(target = handler, args = (s, ip,)).start()
