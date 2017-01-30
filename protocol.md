
# Group DEF Protocol definition version 1.0
## This document defines the protocol for Member-Conductor and Member-Member communications.
### Authors:
* Edward Beeching     (Team D)
* Valentin Benozillo  (Team E)
* Jorge Chang         (Team D)
* Ishwer Purushotham  (Team F)

### Key points:
 * All messages start with a 4 character string to identify the message
 * All messages are encoded as UTF-8
 * Throughout this document we write " \r\n " with white space, this is to make the doc. more readable, the white space should not be included in the actual protocol
 * FILENAME should be limited to a maximum of 256 characters
 * Checksums are computed with the SHA-1 checksum
 * All communications are made over a TCP connection
 * KEY POINT!! Connections made between members are two-way, requests are made in both directions (possibly simultaneously depending on implementation) a connection is not a client-server relationship!
	
### 1. Orchestra file

The .orch file is defined as follows:

    CONDUCTOR IP:PORT
    FILENAME
    FULL_FILE_CHECKSUM
    FILE SIZE IN BYTES
    PART SIZE IN BYTES
    NUMBER OF PARTS
    CHECKSUM OF PART1
    CHECKSUM OF PART2
    ...............
    CHECKSUM OF LAST PART
 
Example:

    192.168.1.1:9999
    maxresdefault.jpg
    2953289a34e0cc2bf776decc3f8b86622d66b705
    142044
    16384
    9
    d53bff7979a4ac6f56da2f7085e6c2dff49656eb
    a36d78065883e2b2cf4b02f61ebbdc3b5dca7a26
    6dc13d0429aea3e39979a0191ca0aa80b6ab55d4
    a9ff34e937e3bf554046072fa489339c3df550fc
    0d597842e3d47c1d32c529d03f9f89054dcf3c76
    2d1fc8ff5acdf2e63a694f1b99cae4a173933430
    1d9de0a99e38435b7001f3c85020e388d73529a8
    32fa02ffdbcde31deb1290a24beafcf5554f35b7
    c0a63314f9a0e677ecdd5bebeb5b746024deabba


### 2. Member-Conductor communications

#### 2.1 How a member requests a list of IP:ports from the conductor
   The member sends the following message over TCP (after making a normal connection not the IP and port of the conductor are provided in the .orch file):

		DOWN \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <PORT> \r\n 

		Example:

		DOWN \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 9999 \r\n
		
#### 2.2 There are two possible replies
##### 2.2.1 	The server does not have a list of IP:ports for this file:
		
		NONE \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n 

		 Example:

		NONE \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 
		
##### 2.2.2 The server has a list of IP:ports for this file:
		
		SEND \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n NUMBER \r\n 
		IP:PORT \r\n 
		IP:PORT \r\n 
		IP:PORT \r\n 
		....

		Example:

		SEND \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 2 \r\n 
		1.1.1.1:1234 \r\n 
		192.168.1.1:9999 \r\n 
			
#### 2.3 Occasionally a member may wish to complain to the conductor about another member (the conductors response is implementation specific)
	
		COMP \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n IP:PORT \r\n

		Example:

		COMP \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 192.168.1.1:1234 \r\n
	
#### 2.4 It's possible that a member wants to announce to the conductor that they are sharing a new file
		
		UPLD \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <PORT> \r\n 

		Example:

		UPLD \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 9999 \r\n
		
		
### 3. Member-Member communications

####	3.1	When a member connects to another members for the first time it will typically ask for a list of available parts:
	
		DOWN \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n

		Example:

		DOWN \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 
		
#### 3.2 The second Member can make either of two replies:
	
##### 3.2.1 For some reason the second Member does not have the file at all: (note the zero included at the end of the message)
		
		NONE \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n 0 \r\n

		Example:

		NONE \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 0 \r\n
		
##### 3.2.2 The second Member has the file and replies with a binary encoded int, which details the list of parts
				
		SEND \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n

		Example:

		SEND \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 511 \r\n

The binary encoding is a bitwise representation of what parts are available:
A few examples:
E.g a file with 9 parts:

member has all parts: reply = 511  which is 11111111 11111111 (note the leading ones in order to make this two bytes)
member has first and third part: 	reply = 5    which is 11111110 00000101 (note the leading ones in order to make this two bytes)

In all cases the number of bytes will be the same, regardless of the whether a member has all parts or 0 parts.
i.e the number of bytes is floor((num_parts + 7)/8)
	
#### 3.3 Once the first Member has the list of parts, it will request a particular part with the following message:
	
		PART \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n

		Example:

		PART \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 5 \r\n
		
#### 3.4 The second Member now has two options:
	
##### 3.4.1 Send a message about the transfer of part, followed immediately by the part: note there is no newline after "DATA"
			
		STRT \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n <DATA>

		Example:

		STRT \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 5 \r\n 010010101010101010101010 ... 00001101
			
##### 3.4.2 For some reason the second member does not have this part and replies:
		
		NONE \r\n <FILENAME> \r\n <FULL_FILE_CHECKSUM> \r\n <INTEGER> \r\n

		Example:

		NONE \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 5 \r\n
	
	
### An example connection and transfer might go as follows:

1. 	Open .orch file to get conductor IP, checksums, part sizes, file size etc.

2. 	Member1 connects to the conductor and requests a list of IP:PORT with:
    
		DOWN \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 

3. Conductor replies with the list of IP:PORT:

		SEND \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 2 \r\n 
		1.1.1.1:1234 \r\n 
		192.168.1.1:9999 \r\n 

4. Member1 connects to Member2 at IP:PORT 1.1.1.1:1234 and requests list of parts:

		DOWN \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 
		
5. Member2 replies with list of parts encoded in an int:

		SEND \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 511 \r\n
		
6. Member1 requests a particular part from members2 available parts:

		PART \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 5 \r\n
		
7. Member2 starts the transfer of the requested part:

		STRT \r\n maxresdefault.jpg \r\n 2953289a34e0cc2bf776decc3f8b86622d66b705 \r\n 5 \r\n 010010101010101010101010 ... 00001101
		
8. GOTO 6. Until all parts have been copied. 	(note that in reality there are many members and many simultaneous part requests to different members, but the details of this are implementation specific)
