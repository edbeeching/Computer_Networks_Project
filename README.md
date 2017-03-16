# Semester 2 Computer Networks Project
## Université Jean-Monnet, Saint-Étienne, France.
## Peer to Peer file sharing

# Contents
* Description of the subject
* Instructions
* Architecture
* Acknowledgements

## Subject

The objective of this project was to create a peer-to-peer filesharing program with the following features.
This is the collaborative work of 4 students: Jorge Chang, Sejal Jaiswal, Arslen Remaci and Edward Beeching.

### Composer
A small utility program that will take an input Composition (file) and generate a small Orchestra (.orch) file that can be used to share the large input file. An example orchestra file is as follows:

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

### Conducter
A networked program that provides IP:ports of users that are sharing parts of the Composition.

### Member 
The core program that is used to get/share the Composition, this requires the .orch file to know detail of what composition to share/download.

## Instructions for using the program

### Dependancies
The software is tested to run on Python 3.5, it may work on Python 2.7 but this has not been tested.
The following dependancy is included in the GitHub repository **progressbar2-3.12.0-py2.py3-none-any.whl** , this can be installed with pip or any other wheel installation package, be sure that it is installing to python3 as some systems default to python2. Note the .whl file is cross-platform and has been tested on Window, Linux (Ubuntu) and Mac OSX.

For installation, simply clone the GitHub repository. **Note there is a .gitignore on a directory called "logs" so you will need to make this directory yourself otherwise there will be an error writing the log file.**

### Using the Composer
In order to share a new file the composer can be used from the command line.

There are 3 options:

    composer.py filename
    composer.py filename conductor_ip_port
    composer.py filename conductor_ip_port part_size

If the ip and port are note know, the file line of the text file generated can be updated at a later stage.
![Alt text](torrentNchill/screenshots/instructions_composer.png?raw=true "Using the Composer")
### Using the Conductor

The conductor can be run from the command line with conductor.py as new members connect the conductor will maintain a list of IP:port of connected members.


### Using the Member

To use the member to share/get a file run **member.py orch_filename** the member will then get the file. A small progress bar is shown with the parts remaining and percentage, you can press q at any time to quit (this may take up to 5 seconds to cleaning close all conections)
![Alt text](torrentNchill/screenshots/instructions_member.png?raw=true "Using the Member")


## System Architecture
### Simple UML Diagram of Member

Classes commiunicate through messages on blocking queues. The use of queues always for easy testing of individual classes and functions.
![Alt text](torrentNchill/screenshots/Simple_UML.png?raw=true "A simplfied UML diagram")

### Example animation of Member architecture
Below is a simplified animation of the Member instantiation, connection and sharing one part.
![Alt text](torrentNchill/screenshots/architecture.gif?raw=true "Example of connecting and sharing a part")

## Acknowledgements
We would like to mention the following:
http://home.wlu.edu/~levys/software/kbhit.py for the keyboard interupt class.

Remi Emonet for the **netutils** function

All other code is written by the Authors of this project.




