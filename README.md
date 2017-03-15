# Semester 2 Computer Networks Project
## Université Jean-Monnet, Saint-Étienne, France.
## Peer to Peer file sharing

# Contents
* Description of the subject
* Instructions
* Architecture


## Subject

The objective of this project was to create a peer-to-peer filesharing program with the following features.

### Composer
A small utility program that will take an input Composition (file) and generate a small Orchestra (.orch) file that can be used to share the large input file. An example orchestra file is as follows:

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

### Conducter
A networked program that provides IP:ports of users that are sharing parts of the Composition.

### Member 
The core program that is used to get/share the Composition this required the .orch file to know detail of what composition to share/download.

## Intructions for using the program

### Dependancies
The software is tested to run on Python 3.5, it may work on Python 2.7 but this has not been test.
The following depenancy is included in the GitHub repository progressbar2-3.12.0-py2.py3-none-any.whl , this can be installed with pip or any other wheel installation package, be sure that it is installing to python3 as some system default to python2. Note the .whl file is cross-platform and has been tested on Window, Linux (Ubuntu) and Mac OSX.

For installation, simply clone the GitHub repository. Note there is a .gitignore on a directory called "logs" so you will need to make this directory yourself otherwise there will be an error writing the log file.

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

To use the member to share/get a file run member.py orch_filename the member will then get the file. A small progress bar is shown with the parts remaining and percentage, you can press q at any time to quit (this may take up to 5 seconds to cleaning close all conections)
![Alt text](torrentNchill/screenshots/instructions_member.png?raw=true "Using the Member")


## System Architecture


![Alt text](torrentNchill/screenshots/architecture.gif?raw=true "Example of connecting and sharing a part")

