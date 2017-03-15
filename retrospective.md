<!---
Contents: about organization of the work, technology, etc
* compared to your objectives: 
  - what works
  - what does not work yet
* the difficulties you faced
* Learnings:
  - the lessons you learned
  - what you would keep for next time, what you would do differently
-->

# Group D torrentNchill Retrospective
## Documentation of the issues, solutions and future work related to the planning and implementation of the project.

### 1. Technology
* We used Python Version 3.5 to implement the project. Familiarity of the language to most of the members was the primary reason for choosing Python. Also the exercises and examples during the Computer Network lab session provided a good direction towards threading and network programming in general. 
* We also used PyQT5 for building the GUI for the project.

### 2. Objective
* To draft a protocol that provides sufficient details such that a reader can read and implement a peer that is compatible with ours.
* To understand, structure and implement a P2P system that follows the drafted protocol. 
* Using the TCP sockets.
* The information to be transferred can be in the form of mixed text/binary data, need to handle any kind of data.
* Proper usage of GitHub for version control and project delivery.
* Create a GUI for the software.
* Proper and multiple testing of the software built. Also perform independent testing of each module. Test against 'bad behavior' from other peers, various stress tests.
* The software that is built must be cross-platform and cross-language compatible. Cross-language compatibility has to be checked within the meta-groups.

#### 2.1 Goals achieved
* We were able to draft a protocol in collaboration with the members of other groups in the meta-group formed. The protocol defines the way we connect to other peers, exchange and meaning of the messages, transfer and sharing of files, etc. 
* We were able to implement a P2P system that followed the protocol specified. 
* Used TCP sockets for establishing communication with the goal of transferring and exchanging information. We used the 'socket', an in-built Python module for the same.
* The software built is able to handle any various kind of data format. Also, files or fiel parts are handled differently: could be loaded from memory or the disc depending on its usage history.
* GitHub was used to frequently push changes made by the members and to keep the project synced with all members, whether working on the same module or different, so that the modules are all compatible and functioning well.
* The software has been tested multiple times. Each member was responsible for making sure his/her module worked error-free independently and for helping solve any error that arised due to his/her changes pushed on to the main branch. 
 For testing against various 'bad behavior' that could arise due to a faulty connection or file-sharing with other peers, proper exception throw and catch mechanism have been implemented and handled wherever possible.
* The software was tested for cross-platform compatibility between Linux, Windows and MacOS. The tests hinted towards some OS dependent modules and these were changed to a more platform robust module.
 Cross-language compatibility test was doing in collaboration with Group E, who are using Java. The system was able to send and receive files in accordance to the protocol described and followed by both the teams.
 
### 3. Challenges faced
* We found thread synchronization and specially it's debugging to be difficult.
* A lot of time was spent to research about other asynchronous concurrent models like coroutines.
* The modules for the software are very corelated and hence it was difficult to design and structure modules to work as independently as possible. 
* We tried to design a GUI for the software. A lot of time was spent on trying to set up and use PyQt5, and the various ways to be able to intercept the various threads for the software from the UI calls. 
* It was sometimes difficult to debug problems because we were not sure if the bug was due to wrong programming or due to platform incompatibility. Many modules had to be tested, re-tested and checked so as to be platform independent. This again needed some research work to be done and took up time.

#### 4. Lessons learnt 

##### 4.1 Learnings
* Usage of thread safe queues to solve the problem of thread synchronization. We used this to solve thread synchronisation problems.
* Usage of queue messages to remove strong dependencies of various modules. This helps to implement modules separately and be able to test them independent of each other.
* Good design of the protocol and it's proper implementation helped in dealing with corner/stress cases. This also helped with cross-language testing with the other team.
* Realised the importance of good development practises and the flexibility, understanding it provides specially when multiple people are working on the same module/code.

#### 4.2 Until next time (What can be done differently?)
* We programmed the software using multi-threads rather than multi-processing. Maybe it is a better idea to use multi-programming for this kind of situation. Using multiprocessing would be an easier and better way to get around the Global Interpreter Lock so as to not write on the same memory space, unlike in multi-threading.
* Follow up with a fully functional GUI to be able to present the software better to a person with limited knowledge of networking.
