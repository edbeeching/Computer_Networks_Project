# Semester 2 Computer Networks Project
## Université Jean-Monnet, Saint-Étienne, France.
## Peer to Peer file sharing

# Contents
* Time spent by each member of the project
* Division of work
* Good Practices

## Time spent
The total workload for the team is:
* Edward Beeching, 30 h
* Jorge Chong, 20 h
* Sejal Jaiswal, 20 h
* Arslen Remaci, 10 h

## Division of work
We tried to find a division of the system in modules that minimized dependencies, though this provided to be difficult. We divided the work in the following:
* Protocol specification and architecture, by Edward Beeching and Jorge Chong
* Utilities and Composer, by Edward Beeching
* Member, by Edward Beeching
* Connection, by Jorge Chong
* File handler, by Sejal Jaiswal
* Conductor, by Arslen Remaci

## Good Practices
We followed most of the recommended good practices for software development, though we didn't work using unit testing and automated tests. This will be included in a production development environment for the next iteration of the product as well as an agile methodology for software development.

For the implementation in Python language, we used an object oriented design without using global variables. Modules have a single point of dependency in the form of queue messages. We agreed upon the type of messages (format an semantics) in the planning phase in order to work independently in each module.

We found debugging and testing multi-threaded network software particularly tricky, specially due to the asynchronous nature of queue handling and our choice of using independent threads for receiving, connection handler, file handler, and the member main thread). For this reason we used logging to files for debugging and testing.

As for the protocol design we followed simplicity and efficiency guidelines, as well as minimization of dependencies. This can be noted in the use of <FILENAME> and <CHECKSUM> in each message, making each message self-contained. Efficiency in network and processing resources were also considered, for example the list of parts is a bit map.
