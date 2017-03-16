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
* Sejal Jaiswal, NA
* Arslen Remaci, NA

## Division of work
We tried to find a division of the system in modules that minimized dependencies, though this proved to be challenging due to relationship between the modules. We divided the work in the following:
* Protocol specification and architecture, by Edward Beeching and Jorge Chong
* Utilities and Composer, by Edward Beeching
* Member, by Edward Beeching
* Connection, by Jorge Chong
* File handler, by Sejal Jaiswal
* Conductor, by Arslen Remaci

## Good Practices
We followed most of the recommended good practices for software development, though we didn't work in unit testing, and we think this should be included in a production development environment for the next iteration of the product. 

We found debugging and testing multi-threaded network software particularly tricky, specially for the asynchronous nature of queue handling and our choice of using independent threads for receiving and sending, among other threads (connection handler, file handler, member main thread). That motivated us to log to files for testing and debugging.
