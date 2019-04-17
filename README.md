Distributed Replication System about a Movie Rating Dataset

>>> Introduction
I implemented a reliable distributed replication system based on the gossip architecture which supports a client to retrieve, update, and submit movie ratings. There are five source files:

1. server0.py
2. server1.py
3. server2.py
4. FE.py
5. client.py

>>> Requirement
This system is implemented and tested on Python 3.7.2 or later version. To run the program, you need install Pyro4 package in your local Python environment. Read https://pythonhosted.org/Pyro4/install.html for further information. Before setup, ensure Pyro4 naming server is created. You can do so by typing "python -m Pyro4.naming" in your command prompt.

>>> Setup
The program consists of 3 backend servers, 1 frontend server, and a client program. Hence, you will need open 5 windows of command prompt, change directory to the parent folder of this text document, and execute by typing "python3 FILENAME". To initialize properly, you'd better execute them in the command prompt in order (backend -> frontend -> client).

>>> Implementation
To allow the simulation of server availability and failure situations, I arbitrarily set the possibility that a server reports itself as active/offline to 95%/5% respectively. Also, whenever the number of update/submit that are pending gossip exceeds 5, the server will report itself as "overloaded". 

This distributed replication system uses lazy ordering to ensure the information stored in each server is up-to-date eventually. The time period that each server pushes its gossip message is set to 5 seconds. This setting is good enough to return an effective response to the clients.

However, in very unlikely cases, for example, one client submits the rating of a movie and another client queries it at the same time. My implementation will overcome this issue by maintaining a pending list, i.e., only the query that is not in pending list will be processed. Otherwise, an error will be printed on client side.

My implementation also takes the accidental shutdown of servers into consideration. Whenever the closed server restarts again, all associated programs will reconnect to it automatically. However, accidental shutdown will lead to data loss, i.e., those data that did not go through gossip process before the server is closed will be lost.