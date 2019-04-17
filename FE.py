import sys
import Pyro4
import threading
import uuid
import random

movies = []

with open("MovieLens/movies.csv","r") as moviesFile:
    moviesFile.readline()
    for line in moviesFile:
        arr = line.split(",")
        if len(arr) == 3:
            movies.append({"name": arr[1],"id": int(arr[0]),"ts":[0,0,0]})
        else:
            name = ""
            for i in range(1,len(arr)-1):
                name += arr[i] + ","
            movies.append({"name": name[1:-2], "id": int(arr[0]), "ts": [0,0,0]})

def movieCheck(keyword):
    if isinstance(keyword,int):
        for i in movies:
            if i["id"] == keyword:
                return i
        return None
    else:
        for i in movies:
            if i["name"] == keyword:
                return i
        return None

class Error(Exception):
    pass

class MovieNotFoundException(Error):
    pass

class MovieAlreadyExistException(Error):
    pass

ns = Pyro4.locateNS()

@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class FrontendServer(object):

    def __init__(self):
        self.pending = []

    def retrieve(self, name):
        this = movieCheck(name)
        if name in self.pending:
            return "pending"
        elif this != None :
            backend = ns.list(prefix="backend.server").keys()
            tried = []
            while True:
                server_no = random.randint(0,len(backend)-1)
                if len(tried) == len(backend):
                    return False
                elif server_no in tried:
                    continue
                try:
                    print("I am sending retrieve request to " + backend[server_no])
                    server = Pyro4.core.Proxy("PYRONAME:"+backend[server_no])
                    status = server.getRandomState()
                    if status == "active":
                        result = server.retrieve(this["id"],this["ts"])
                        if result == False:
                            print(backend[server_no] + " is outdated")
                            tried.append(server_no)
                        else:
                            this["ts"] = result["ts"]
                            return result["rating"]
                    elif status == "overloaded":
                        print(backend[server_no] + " is overloaded.")
                        tried.append(server_no)
                    else:
                        print(backend[server_no] + " is offline.")
                        tried.append(server_no)
                except Pyro4.errors.CommunicationError:
                    print("Failed to connect to " + backend[server_no])
                    tried.append(server_no)
        else:
            raise MovieNotFoundException

    def update(self, name, rating):
        this = movieCheck(name)
        if name in self.pending:
            return "pending"
        if this != None:
            operationID = uuid.uuid4()
            self.pending.append(this["id"])
            self.pending.append(this["name"])
            backend = ns.list(prefix="backend.server").keys()
            tried = []
            while True:
                server_no = random.randint(0,len(backend)-1)
                if len(tried) == len(backend):
                    self.pending.remove(this["id"])
                    self.pending.remove(this["name"])
                    return False
                elif server_no in tried:
                    continue
                try:
                    print("I am sending update request to " + backend[server_no])
                    server = Pyro4.core.Proxy("PYRONAME:"+backend[server_no])
                    status = server.getRandomState()
                    if status == "active":
                        result = server.update(operationID,this["id"],rating,this["ts"])
                        this["ts"] = result["ts"]
                        self.pending.remove(this["id"])
                        self.pending.remove(this["name"])
                        return
                    elif status == "overloaded":
                        print(backend[server_no] + " is overloaded.")
                        tried.append(server_no)
                    else:
                        print(backend[server_no] + " is offline.")
                        tried.append(server_no)
                except Pyro4.errors.CommunicationError:
                    print("Failed to connect to " + backend[server_no])
                    tried.append(server_no)
        else:
            raise MovieNotFoundException

    def submit(self, name, rating):
        if movieCheck(name) == None:
            if name in self.pending:
                return "pending"
            self.pending.append(name)
            backend = ns.list(prefix="backend.server").keys()
            tried = []
            while True:
                server_no = random.randint(0,len(backend)-1)
                if len(tried) == len(backend):
                    self.pending.remove(name)
                    return False
                elif server_no in tried:
                    continue
                try:
                    operationID = uuid.uuid4()
                    movieID = random.randint(0,1000000)
                    while True:
                        if movieCheck(movieID) == None:
                            break
                        movieID = random.randint(0,1000000)
                    print("I am sending submit request to " + backend[server_no])
                    server = Pyro4.core.Proxy("PYRONAME:"+backend[server_no])
                    status = server.getRandomState()
                    if status == "active":
                        result = server.submit(operationID, movieID, name, rating)
                        result["id"] = movieID
                        movies.append(result)
                        self.pending.remove(name)
                        return
                    elif status == "overloaded":
                        print(backend[server_no] + " is overloaded.")
                        tried.append(server_no)
                    else:
                        print(backend[server_no] + " is offline.")
                        tried.append(server_no)
                except Pyro4.errors.CommunicationError:
                    print("Failed to connect to " + backend[server_no])
                    tried.append(server_no)
        else:
            raise MovieAlreadyExistException

server = FrontendServer()

Pyro4.Daemon.serveSimple({
    server: "frontend.server"
})
