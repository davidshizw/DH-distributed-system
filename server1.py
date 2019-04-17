import Pyro4
import threading
import random

movies = {}

with open("MovieLens/movies.csv","r") as moviesFile:
    moviesFile.readline()
    for line in moviesFile:
        arr = line.split(",")
        if len(arr) == 3:
            movies[int(arr[0])] = {"name": arr[1], "user": 0, "ts": [0,0,0]}
        else:
            name = ""
            for i in range(1,len(arr)-1):
                name = arr[i] + ","
            movies[int(arr[0])] = {"name": name[1:-2], "user": 0, "ts": [0,0,0]}

with open("MovieLens/ratings.csv","r") as ratingsFile:
    ratingsFile.readline()
    for line in ratingsFile:
        arr = line.split(",")
        id = int(arr[1])
        if len(movies[id]) == 4:
            movies[id]["rating"] = (movies[id]["rating"] * movies[id]["user"] + float(arr[2])) / (movies[id]["user"]+1)
        else:
            movies[id]["rating"] = float(arr[2])
        movies[id]["user"] += 1

def tsChecker(id,ts):
    if id not in movies.keys():
        return False
    valueTS = movies[id]["ts"]
    for i in range(0,3):
        if valueTS[i] < ts[i]:
            return False
    return True

ns = Pyro4.locateNS()

@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class BackendServer(object):
    def __init__(self):
        self.SERVER_NO = 1
        self.pending_gossip = 0
        self.executed_operation_table = []
        self.replica_timestamp = [0,0,0]
        self.timestamp_table = [[0,0,0],self.replica_timestamp,[0,0,0]]
        self.log = {}
        self.state = "active"
        self.gossip_thread = None
        self.gossip()

    def retrieve(self,id,ts):
        if tsChecker(id,ts):
            return movies[id]
        else:
            return False

    def update(self,operationID,id,rating,ts):
        if len(movies[id]) == 4:
            movies[id]["rating"] = (movies[id]["rating"] * movies[id]["user"] + rating) / (movies[id]["user"] + 1)
        else:
            movies[id]["rating"] = rating
        movies[id]["user"] += 1
        self.replica_timestamp[self.SERVER_NO] += 1
        movies[id]["ts"][self.SERVER_NO] = self.replica_timestamp[self.SERVER_NO]
        self.executed_operation_table.append(operationID)
        self.log[self.SERVER_NO, self.replica_timestamp[self.SERVER_NO]] = {"SERVER_NO": self.SERVER_NO, "ts": movies[id]["ts"].copy(), "id": id, "rating": rating, "prev": ts.copy(), "operationID": operationID}
        self.pending_gossip += 1
        return movies[id]

    def submit(self,operationID,movieID,name,rating):
        movies[movieID] = {"name": name, "user": 1, "rating": rating, "ts": [0,0,0]}
        self.replica_timestamp[self.SERVER_NO] += 1
        movies[movieID]["ts"][self.SERVER_NO] = self.replica_timestamp[self.SERVER_NO]
        self.executed_operation_table.append(operationID)
        self.log[self.SERVER_NO, self.replica_timestamp[self.SERVER_NO]] = {"SERVER_NO": self.SERVER_NO, "ts": movies[movieID]["ts"].copy(), "id": movieID, "name": name, "rating": rating, "prev": [0,0,0], "operationID": operationID}
        self.pending_gossip += 1
        return movies[movieID]

    def push(self):
        self.pending_gossip = 0
        for i in ns.list(prefix="backend.server").keys():
            if str(self.SERVER_NO) not in i:
                try:
                    gossip_server = Pyro4.core.Proxy("PYRONAME:"+i)
                    if gossip_server.getState() != "offline":
                        gossip_server.pull(self.SERVER_NO, self.log, self.replica_timestamp)
                        print("Gossip message sent to " + i + " successfully")
                    else:
                        print(i + " is offline.")
                except Exception:
                    print("Failed to connect to " + i)

    def pull(self,server_no,log,replica_ts):
        self.timestamp_table[server_no] = replica_ts
        for j in range(self.replica_timestamp[server_no]+1,replica_ts[server_no]+1):
            update = log[server_no,j]
            operationID = update["operationID"]
            if operationID not in self.executed_operation_table:
                id = update["id"]
                rating = update["rating"]
                ts = update["ts"]
                if id in movies.keys():
                    if len(movies[id]) == 4:
                        movies[id]["rating"] = (movies[id]["rating"] * movies[id]["user"] + rating) / (movies[id]["user"] + 1)
                    else:
                        movies[id]["rating"] = rating
                    movies[id]["user"] += 1
                    self.log[server_no, j] = {"SERVER_NO": server_no, "ts": ts.copy(), "id": id, "rating": rating, "prev": update["prev"].copy(), "operationID": operationID}
                else:
                    movies[id] = {"name": update["name"], "user": 1, "rating": rating, "ts": ts}
                    self.log[server_no, j] = {"SERVER_NO": server_no, "ts": ts.copy(), "id": id, "rating": rating, "name": update["name"], "prev": update["prev"].copy(), "operationID": operationID}
                movies[id]["ts"][server_no] = ts[server_no]
                self.executed_operation_table.append(operationID)
        self.replica_timestamp[server_no] = replica_ts[server_no]


    def getState(self):
        return self.state

    def getRandomState(self):
        if self.pending_gossip > 5:
            self.state = "overloaded"
            return "overloaded"
        if random.random() < 0.95:
            if not self.gossip_thread.is_alive():
                self.gossip()
            self.state = "active"
            return "active"
        else:
            self.gossip_thread.cancel()
            print("server 1 stopped pushing\n")
            self.state = "offline"
            return "offline"

    def gossip(self):
        self.push()
        print()
        self.gossip_thread = threading.Timer(5.0, self.gossip)
        self.gossip_thread.start()

server = BackendServer()

Pyro4.Daemon.serveSimple({
    server: "backend.server1"
})
