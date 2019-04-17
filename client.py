import sys
import threading
import Pyro4

if sys.version_info < (3, 0):
    input = raw_input

def isNaN(s):
    try:
        int(s)
        return False
    except ValueError:
        return True

ns = Pyro4.locateNS()

class Client(object):

    @Pyro4.expose
    def start(self):
        print("Welcome to movie rating dataset!")
        while True:
            try:
                print("--------------------------------")
                print("Select the option to continue: ")
                print("1. Retrieve movie ratings")
                print("2. Update movie ratings")
                print("3. Submit movie ratings")
                print("4. Exit")
                option = int(input("Enter number (1-4): "))
                print("--------------------------------")
                if option == 1:
                    while True:
                        try:
                            print("You can always type 'back' to go back or 'exit' to exit.")
                            name = input("Enter movie title or movie ID: ").strip()
                            if name == "back":
                                break
                            elif name == "exit":
                                sys.exit()
                            if not isNaN(name):
                                name = int(name)
                            result = Pyro4.core.Proxy(ns.lookup("frontend.server")).retrieve(name)
                            if result == False:
                                print(">>> Failed to connect to all backend servers. Please try again later.")
                            elif result == "pending":
                                print(">>> This movie object is unstable. Please try again later.")
                            else:
                                print("Rating: ",result)
                        except Pyro4.errors.CommunicationError:
                            print(">>> Failed to connect to the frontend server. Please try again later.")
                        except Pyro4.errors.NamingError:
                            print(">>> Frontend server does not exist. Please try again later.")
                        except Exception:
                            print(">>> The movie you entered does not exist. Please try again.")
                        finally:
                            break
                elif option == 2:
                    while True:
                        try:
                            print("You can always type 'back' to go back or 'exit' to exit.")
                            name = input("Enter movie title or movie ID: ").strip()
                            if name == "back":
                                break
                            elif name == "exit":
                                sys.exit()
                            if not isNaN(name):
                                name = int(name)
                            rating = float(input("Enter your rating of this movie (0-5): "))
                            if rating < 0 or rating > 5:
                                raise ValueError
                            result = Pyro4.core.Proxy(ns.lookup("frontend.server")).update(name,rating)
                            if result == False:
                                print(">>> Failed to connect to all backend servers. Please try again later.")
                            elif result == "pending":
                                print(">>> This movie object is unstable. Please try again later.")
                            else:
                                print("Your rating is updated successfully.")
                        except Pyro4.errors.CommunicationError:
                            print(">>> Failed to connect to the frontend server. Please try again later.")
                        except Pyro4.errors.NamingError:
                            print(">>> Frontend server does not exist. Please try again later.")
                        except ValueError:
                            print(">>> Invalid rating! Please try again")
                        except Exception:
                            print(">>> The movie you entered does not exist. Please try again.")
                        finally:
                            break
                elif option == 3:
                    while True:
                        try:
                            print("You can always type 'back' to go back or 'exit' to exit.")
                            name = input("Enter movie title: ").strip()
                            if name == "back":
                                break
                            elif name == "exit":
                                sys.exit()
                            rating = float(input("Enter your rating of this movie (0-5): "))
                            if rating < 0 or rating > 5:
                                raise ValueError
                            result = Pyro4.core.Proxy(ns.lookup("frontend.server")).submit(name, rating)
                            if result == False:
                                print(">>> Failed to connect to all backend servers. Please try again later.")
                            elif result == "pending":
                                print(">>> This movie object is unstable. Please try again later.")
                            else:
                                print("Your rating is submitted successfully.")
                        except Pyro4.errors.CommunicationError:
                            print(">>> Failed to connect to the frontend server. Please try again later.")
                        except Pyro4.errors.NamingError:
                            print(">>> Frontend server does not exist. Please try again later.")
                        except ValueError:
                            print(">>> Invalid rating! Please try again")
                        except Exception:
                            print(">>> The movie you entered is already in database. Please start over.")
                        finally:
                            break
                elif option == 4:
                    sys.exit()
                else:
                    raise ValueError
            except ValueError:
                print(">>> Invalid input! Please try again.")

class DaemonThread(threading.Thread):
    def __init__(self, client):
        threading.Thread.__init__(self)
        self.client = client
        self.setDaemon(True)

    def run(self):
        with Pyro4.core.Daemon() as daemon:
            daemon.register(self.client)
            daemon.requestLoop()

client = Client()
daemonthread = DaemonThread(client)
daemonthread.start()
client.start()
