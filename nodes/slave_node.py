import ast
import rpyc
import json
import os
import threading
import signal
import rpyc
import time, sys
import threading
from rpyc.utils.server import ThreadedServer
import time
#from services.search import Searcher

class SlaveService(rpyc.Service):
    def __init__(self):
        self.path = "../data"
        self.alive = True
        self.notification_interval = 2
        threading.Thread(target=self.notify_cluster_manager, daemon=True).start()

        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def exposed_insert(self, archive_name, archive):
        print(f"data: saving {archive_name}")

        file_names = archive_name.split("_")
        file_path = f"{self.path}/{file_names[0]}/{file_names[1]}.txt"

        if not os.path.exists(f"{self.path}/{file_names[0]}"):
            os.makedirs(f"{self.path}/{file_names[0]}")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(str(archive))

        print("SLAVE: finished")
    
    def exposed_search(self, archive, chunk, query):
        results = []
        with open(f"{self.path}/{archive}/{chunk}.txt", "r", encoding="utf-8") as f:
            archive_content = f.read()

            data = ast.literal_eval(archive_content)

            for item in data:
                if item["maintext"] and query.lower() in item.get('maintext').lower():
                    results.append(item["title"])       
        return results
    
    def notify_cluster_manager(self):
        while self.alive:
            try:
                conn = rpyc.connect_by_service("CLUSTERMANAGER", config={'allow_public_attrs': True})
                conn.root.notify_alive(self.__class__.__name__)
                conn.close()
                print(f"Notification sent to cluster manager")
            except Exception as e:
                print(f"Failed to notify cluster manager: {e}")
            
            time.sleep(self.notification_interval)
        
def create_slave(name):
    return type(name, (SlaveService,), {})

def stop_server(signal, frame):
    print("Shutting down...")
    sys.exit(0)

if __name__ == "__main__":
    name= "1"
    slave_name = f"Slave{name}Service"
    slave_port = 19817

    signal.signal(signal.SIGINT, stop_server)
    signal.signal(signal.SIGTERM, stop_server)

    slave = create_slave(slave_name) 
    
    for i in range(1):
        try:
            server = ThreadedServer(slave(), port=slave_port, protocol_config={'allow_public_attrs': True}, auto_register=True)
            print(f"Starting Slave {slave_name} in port {slave_port}")
            server.start()
            break

        except KeyboardInterrupt:
            stop_server(None, None)
            break

        except Exception as e:
            if "Address already in use" in str(e):
                slave_port+=1
                continue
            else:
                print(f"Error when starting Slave: {e}")
                break
