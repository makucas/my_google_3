import ast
import rpyc
import json
import os
import threading
import time
#from services.search import Searcher

class SlaveService(rpyc.Service):
    def __init__(self):
        self.alive = True
        self.notification_interval = 50
        threading.Thread(target=self.notify_cluster_manager, daemon=True).start()

        if not os.path.exists("data"):
            os.makedirs("data")

    def exposed_insert(self, archive_name, archive):
        print(f"data: saving {archive_name}")

        file_names = archive_name.split("_")
        file_path = f"data/{file_names[0]}/{file_names[1]}.txt"

        if not os.path.exists(f"data/{file_names[0]}"):
            os.makedirs(f"data/{file_names[0]}")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(str(archive))

        print("SLAVE: finished")

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
    
    def exposed_search(self, archive, chunk, query):
        results = []
        with open(f"data/{archive}/{chunk}.txt", "r", encoding="utf-8") as f:
            archive_content = f.read()

            data = ast.literal_eval(archive_content)

            for item in data:
                if item["maintext"] and query.lower() in item.get('maintext').lower():
                    results.append(item["title"])       
        return results
        
def create_slave(name):
    return type(name, (SlaveService,), {})

