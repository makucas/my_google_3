import rpyc
import json
import os
#from services.search import Searcher

class SlaveService(rpyc.Service):
    def __init__(self):
        if not os.path.exists("data"):
            os.makedirs("data")

    def exposed_insert(self, archive_name, archive):
        print(f"data: saving {archive_name}")

        file_names = archive_name.split("_")
        file_path = f"data/{file_names[0]}/{file_names[1]}.json"

        if not os.path.exists(f"data/{file_names[0]}"):
            os.makedirs(f"data/{file_names[0]}")

        with open(file_path, "w") as f:
            json.dump(archive, f)

        print("SLAVE: finished")
        
def create_slave(name):
    return type(name, (SlaveService,), {})