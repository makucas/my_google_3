from rpyc.utils.server import ThreadedServer
import time
import threading
import sys
import signal

from nodes.master_node import MasterService
from nodes.insert_node import InsertService
from nodes.search_node import SearchService
from nodes.slave_node import SlaveService
from nodes.slave_node import create_slave
from nodes.hash_table import HashTableService 

MASTER_PORT = 19811
INSERT_PORT = 19812
DATA_PORT = 19813
HASH_TABLE_PORT = 19814
SEARCH_PORT = 19815
SLAVE_PORT = 19816

def start_server(service, port):
    server = ThreadedServer(service, port=port, protocol_config={'allow_public_attrs': True}, auto_register=True)
    print(f"Starting server on port {port}...")
    server.start()

def start_thread(service, port):
    thread = threading.Thread(target=start_server, args=(service, port))
    thread.daemon = True
    thread.start()
    return thread

def stop_server(signal, frame):
    print("Shutting down...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop_server)
    signal.signal(signal.SIGTERM, stop_server)

    try:
        # slave_services = ["Slave1Service", "Slave2Service", "Slave3Service"]
        # slaves = [create_slave(service) for service in slave_services]

        threads = []

        # Iniciar servidor mestre
        threads.append(start_thread(MasterService, MASTER_PORT))
        time.sleep(1)

        # Iniciar servidores escravos
        # for i, slave in enumerate(slaves, start=1):
        #     threads.append(start_thread(slave(), SLAVE_PORT + i))
        # time.sleep(1)

        threads.append(start_thread(InsertService(), INSERT_PORT))
        threads.append(start_thread(SearchService, SEARCH_PORT))
        threads.append(start_thread(HashTableService, HASH_TABLE_PORT))
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        stop_server(None, None)
