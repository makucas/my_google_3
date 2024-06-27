from rpyc.utils.server import ThreadedServer
import time
import threading

# nodes
from nodes.master_node import MasterService
from nodes.insert_node import InsertService
from nodes.slave_node import SlaveService
from nodes.slave_node import create_slave

MASTER_PORT = 19811
INSERT_PORT = 19812
DATA_PORT = 19813
SLAVE_PORT = 19814

def start_server(service, port):
    server = ThreadedServer(service, port=port, protocol_config={'allow_public_attrs': True}, auto_register=True)
    print("starting...")
    server.start()

def start_thread(service, port):
    thread = threading.Thread(target=start_server, args=(service, port))
    thread.start()
    return thread

if __name__ == "__main__":
    # Criando dinamicamente os SlaveServices
    slave_services = ["Slave1Service", "Slave2Service", "Slave3Service"]
    slaves = [create_slave(service) for service in slave_services]

    threads = []

    # Iniciar servidor mestre
    threads.append(start_thread(MasterService, MASTER_PORT))
    time.sleep(1)

    # Iniciar servidores escravos
    for i, slave in enumerate(slaves, start=1):
        threads.append(start_thread(slave, SLAVE_PORT + i))
    time.sleep(1)

    # Iniciar o insert node
    threads.append(start_thread(InsertService(replicator_factor=3), INSERT_PORT))

    # Aguardar todas as threads terminarem
    for thread in threads:
        thread.join()

