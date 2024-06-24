from rpyc.utils.server import ThreadedServer
import threading

# nodes
from nodes.master_node import MasterService
from nodes.slave_node import SlaveService
from nodes.slave_node import create_slave

MASTER_PORT = 18812
DATA_PORT = 18813
SLAVE_PORT = 18814

def start_server(service, port):
    server = ThreadedServer(service, port=port, auto_register=True)
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

    # Iniciar servidores escravos
    for i, slave in enumerate(slaves, start=1):
        threads.append(start_thread(slave, SLAVE_PORT + i))

    # Aguardar todas as threads terminarem
    for thread in threads:
        thread.join()

