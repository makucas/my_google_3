import signal
import rpyc
import time
import threading
import sys

from rpyc.utils.server import ThreadedServer

def stop_server(signal, frame):
    print("Shutting down...")
    sys.exit(0)

class ClusterManagerService(rpyc.Service):
    def __init__(self, timeout=4):
        self.timeout = timeout
        self.nodes = {}

        self.lock = threading.Lock()
        threading.Thread(target=self.check_inactive_nodes, daemon=True).start()

    def update_load_balancer(name):
        """
            Se conecta nos respectivos load balancer, atualiza os nós dos mesmos
            e encerra a conexão.
        """
        conn = rpyc.connect_by_service("INSERTLOADBALANCER", config={'allow_public_attrs': True})
        conn.root.update_nodes()
        conn.close()
    
        #ADICIONAR O LOAD BALANCER DE PESQUISA

    def exposed_notify_alive(self, node_name):
        notify = False
        """
            Esse método é utilizado pelos slaves para se registrarem
            no monitoramento do cluster manager ou notificar que ainda estão vivos.

            Quando um novo nó se registra, o cluster manager atualiza a lista de nós
            do load balancer.
        """
        with self.lock:
            if node_name not in self.nodes:
                print(f"Node {node_name} is registering")
                notify = True
            else:
                print(f"Node {node_name} notified it's alive.")
            self.nodes[node_name] = time.time()

        if notify:
            self.update_load_balancer()

    def exposed_get_nodes(self):
        """
            Retorna todos os nós ativos do Cluster.
        """
        try:
            with self.lock:
                node_names = [node.replace("Service", "") for node in self.nodes.keys()]
        except Exception as e:
            print(f"ERROR: Na função exposed_get_nodes -- {e}")

        return node_names

    def check_inactive_nodes(self):
        """
            Verifica quais nós estão inativos e deleta os mesmos da lista de nós
            do Cluster.

            Quando um nó é deletado, o cluster manager atualiza a lista de nós
            do load balancer.
        """
        while True:
            time.sleep(2)  # Check every 5 seconds
            current_time = time.time()
            update = False

            print(f"current time: {current_time}")
            print(f"self nodes: {self.nodes}")
            print()

            with self.lock:
                inactive_nodes = [node for node, last_time in self.nodes.items() \
                                  if current_time - last_time > self.timeout]
                if inactive_nodes:
                    for node in inactive_nodes:
                        del self.nodes[node]
                        print(f"Node {node} has been removed due to inactivity.")
                    update = True # set the update flag
            if update:
                print(f"Updating load balancer")
                self.update_load_balancer()
                update = False

if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop_server)
    signal.signal(signal.SIGTERM, stop_server)

    try:
        server = ThreadedServer(ClusterManagerService(), port=19901, protocol_config={'allow_public_attrs': True}, auto_register=True)
        print("Starting Cluster Manager")
        server.start()

    except KeyboardInterrupt:
        stop_server(None, None)

    except Exception as e:
        print(f"Error when starting Cluster Manager: {e}")


