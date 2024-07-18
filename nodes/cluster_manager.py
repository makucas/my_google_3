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
    def __init__(self, timeout=60):
        self.timeout = timeout
        self.nodes = {}

        self.lock = threading.Lock()
        threading.Thread(target=self.check_inactive_nodes, daemon=True).start()

    def exposed_notify_alive(self, node_name):
        with self.lock:
            self.nodes[node_name] = time.time()
        print(f"Node {node_name} notified it's alive.")

    def exposed_get_nodes(self):
        """
            Retorna todos os nós ativos do Cluster.
        """
        with self.lock:
            node_names = [node.replace("Service", "") for node in self.nodes.keys()]
        return node_names

    def check_inactive_nodes(self):
        """
            Verifica quais nós estão inativos e deleta os mesmos da lista de nós
            do Cluster.
        """
        while True:
            time.sleep(5)  # Check every 5 seconds
            current_time = time.time()

            print(f"current time: {current_time}")
            print(f"self nodes: {self.nodes}")
            print()

            with self.lock:
                inactive_nodes = [node for node, last_time in self.nodes.items() \
                                  if current_time - last_time > self.timeout]
                for node in inactive_nodes:
                    del self.nodes[node]
                    print(f"Node {node} has been removed due to inactivity.")
    
if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop_server)
    signal.signal(signal.SIGTERM, stop_server)

    try:
        server = ThreadedServer(ClusterManagerService(), port=19901, protocol_config={'allow_public_attrs': True}, auto_register=True)
        print("Strating Cluster Manager")
        server.start()

    except KeyboardInterrupt:
        stop_server(None, None)

    except Exception as e:
        print(f"Error when strating Cluster Manager: {e}")


