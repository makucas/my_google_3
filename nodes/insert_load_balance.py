from rpyc.utils.server import ThreadedServer
import signal
import rpyc
import sys

def stop_server(signal, frame):
    print("Shutting down...")
    sys.exit(0)

class InsertLoadBalancerService(rpyc.Service):
    def __init__(self, replicator_factor):
        self.nodes = []
        self.replicator_factor = replicator_factor
        self.index = 0

        self.conn = rpyc.connect_by_service("CLUSTERMANAGER", config={'allow_public_attrs': True})
        self.exposed_update_nodes()

    def exposed_update_nodes(self):
        """
            Atualiza a lista de nós com base no cluster manager.
        """
        self.nodes = self.conn.root.get_nodes()
        print(f"Updating nodes: {self.nodes}")

    def get_next_node(self):
        """
            Retorna o próximo nó da lista de nós disponíveis.
        """
        node = self.nodes[self.index]
        self.index = self.index + 1
        if self.index == len(self.nodes):
            self.index = 0
        return node

    def exposed_forward_request(self):
        """
            Retorna as conexões contendo os nós de inserção com base
            no fator de réplica.
        """
        connections = []
        for _ in range(self.replicator_factor):
            node = self.get_next_node()
            connections.append(node)
        return connections

if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop_server)
    signal.signal(signal.SIGTERM, stop_server)

    try:
        server = ThreadedServer(InsertLoadBalancerService(replicator_factor=1), port=19902, protocol_config={'allow_public_attrs': True}, auto_register=True)
        print("Starting Insert Load Balancer")
        server.start()

    except KeyboardInterrupt:
        stop_server(None, None)

    except Exception as e:
        print(f"Error when starting Insert Load Balancer: {e}")