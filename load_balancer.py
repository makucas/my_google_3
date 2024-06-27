from cluster_manager import Manager
import rpyc

class LoadBalancer():
    def __init__(self, factor):
        self.nodes = []
        self.factor = factor
        self.index = 0

        self.update_nodes()

    def update_nodes(self):
        self.nodes = Manager().return_all_nodes()

    def get_next_node(self):
        node = self.nodes[self.index]
        self.index = self.index + 1
        if self.index == len(self.nodes):
            self.index = 0
        return node

    def forward_request(self):
        print(self.nodes)
        connections = []
        # pegando os próximos nós com base no fator de réplica
        for _ in range(self.factor):
            node = self.get_next_node()
            connections.append(node)
        return connections