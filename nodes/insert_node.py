from load_balancer import LoadBalancer
import rpyc

class InsertService(rpyc.Service):
    def __init__(self, replicator_factor):
        self.lb = LoadBalancer(replicator_factor)

    def exposed_insert(self, archive_name, archive):
        connections = self.lb.forward_request()
        
        # c = []
        # for connection in connections:
        #     c.append(rpyc.connect_by_service(connection))

        # for i in range(len(c)):
        #     c[i].root.insert(archive_name, archive)

        c = rpyc.connect_by_service(connections[0], config={'allow_public_attrs': True})
        c.root.insert(archive_name, archive)

            



