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

        connection = connections[0]

        c = rpyc.connect_by_service(connection, config={'allow_public_attrs': True})
        c.root.insert(archive_name, archive)

        c2 = rpyc.connect_by_service("HASHTABLE")
        archive_name = archive_name + f"_{connection}"
        c2.root.add_entry(archive_name)

            



