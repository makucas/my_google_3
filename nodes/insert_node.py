from modules.load_balancer import LoadBalancer
import threading
import rpyc

class InsertService(rpyc.Service):
    def __init__(self, replicator_factor):
        self.lb = LoadBalancer(replicator_factor)

    def insert_to_root(self, root, root2, archive_name, archive, connection):
        print(f"INSERT: Sending data to {connection}")
        root.insert(archive_name, archive)

        archive_name = archive_name + f"_{connection}"
        root2.add_entry(archive_name)

    def exposed_insert(self, archive_name, archive):
        """
            Pega as conexões com o load balancer e envia os arquivos para inserção
            simultâneamente utilizando threads.
        """
        connections = self.lb.forward_request()

        threads = []
        c2 = rpyc.connect_by_service("HASHTABLE")
        for connection in connections:
            c = rpyc.connect_by_service(connection, config={'allow_public_attrs': True})
            thread = threading.Thread(target=self.insert_to_root, args=(c.root, c2.root, archive_name, archive, connection))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

            



