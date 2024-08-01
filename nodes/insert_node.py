from modules.load_balancer import LoadBalancer
import threading
import rpyc

class InsertService(rpyc.Service):
    def __init__(self):
        self.lb = rpyc.connect_by_service("INSERTLOADBALANCER", config={'allow_public_attrs': True})
        #self.lb = LoadBalancer(replicator_factor)

    def insert_to_root(self, root, root2, archive_name, archive, connection):
        print(f"INSERT: Sending data to {connection}")
        root.insert(archive_name, archive)
        archive_name = archive_name + f"_{connection}"
        root2.add_entry(archive_name)

    def exposed_insert_in_connections(self, connections, archive_name, archive):        
        errors = []
        threads = []
        c2 = rpyc.connect_by_service("HASHTABLE")
        for connection in connections:
            try:
                c = rpyc.connect_by_service(connection, config={'allow_public_attrs': True})
            except Exception as e:
                print(f"INSERT NODE: {e}")
                errors.append(connection)
                continue
            thread = threading.Thread(target=self.insert_to_root, args=(c.root, c2.root, archive_name, archive, connection))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        return errors

    def exposed_insert(self, archive_name, archive):
        """
            Pega as conexões com o load balancer e envia os arquivos para inserção
            simultâneamente utilizando threads.
        """
        connections = self.lb.root.forward_request()

        errors = self.exposed_insert_in_connections(connections, archive_name, archive)

        if errors:
            print(f"Retrying for {(archive_name.split('_'))[1]}")
            n_connections_failed = len(errors)
            connections = self.lb.root.forward_request(replicator_factor=n_connections_failed)
            errors = self.exposed_insert_in_connections(connections, archive_name, archive)
            
            if errors:
                raise Exception("Fatality")