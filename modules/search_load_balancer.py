import rpyc

class SearchLoadBalancer():
    def __init__(self):
        self.nodes = []

        self.conn = rpyc.connect_by_service("CLUSTERMANAGER", config={'allow_public_attrs': True})
        self.hash_table = rpyc.connect_by_service("HASHTABLE", config={'allow_public_attrs': True})

        self.update_nodes()

    def update_nodes(self):
        self.nodes = self.conn.root.get_nodes()

    def get_nodes(self):
        nodes = [i.lower() for i in self.nodes]        
        machine_per_chunk = {}
        archives_chunks = self.hash_table.root.get_all_chunks()

        for archive in archives_chunks:
            machine_per_chunk[archive] = {}
            for chunk in archives_chunks[archive]:
                machines = self.hash_table.root.get_chunk_location(archive, chunk)
                for machine in machines:
                    if machine.lower() in nodes:
                        machine_per_chunk[archive][chunk] = machine

                if not chunk in machine_per_chunk[archive]:
                    print(f"No avaiable machines for chunk {chunk} of archive {archive}")
        
        return machine_per_chunk