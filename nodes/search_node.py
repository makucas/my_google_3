import threading
from modules.search_load_balancer import SearchLoadBalancer
import rpyc

class SearchService(rpyc.Service):
    def __init__(self):
        self.lb = SearchLoadBalancer()

    def search_to_root(self, root, archive, chunk, query, responses, lock):
        print(f"INSERT: Searching query in chunk {chunk} of archive {archive}")
        response = root.search(archive, chunk, query)
        with lock:
            responses.append(response)

    def call_search(self, query, responses, chunk_connection):
        lock = threading.Lock()

        threads = []
        errors = []

        for archive in chunk_connection:
            for chunk, connection in chunk_connection[archive].items():
                try:
                    c = rpyc.connect_by_service(connection, config={'allow_public_attrs': True, 'sync_request_timeout': None})
                except Exception as e:
                    print(f"SEARCH NODE: {e}")
                    errors.append([archive, chunk, connection])
                    continue
                thread = threading.Thread(target=self.search_to_root, args=(c.root, archive, chunk, query, responses, lock))
                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()
        
        return responses, errors

    def exposed_search(self, query):
        """
        Realiza a busca de uma frase em cada chunk de todos os arquivos
        """
        max_retries=3
        errors = []
        chunk_connection = self.lb.get_nodes()
        responses = []
        retries = 0

        while retries < max_retries:
            responses, errors = self.call_search(query, responses, chunk_connection)
            if not errors:
                break
            else:
                # Prepare chunk_connection for the next retry with only the error chunks
                chunk_connection = {}
                for i in errors:
                    archive, chunk, connection = i[0], i[1], i[2]
                    chunk_connection[archive] = {}
                    new_node = self.lb.get_next_node_machine(archive, chunk, connection)
                    chunk_connection[archive][chunk] = new_node	
                retries += 1

        if errors:
            print(f"SEARCH NODE: Failed to search in some chunks after {max_retries} retries: {errors}")

        return responses