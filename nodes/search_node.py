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

    def exposed_search(self, query):
        """
        Realiza a busca de uma frase em cada chunk de todos os arquivos
        """
        threads = []
        chunk_connection = self.lb.get_nodes()
        responses = []
        lock = threading.Lock()

        for archive in chunk_connection:
            for chunk, connection in chunk_connection[archive].items():
                c = rpyc.connect_by_service(connection, config={'allow_public_attrs': True})
                thread = threading.Thread(target=self.search_to_root, args=(c.root, archive, chunk, query, responses, lock))
                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()

        return responses