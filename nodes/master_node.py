import rpyc

class MasterService(rpyc.Service):
    def __init__(self):
        pass

    def on_connect(self, conn):
        pass

    def on_disconnect(self, conn):
        pass

    def exposed_insert(self, archive_name, archive):
        c = rpyc.connect_by_service("INSERT", config={'allow_public_attrs': True})
        c.root.insert(archive_name, archive)
    
    def exposed_search(self, query):
        c = rpyc.connect_by_service("SEARCH", config={'allow_public_attrs': True})
        search_results = c.root.search(query)
        search_result = [item for sublist in search_results for item in sublist]
        return search_result
        




    