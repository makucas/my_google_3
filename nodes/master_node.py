import rpyc

class MasterService(rpyc.Service):
    def __init__(self):
        pass

    def on_connect(self, conn):
        pass

    def on_disconnect(self, conn):
        pass

    def exposed_insert(self, archive_name, archive):
        c = rpyc.connect_by_service("INSERT")
        c.root.insert(archive_name, archive)
        




    