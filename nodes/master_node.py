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
        




    