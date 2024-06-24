from load_balancer import LoadBalancer
import rpyc

class MasterService(rpyc.Service):
    def __init__(self):
        self.lb = LoadBalancer(3)
        pass

    def on_connect(self, conn):
        pass

    def on_disconnect(self, conn):
        pass

    def exposed_insert(self, archive):
        connections = self.lb.forward_request()
        print(connections)

        ip, port = connections["SLAVE1"]
        c = rpyc.connect(ip, port)
        result = c.root.insert(archive)

        # c = rpyc.connect(ip, port)
        # result = c.root.search()
        # return result

    