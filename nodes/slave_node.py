import rpyc
#from services.search import Searcher

class SlaveService(rpyc.Service):
    def __init__(self):
        pass

    def exposed_insert():
        pass

def create_slave(name):
    return type(name, (SlaveService,), {})