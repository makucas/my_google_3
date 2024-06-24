import rpyc

class Manager():
    def __init__(self):
        pass
                
    def return_all_nodes(self):
        nodes = rpyc.list_services()
        nodes = [n for n in nodes if 'SLAVE' in n]
        return nodes
