import rpyc

ip, port = rpyc.discover("MASTER")[0]
c = rpyc.connect(ip, port)
c.root.insert("teste")