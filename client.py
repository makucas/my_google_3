import rpyc
import json
import pandas as pd

with open("sample_dataset.json", 'r') as json_file:
    data = json.load(json_file)

c = rpyc.connect_by_service("MASTER")
c.root.insert("arquivo1_chunk0", data)