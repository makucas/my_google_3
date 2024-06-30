import rpyc
import json
import pandas as pd
import sys
from pympler import asizeof
class ChunkLoader():
    def __init__(self, connection, max_size=1*1024):
        self.connection = connection

        self.chunk = []
        self.max_size = max_size
        self.total_size = 0
        self.chunk_id = 1

    def reset_chunk(self):
        self.chunk = []
        self.total_size = 0
        self.chunk_id += 1

    def send_chunk(self, chunk, id, archive_name):
        print("sending another chunk")
        chunk_name = f"{archive_name}_chunk{id}"
        self.connection.root.insert(chunk_name, chunk)

    def load_chunk(self, archive_path, archive_name):
        """
            Adiciona cada item do json a chunk, quando o tamanho da chunk em kB superar 
            o tamanho máximo previsto, envia a chunk, reinicia ela e repete o processo 
            enquanto existir itens no json.
        """
        with open(archive_path, 'r') as jsonl_file:
            for line in jsonl_file:
                item = json.loads(line)
                item_size = (asizeof.asizeof(item))/(1024) # tamanho em kB
                if self.total_size + item_size > self.max_size:
                    self.send_chunk(self.chunk, self.chunk_id, archive_name)
                    self.reset_chunk()
                else:
                    self.chunk.append(item)
                    self.total_size += item_size

if __name__ == "__main__":
    c = rpyc.connect_by_service("MASTER", config={'allow_public_attrs': True})

    loader = ChunkLoader(connection=c)
    loader.load_chunk(archive_path="archives/2016.jsonl", archive_name="2016")



