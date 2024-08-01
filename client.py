import rpyc
import json
import pandas as pd
import sys
from pympler import asizeof
import os
class ChunkLoader():
    def __init__(self, connection, max_size=10*1024):
        self.connection = connection

        self.chunk = []
        self.max_size = max_size
        self.chunk_size = 0
        self.chunk_id = 1

        self.chunk_total_size = 0
        self.archive_total_size = 0

    def reset_chunk(self):
        self.chunk = []
        self.chunk_total_size += self.chunk_size
        self.chunk_size = 0
        self.chunk_id += 1

    def send_chunk(self, chunk, id, archive_name):
        print(f"Sending chunk{id} - {int((100*self.chunk_total_size)/self.archive_total_size)}%")
        chunk_name = f"{archive_name}_chunk{id}"
        self.connection.root.insert(chunk_name, chunk)

    def load_chunk(self, archive_path, archive_name):
        """
            Adiciona cada item do json a chunk, quando o tamanho da chunk em kB superar 
            o tamanho máximo previsto, envia a chunk, reinicia ela e repete o processo 
            enquanto existir itens no json.
        """
        self.archive_total_size = (os.path.getsize(archive_path))/(1024) # tamanho em kB

        with open(archive_path, 'r') as f:
            while True:
                item = f.readline()
                if not item:
                    self.send_chunk(self.chunk, self.chunk_id, archive_name)
                    break

                item_size = (asizeof.asizeof(item)/(1024)) # tamanho em kB
                if self.chunk_size + item_size > self.max_size:
                    self.send_chunk(self.chunk, self.chunk_id, archive_name)
                    self.reset_chunk()
                else:
                    self.chunk.append(json.loads(item))
                    self.chunk_size += item_size

if __name__ == "__main__":
    c = rpyc.connect_by_service("MASTER", config={'allow_public_attrs': True})

    loader = ChunkLoader(connection=c)
    loader.load_chunk(archive_path="archives/2016_copy.txt", archive_name="2016")

    #search_results = c.root.search("pode resultar")
    #print(search_results)

