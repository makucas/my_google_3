import rpyc
import csv
import os

class HashTableService(rpyc.Service):
    def __init__(self):
        self.file_path = 'data/hash_table.csv'

        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as _:
                pass

        self.hash_table = self.load_hash_table()

    def load_hash_table(self):
        hash_table = {}
        try:
            with open(self.file_path, mode='r') as file:
                reader = csv.reader(file)
                for row in reader:
                    archive, chunk, machine = row
                    if archive not in hash_table:
                        hash_table[archive] = []
                    hash_table[archive].append({'chunk': chunk, 'machine': machine})
        except FileNotFoundError:
            pass
        return hash_table

    def exposed_get_chunk_location(self, archive, chunk):
        chunk_machines = []
        if archive in self.hash_table:
            for entry in self.hash_table[archive]:
                if entry['chunk'] == chunk:
                    chunk_machines.append(entry['machine'])
        return chunk_machines

    def exposed_add_entry(self, archive_name):
        archive, chunk, machine = archive_name.split('_')
        if archive not in self.hash_table:
            self.hash_table[archive] = []
        self.hash_table[archive].append({'chunk': chunk, 'machine': machine})
        self.save_hash_table()

    def save_hash_table(self):
        with open(self.file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            for archive, entries in self.hash_table.items():
                for entry in entries:
                    writer.writerow([archive, entry['chunk'], entry['machine']])
    
    def exposed_get_all_chunks(self):
        """
        Obtem todos os chunks de todos os arquivos no hash table
        """
        def extract_number(chunk):
            return int(chunk[5:])
        
        self.hash_table = self.load_hash_table()

        unique_chunks = {}

        for archive, data_list in self.hash_table.items():
            unique_chunks[archive] = []
            for item in data_list:
                unique_chunks[archive].append(item["chunk"])
        
        for archive in unique_chunks:
            unique_chunks[archive] = sorted(list(set(unique_chunks[archive])), key=extract_number)

        return unique_chunks