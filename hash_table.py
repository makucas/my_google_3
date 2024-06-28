import rpyc
import csv

class HashTableService(rpyc.Service):
    def __init__(self):
        self.file_path = 'hash_table.csv'
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
        if archive in self.hash_table:
            for entry in self.hash_table[archive]:
                if entry['chunk'] == chunk:
                    return entry['machine']
        return None

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