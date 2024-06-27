import json
import os

def insert(archive_name, archive):
    file_names = archive_name.split("_")
    file_path = f"data/{file_names[0]}/{file_names[1]}.json"
    print(file_path)

    if not os.path.exists(f"data/{file_names[0]}"):
        os.makedirs(f"data/{file_names[0]}")
    
    with open(file_path, "w") as f:
        json.dump(archive, f)

if __name__ == "__main__":
    with open('sample_dataset.json', 'r') as file:
        dados = json.load(file)

    insert("arquivo1_chunk0", dados)