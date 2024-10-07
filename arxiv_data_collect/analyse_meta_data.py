import json
import os

data_dir = "data"
data = []
for file in os.listdir(data_dir):
    print("file", file)
    if ".json" not in file:
        continue
    file_path = os.path.join(data_dir, file)
    with open(file_path, "r") as fi:
        curr = json.load(fi)
        data += list(curr.keys())


print("length", len(data))




