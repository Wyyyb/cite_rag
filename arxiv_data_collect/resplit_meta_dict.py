import json
import os
from datetime import datetime

input_dir = "data_bk"
data = {}


def convert_date_format(date_string):
    # print("original date", date_string)
    # 将输入的日期字符串解析为datetime对象
    date_object = datetime.strptime(date_string, "%d %B, %Y")

    # 将datetime对象格式化为所需的输出格式
    formatted_date = date_object.strftime("%Y-%m-%d")

    return formatted_date


for file in os.listdir(input_dir):
    if file.endswith(".json"):
        with open(os.path.join(input_dir, file), "r") as fi:
            curr = json.load(fi)
            for k, v in curr.items():
                if k not in data:
                    data[k] = v

print("original length", len(data))

filtered_data = {}
for k, v in data.items():
    if "." not in k:
        continue
    filtered_data[k] = v
print("filtered length", len(filtered_data))

keys = list(filtered_data.keys())
keys = sorted(keys, key=lambda x: convert_date_format(filtered_data[x]["submission_date"]), reverse=True)

print("reversing finished")

res = {}
for each_key in keys:
    res[each_key] = filtered_data[each_key]

# with open("meta_data/meta_data_collection_1007.json", "w") as fo:
#     fo.write(json.dumps(res))

count = len(keys) // 30000 + 1
for i in range(count):
    start = i * 30000
    end = min((i + 1) * 30000, len(keys))
    curr_res = {}
    for j in range(start, end):
        key = keys[j]
        value = filtered_data[key]
        curr_res[key] = value
    file_path = f"meta_data/meta_data_collection_1007_{str(i+1)}.json"
    with open(file_path, "w") as fo:
        fo.write(json.dumps(curr_res, indent=2))





