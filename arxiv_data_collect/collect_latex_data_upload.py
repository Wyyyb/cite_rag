import os
import requests
import zipfile
import tarfile
import multiprocessing
import shutil
from concurrent.futures import ProcessPoolExecutor
import json
import time


def download_and_extract(eprint_id, target_folder):
    url = f"https://arxiv.org/src/{eprint_id}"
    file_path = os.path.join(target_folder, f"{eprint_id}.tar.gz")
    time.sleep(1)

    try:
        # 下载文件
        response = requests.get(url)
        response.raise_for_status()

        # 保存文件
        with open(file_path, 'wb') as file:
            file.write(response.content)

        # 尝试解压文件
        if tarfile.is_tarfile(file_path):
            with tarfile.open(file_path, 'r:gz') as tar:
                tar.extractall(path=os.path.join(target_folder, eprint_id))
            os.remove(file_path)  # 解压成功后删除原压缩包
        elif zipfile.is_zipfile(file_path):
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(os.path.join(target_folder, eprint_id))
            os.remove(file_path)  # 解压成功后删除原压缩包
        else:
            # 如果不是压缩文件，则删除
            # os.remove(file_path)
            # print(f"Deleted non-archive file: {eprint_id}")
            print(eprint_id, "non-archive file", file_path)

        print(f"Successfully processed: {eprint_id}")
    except Exception as e:
        print(f"Error processing {eprint_id}: {str(e)}")
        # 如果发生错误，尝试删除已下载的文件（如果存在）
        if os.path.exists(file_path):
            os.remove(file_path)


def multi_process_download(eprint_id_list, target_folder, num_processes=1):
    # 确保目标文件夹存在
    os.makedirs(target_folder, exist_ok=True)

    # 使用进程池执行下载和解压任务
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        executor.map(download_and_extract, eprint_id_list, [target_folder] * len(eprint_id_list))


def download():
    eprint_id_list = []
    input_dir = "temp_data"
    target_folder = "latex_data"
    exist_ids = []
    for file in os.listdir(target_folder):
        if os.path.isdir(os.path.join(target_folder, file)):
            # print("exist file", file)
            exist_ids.append(file)
    for file in os.listdir(input_dir):
        if not file.endswith(".json"):
            continue
        with open(os.path.join(input_dir, file), "r") as fi:
            curr = json.load(fi)
            for k, v in curr.items():
                if k not in exist_ids:
                    eprint_id_list.append(k)
                # else:
                #     print("skip it")
    multi_process_download(eprint_id_list, target_folder)


if __name__ == "__main__":
    download()





