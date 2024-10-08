import os
import requests
import json
import time
import subprocess
import fcntl
from concurrent.futures import ProcessPoolExecutor
from functools import partial


def acquire_lock(file_handle):
    fcntl.flock(file_handle, fcntl.LOCK_EX)


def release_lock(file_handle):
    fcntl.flock(file_handle, fcntl.LOCK_UN)


def read_json_with_lock(file_path):
    with open(file_path, 'r') as f:
        acquire_lock(f)
        try:
            data = json.load(f)
        finally:
            release_lock(f)
    return data


def write_json_with_lock(file_path, data):
    with open(file_path, 'w') as f:
        acquire_lock(f)
        try:
            json.dump(data, f)
        finally:
            release_lock(f)


def download_and_upload(eprint_id, target_path, downloaded_files_path):
    url = f"https://arxiv.org/src/{eprint_id}"
    local_file_path = f"tmp_latex_data/{eprint_id}.tar.gz"
    # time.sleep(1)

    try:
        # 检查是否已下载
        downloaded_files = read_json_with_lock(downloaded_files_path)
        if eprint_id in downloaded_files:
            print(f"Skipping {eprint_id}: already processed")
            return

        # 下载文件
        response = requests.get(url)
        response.raise_for_status()

        # 保存文件到临时目录
        with open(local_file_path, 'wb') as file:
            file.write(response.content)

        # 上传文件到服务器
        ssh_command = [
            "scp", "-P", "2222", local_file_path,
            f"xcs-run-batch-1-44zcf-master-0.ou-600a79a43b2e47a07dfcc2c984743ee8.prod.pod@sshproxy.dh3.ai:{target_path}/{eprint_id}.tar.gz"
        ]
        subprocess.run(ssh_command, check=True)

        # 删除本地临时文件
        os.remove(local_file_path)

        # 更新已下载文件列表
        downloaded_files = read_json_with_lock(downloaded_files_path)
        downloaded_files[eprint_id] = True
        write_json_with_lock(downloaded_files_path, downloaded_files)

        print(f"Successfully processed: {eprint_id}")
    except Exception as e:
        print(f"Error processing {eprint_id}: {str(e)}")
        if os.path.exists(local_file_path):
            os.remove(local_file_path)


def multi_process_download(eprint_id_list, target_path, num_processes=4):
    downloaded_files_path = 'downloaded_files.json'

    # 确保 downloaded_files.json 存在
    if not os.path.exists(downloaded_files_path):
        write_json_with_lock(downloaded_files_path, {})

    # 使用偏函数来固定 target_path 和 downloaded_files_path 参数
    download_func = partial(download_and_upload, target_path=target_path, downloaded_files_path=downloaded_files_path)

    # 使用进程池执行下载和上传任务
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        executor.map(download_func, eprint_id_list)


def download():
    eprint_id_list = []
    input_dir = "temp_data"
    target_path = "/gpfs/public/research/xy/yubowang/cite_rag/arxiv_data_collect/latex_data/"  # 更改为服务器上的目标路径

    for file in os.listdir(input_dir):
        if file.endswith(".json"):
            with open(os.path.join(input_dir, file), "r") as fi:
                curr = json.load(fi)
                eprint_id_list.extend(curr.keys())

    multi_process_download(eprint_id_list, target_path)


if __name__ == "__main__":
    download()


