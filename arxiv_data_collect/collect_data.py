from lxml import html
import re
import math
from bs4 import BeautifulSoup
import time
from tqdm import tqdm
import requests
import os
import tarfile
import shutil
import json
import multiprocessing
from functools import partial
from datetime import datetime


def contains_latex_files(directory):
    for root, dirs, files in os.walk(directory):
        if any(file.endswith('.tex') for file in files):
            return True
    return False


def get_total_results(url):
    """获取总结果数"""
    response = requests.get(url)
    tree = html.fromstring(response.content)
    result_string = ''.join(tree.xpath('//*[@id="main-container"]/div[1]/div[1]/h1/text()')).strip()
    match = re.search(r'of ([\d,]+) results', result_string)
    if match:
        total_results = int(match.group(1).replace(',', ''))
        return total_results
    else:
        print("没有找到匹配的数字。")
        return 0


def get_paper_info(url):
    """根据URL爬取一页的论文信息"""
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    papers = []

    for article in soup.find_all('li', class_='arxiv-result'):
        title = article.find('p', class_='title').text.strip()
        authors_text = article.find('p', class_='authors').text.replace('Authors:', '').strip()
        authors = [author.strip() for author in authors_text.split(',')]
        abstract = article.find('span', class_='abstract-full').text.strip()
        submitted = article.find('p', class_='is-size-7').text.strip()
        submission_date = submitted.split(';')[0].replace('Submitted', '').strip()
        eprint_id = article.find('a', href=lambda href: href and href.startswith('https://arxiv.org/abs/'))['href'].split('/')[-1]

        papers.append({'title': title, 'authors': authors,
                       'abstract': abstract, 'submission_date': submission_date,
                       'eprint_id': eprint_id})

    return papers


def process_paper(paper, latex_dir):
    current_process = multiprocessing.current_process()
    pid = os.getpid()
    process_name = current_process.name
    process_id = current_process._identity[0]

    # print(f"Process {process_name} (PID: {pid}, ID: {process_id}) processing paper: {paper['eprint_id']}")
    get_latex_data(paper["eprint_id"], latex_dir)


def get_latex_data(eprint_id, target_folder):
    if os.path.exists(os.path.join(target_folder, eprint_id)):
        return None
    url = f"https://arxiv.org/src/{eprint_id}"
    # Create the target folder if it doesn't exist
    os.makedirs(target_folder, exist_ok=True)

    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Get the filename from the Content-Disposition header
        content_disposition = response.headers.get('Content-Disposition')
        if content_disposition:
            filename = content_disposition.split('filename=')[1].strip('"')
        else:
            filename = f"{eprint_id}.tar.gz"

        # Full path for the downloaded file
        file_path = os.path.join(target_folder, filename)

        # Save the content to a file
        with open(file_path, 'wb') as file:
            file.write(response.content)

        print(f"Successfully downloaded: {file_path}")

        # Extract the tar.gz file
        try:
            with tarfile.open(file_path, "r:gz") as tar:
                # Create a subdirectory for extraction
                extract_path = os.path.join(target_folder, eprint_id)
                os.makedirs(extract_path, exist_ok=True)

                # Extract all contents
                tar.extractall(path=extract_path)
            # Check if the extracted content is a directory
            # After extraction
            if not contains_latex_files(extract_path):
                print(f"No LaTeX files found in: {extract_path}")
                shutil.rmtree(extract_path)
                return None
            extracted_contents = os.listdir(extract_path)
            if len(extracted_contents) == 1 and os.path.isfile(os.path.join(extract_path, extracted_contents[0])):
                print(f"Extracted content is not a directory. Deleting: {extract_path}")
                shutil.rmtree(extract_path)
                return None
            # print(f"Successfully extracted to: {extract_path}")

            # Optionally, remove the tar.gz file after extraction
            os.remove(file_path)
            # print(f"Removed the compressed file: {file_path}")

            return extract_path
        except tarfile.TarError as e:
            print(f"Failed to extract the file: {e}")
            return None
    else:
        print(f"Failed to download. Status code: {response.status_code}")
        return None


def convert_date_format(date_string):
    # 将输入的日期字符串解析为datetime对象
    date_object = datetime.strptime(date_string, "%d %B, %Y")

    # 将datetime对象格式化为所需的输出格式
    formatted_date = date_object.strftime("%Y-%m-%d")

    return formatted_date


def main():
    end_date = "2024-09-04"
    base_url = "https://arxiv.org/search/advanced?advanced=&terms-0-operator=AND&terms-0-term=&terms-0-field=paper_id&terms-1-operator=AND&terms-1-term=&terms-1-field=all&classification-computer_science=y&classification-physics_archives=all&classification-include_cross_list=include&date-year=&date-filter_by=date_range&date-from_date=&date-to_date={$$}&date-date_type=submitted_date&abstracts=show&size=200&order=-announced_date_first"
    total_results = get_total_results(base_url + "&start=0")
    print("Total results", total_results)
    pages = math.ceil(total_results / 200)
    all_papers = {}
    # 指定进程数，例如使用8个进程
    num_processes = 16  # 你可以根据需要更改这个数字
    pool = multiprocessing.Pool(processes=num_processes)
    latex_dir = "latex_data"

    data_path = "data/arxiv_data_collection.json"
    if os.path.exists(data_path):
        with open(data_path, "r") as fi:
            all_papers = json.load(fi)
    exist_id = []
    for file in os.listdir(latex_dir):
        if os.path.isdir(file):
            exist_id.append(file)
    temp = {}
    for k, v in all_papers.items():
        if k not in exist_id:
            continue
        temp[k] = v
    print("cleaned meta data length", len(temp))
    all_papers = temp
    page = 0

    while page < 50:
        start = page * 200
        print(f"Crawling page {page + 1}/{pages}, start={start}")
        if page >= 49:
            end_date = convert_date_format(new_papers[-1]["submission_date"])
            print("update end date to ", end_date)
            base_url = base_url.replace("{$$}", end_date)
            page = 0
        page_url = base_url + f"&start={start}"
        new_papers = []
        for each in get_paper_info(page_url):
            if each["eprint_id"] not in all_papers:
                all_papers[each["eprint_id"]] = each
                new_papers.append(each)
        # 使用多进程并行处理新论文
        with open(data_path, "w") as fo:
            fo.write(json.dumps(all_papers, indent=2))
        process_paper_partial = partial(process_paper, latex_dir=latex_dir)
        pool.map(process_paper_partial, new_papers)
        # time.sleep(2)  # 等待三秒以避免对服务器造成过大压力
        page += 1


if __name__ == "__main__":
    main()


