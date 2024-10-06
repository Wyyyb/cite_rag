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


def convert_date_format(date_string):
    print("original date", date_string)
    # 将输入的日期字符串解析为datetime对象
    date_object = datetime.strptime(date_string, "%d %B, %Y")

    # 将datetime对象格式化为所需的输出格式
    formatted_date = date_object.strftime("%Y-%m-%d")

    return formatted_date


def main():
    # end_date = "2024-09-30"
    end_date = "2024-06-18"
    ori_base_url = "https://arxiv.org/search/advanced?advanced=&terms-0-operator=AND&terms-0-term=&terms-0-field=paper_id&terms-1-operator=AND&terms-1-term=&terms-1-field=all&classification-computer_science=y&classification-physics_archives=all&classification-include_cross_list=include&date-year=&date-filter_by=date_range&date-from_date=&date-to_date={$$}&date-date_type=submitted_date&abstracts=show&size=200&order=-announced_date_first"
    all_papers = {}

    data_path = "data/arxiv_data_collection_1006.json"
    if os.path.exists(data_path):
        with open(data_path, "r") as fi:
            all_papers = json.load(fi)
    new_papers = []
    for k, v in all_papers.items():
        new_papers.append(v)
    page = 0

    while page < 50:
        start = page * 200
        if page >= 49:
            end_date = convert_date_format(new_papers[-1]["submission_date"])
            print("update end date to ", end_date)
            # base_url = base_url.replace("{$$}", end_date)
            page = 0
        base_url = ori_base_url.replace("{$$}", end_date)
        page_url = base_url + f"&start={start}"
        print("page_url", page_url)
        info = get_paper_info(page_url)
        print("get_paper_info(page_url) length", len(info))
        for each in info:
            if each["eprint_id"] not in all_papers:
                all_papers[each["eprint_id"]] = each
                new_papers.append(each)
        if len(all_papers) > 30000:
            prev, post = split_dict(all_papers, 30000)
            all_papers = post
            code = int(data_path.split(".json")[0].split("_")[-1])
            code += 1
            data_path = f"data/arxiv_data_collection_{str(code)}.json"
        with open(data_path, "w") as fo:
            fo.write(json.dumps(all_papers, indent=2))
        time.sleep(3)  # 等待以避免对服务器造成过大压力
        page += 1


def sort_dict(input_dict):
    keys = list(input_dict.keys())
    keys = sorted(keys, key=lambda x: -x)
    res = {}
    for key in keys:
        res[key] = input_dict[key]
    return res


def split_dict(input_dict, index):
    keys = list(input_dict.keys())
    keys = sorted(keys, reverse=True)
    prev = {}
    post = {}
    for i in range(index):
        prev[keys[i]] = input_dict[keys[i]]
    for i in range(index, len(keys)):
        post[keys[i]] = input_dict[keys[i]]
    return prev, post


if __name__ == "__main__":
    main()


