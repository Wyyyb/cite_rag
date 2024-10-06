from lxml import html
import requests
import re
import math
import csv
from bs4 import BeautifulSoup
import time


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
        pdf_link_element = article.find('a', text='pdf')
        if pdf_link_element:
            pdf_link = pdf_link_element['href']
        else:
            pdf_link = 'No PDF link found'

        papers.append({'title': title, 'authors': authors, 'abstract': abstract, 'submission_date': submission_date,
                       'pdf_link': pdf_link})

    return papers


def save_to_csv(papers, filename):
    """将所有爬取的论文信息保存到CSV文件中"""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['title', 'authors', 'abstract', 'submission_date', 'pdf_link']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for paper in papers:
            writer.writerow(paper)


# 主程序
base_url = "https://arxiv.org/search/advanced?advanced=&terms-0-operator=AND&terms-0-term=&terms-0-field=paper_id&terms-1-operator=AND&terms-1-term=&terms-1-field=all&classification-computer_science=y&classification-physics_archives=all&classification-include_cross_list=include&date-filter_by=all_dates&date-year=&date-from_date=&date-to_date=&date-date_type=submitted_date&abstracts=show&size=200&order=-announced_date_first"
total_results = get_total_results(base_url + "&start=0")
pages = math.ceil(total_results / 200)
all_papers = []

for page in range(pages):
    start = page * 200
    print(f"Crawling page {page + 1}/{pages}, start={start}")
    page_url = base_url + f"&start={start}"
    all_papers.extend(get_paper_info(page_url))
    time.sleep(3)  # 等待三秒以避免对服务器造成过大压力

# 保存到CSV
save_to_csv(all_papers, 'data/1006.csv')
print(f"完成！总共爬取到 {len(all_papers)} 条数据，已保存到 1006.csv 文件中。")
