import arxiv
import time
import os
import json
from datetime import datetime, timedelta, date

def download_cs_ai_papers(start_date, end_date, output_dir="cs_ai_papers", batch_size=100, sleep_time=5):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    progress_file = os.path.join(output_dir, "download_progress.json")

    # 确保 start_date 和 end_date 是 date 对象
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date() if isinstance(start_date, str) else start_date
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date() if isinstance(end_date, str) else end_date

    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            progress = json.load(f)
        current_date = datetime.strptime(progress['current_date'], "%Y-%m-%d").date()
    else:
        current_date = start_date
        progress = {'current_date': current_date.strftime("%Y-%m-%d"), 'downloaded': 0}

    total_downloaded = progress['downloaded']

    while current_date <= end_date:
        query = f"cat:cs.AI AND submittedDate:[{current_date.strftime('%Y-%m-%d')} TO {current_date.strftime('%Y-%m-%d')}]"
        search = arxiv.Search(
            query=query,
            max_results=batch_size,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Ascending
        )

        try:
            for result in search.results():
                try:
                    filename = f"{result.get_short_id()}.pdf"
                    filepath = os.path.join(output_dir, filename)

                    if os.path.exists(filepath):
                        print(f"File {filename} already exists. Skipping...")
                        continue

                    result.download_pdf(filename=filepath)
                    print(f"Downloaded: {result.title}")
                    total_downloaded += 1

                    time.sleep(sleep_time)

                except Exception as e:
                    print(f"Error downloading {result.title}: {str(e)}")

            # 更新进度
            progress['current_date'] = current_date.strftime("%Y-%m-%d")
            progress['downloaded'] = total_downloaded
            with open(progress_file, 'w') as f:
                json.dump(progress, f)

            current_date += timedelta(days=1)

        except Exception as e:
            print(f"Error in batch starting {current_date}: {str(e)}")
            time.sleep(60)  # 如果发生错误，等待较长时间后重试

    print(f"Download completed. Total papers downloaded: {total_downloaded}")

# 使用示例
download_cs_ai_papers(
    start_date="2010-01-01",  # 开始日期
    end_date="2023-12-31",    # 结束日期
    output_dir="cs_ai_papers",
    batch_size=100,           # 每批下载的论文数量
    sleep_time=5              # 每次下载之间的等待时间（秒）
)