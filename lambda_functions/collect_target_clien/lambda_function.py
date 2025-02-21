import json
import boto3
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


def get_before_day(day_filter):
    date_obj = datetime.strptime(day_filter, "%Y-%m-%d")
    prev_day = date_obj - timedelta(days=7)
    return prev_day.strftime("%Y-%m-%d")  # 문자열로 변환 후 반환


def lambda_handler(event, context):
    TARGET_URL = "https://www.clien.net/service/search"

    input_date = event["input_date"]
    car_name = event["car_name"]
    search_keywords = event["search_keywords"].split(",")

    if not input_date or not car_name or not search_keywords:
        raise Exception("input_date and car_name and search_keywords are required")

    year, month, day = input_date.split("-")
    start_date = get_before_day(input_date)

    BUCKET_NAME = "the-all-new-bucket"
    OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/target/clien.csv"

    urls = []
    for search_keyword in search_keywords:
        params = {
            "q": search_keyword,
            "sort": "recency",
            "boardCd": "",
            "isBoard": "false",
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
        }

        for i in range(50):
            if i > 0:
                params["p"] = i
            html = requests.get(
                TARGET_URL, params=params, headers=headers, allow_redirects=False
            )
            soup = BeautifulSoup(html.content, "html.parser")

            search_result = soup.find("div", "total_search")
            posts = search_result.find_all("div", "list_item symph_row jirum")
            for post in posts:
                timestamp = post.find("span", "timestamp").text  # 게시물 날짜 추출
                post_date = timestamp[:10]  # "YYYY-MM-DD"

                if start_date <= post_date <= input_date:
                    urls.append("https://www.clien.net" + post.find("a")["href"])

                # 수집 대상 날짜보다 이전 날짜가 나오면 중단
                if post_date < start_date:
                    # print("더 이상 수집할 데이터 없음. 종료.")
                    break

    urls = list(set(urls))

    with open(f"/tmp/clien_{input_date}_{car_name}.csv", "w", encoding="utf-8") as f:
        for url in urls:
            f.write(url + "\n")

    s3 = boto3.client("s3")
    s3.upload_file(
        f"/tmp/clien_{input_date}_{car_name}.csv",
        BUCKET_NAME,
        OBJECT_KEY,
    )

    return {"statusCode": 200}
