import json
import boto3
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta


def get_before_day(day_filter):
    """입력 날짜에서 7일 전 날짜 반환"""
    date_obj = datetime.strptime(day_filter, "%Y-%m-%d")
    prev_day = date_obj - timedelta(days=7)
    return prev_day.strftime("%Y-%m-%d")  # YYYY-MM-DD


def lambda_handler(event, context):
    try:
        TARGET_URL = "https://www.bobaedream.co.kr/search"

        input_date = event["input_date"]
        car_name = event["car_name"]

        start_date = get_before_day(input_date)
        search_keywords = event["search_keywords"].split(",")

        if not input_date or not car_name or not search_keywords:
            return {
                "statusCode": 400,
                "body": json.dumps(
                    "input_date and car_name and search_keywords are required"
                ),
            }

        year, month, day = input_date.split("-")

        BUCKET_NAME = "the-all-new-bucket"
        OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/bobae_target_links.csv"

        target_links = []

        for search_keyword in search_keywords:

            form_data = {
                "colle": "community",
                "searchField": "ALL",
                "page": 0,
                "sort": "DATE",
                "startDate": "",
                "keyword": search_keyword,
            }

            ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.35 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.35"
            host = "www.bobaedream.co.kr"
            origin = "https://www.bobaedream.co.kr"
            referer = "https://www.bobaedream.co.kr/search"

            headers = {
                "User-Agent": ua,
                "Host": host,
                "Origin": origin,
                "Referer": referer,
            }

            page = 1

            while True:
                form_data["page"] = page
                res = requests.post(TARGET_URL, data=form_data, headers=headers)

                soup = BeautifulSoup(res.content, "html.parser")
                links = soup.find("div", "search_Community")
                links = links.find("ul")
                links = links.find_all("li")

                stop_flag = False

                for link in links:
                    # 날짜 추출 및 변환
                    date_str = link.find_all("span", "next")[1].text  # "25. 01. 30"
                    date_parts = date_str.split(". ")
                    link_date = f"20{date_parts[0]}-{date_parts[1]}-{date_parts[2]}"  # YYYY-MM-DD 형식으로 변환

                    # 날짜 비교
                    if start_date <= link_date <= input_date:
                        target_links.append(link.find("a")["href"])
                    elif link_date < start_date:
                        stop_flag = True
                        break  # 날짜가 범위 밖이면 중단

                page += 1
                if stop_flag:
                    break
                time.sleep(0.1)

        target_links = list(set(target_links))

        with open(f"/tmp/bobae_{input_date}_{car_name}.csv", "w") as f:
            for link in target_links:
                f.write(link + "\n")

        s3 = boto3.client("s3")
        s3.upload_file(
            f"/tmp/bobae_{input_date}_{car_name}.csv",
            BUCKET_NAME,
            OBJECT_KEY,
        )

        return {"statusCode": 200}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(str(e))}
