import json
import boto3
import requests
from bs4 import BeautifulSoup
import time


def lambda_handler(event, context):
    try:
        TARGET_URL = "https://www.bobaedream.co.kr/search"

        input_date = event["input_date"]
        car_name = event["car_name"]
        search_keyword = event["search_keyword"]

        if not input_date or not car_name or not search_keyword:
            return {
                "statusCode": 400,
                "body": json.dumps(
                    "input_date and car_name and search_keyword are required"
                ),
            }

        year, month = input_date.split("-")

        BUCKET_NAME = "the-all-new-bucket"
        OBJECT_KEY = f"{car_name}/{year}/{month}/bobae_target_links.csv"

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

        YEAR, MONTH = input_date.split("-")

        page = 1
        target_links = []

        while True:
            form_data["page"] = page
            res = requests.post(
                TARGET_URL,
                data=form_data,
                headers=headers,
            )

            soup = BeautifulSoup(res.content, "html.parser")
            links = soup.find("div", "search_Community")
            links = links.find("ul")
            links = links.find_all("li")

            stop_flag = False

            for link in links:
                date = link.find_all("span", "next")[1].text
                date = date.split(". ")
                target_year = int(YEAR[2:])
                target_month = int(MONTH)
                link_year = int(date[0])
                link_month = int(date[1])
                if link_year == target_year and link_month == target_month:
                    target_links.append(link.find("a")["href"])
                elif link_year < target_year or link_month < target_month:
                    stop_flag = True
                    break
            page += 1
            if stop_flag:
                break
            time.sleep(0.1)

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
