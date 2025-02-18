import json
import boto3
import requests
from bs4 import BeautifulSoup


def lambda_handler(event, context):
    try:
        TARGET_URL = "https://www.clien.net/service/search"

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
        OBJECT_KEY = f"{car_name}/{year}/{month}/clien_target_urls.csv"

        params = {
            "q": search_keyword,
            "p": 1,
            "sort": "recency",
            "boardCd": "",
            "isBoard": "false",
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
        }
        urls = []
        for i in range(50):
            params["p"] = i
            html = requests.get(TARGET_URL, params=params, headers=headers)
            soup = BeautifulSoup(html.content, "html.parser")

            search_result = soup.find("div", "total_search")
            posts = search_result.find_all("div", "list_item symph_row jirum")
            for post in posts:
                timestamp = post.find("span", "timestamp").text
                if input_date == timestamp[:7]:
                    urls.append("https://www.clien.net" + post.find("a")["href"])
                # TODO: should stop when timestamp is before input_date

        with open(
            f"/tmp/clien_{input_date}_{car_name}.csv", "w", encoding="utf-8"
        ) as f:
            for url in urls:
                f.write(url + "\n")

        s3 = boto3.client("s3")
        s3.upload_file(
            f"/tmp/clien_{input_date}_{car_name}.csv",
            BUCKET_NAME,
            OBJECT_KEY,
        )

        return {"statusCode": 200}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(str(e))}
