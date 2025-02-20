import json
import os
import boto3
from googleapiclient.discovery import build
from datetime import datetime, timedelta


def get_before_day(day_filter):
    date_obj = datetime.strptime(day_filter, "%Y-%m-%d")
    prev_day = date_obj - timedelta(days=7)  # 7일 전
    return prev_day.strftime("%Y-%m-%d")  # 문자열로 변환 후 반환


def lambda_handler(event, context):
    try:
        API_KEY = os.environ.get("YOUTUBE_API_KEY")

        input_date = event["input_date"]
        car_name = event["car_name"]
        search_keywords = event["search_keywords"]

        if not input_date or not car_name or not search_keywords:
            return {
                "statusCode": 400,
                "body": json.dumps(
                    "input_date and car_name and search_keywords are required"
                ),
            }

        if API_KEY == "":
            return {
                "statusCode": 400,
                "body": json.dumps("YOUTUBE_API_KEY is required"),
            }

        target_date = get_before_day(input_date)
        year, month, day = input_date.split("-")

        BUCKET_NAME = "the-all-new-bucket"
        OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/youtube_target_videos.csv"

        published_after = f"{target_date}T00:00:00Z"
        published_before = f"{input_date}T23:59:59Z"

        youtube = build("youtube", "v3", developerKey=API_KEY)

        video_ids = []

        for search_keyword in search_keywords:
            response = (
                youtube.search()
                .list(
                    q=search_keyword,
                    part="snippet",
                    maxResults=50,
                    order="viewCount",
                    type="video",
                    publishedAfter=published_after,
                    publishedBefore=published_before,
                    regionCode="KR",
                )
                .execute()
            )

            video_ids.extend([item["id"]["videoId"] for item in response["items"]])

        # 중복 제거
        video_ids = list(set(video_ids))

        with open(f"/tmp/youtube_{input_date}_{car_name}.csv", "w") as f:
            for video_id in video_ids:
                f.write(video_id + "\n")

        s3 = boto3.client("s3")
        s3.upload_file(
            f"/tmp/youtube_{input_date}_{car_name}.csv", BUCKET_NAME, OBJECT_KEY
        )

        return {"statusCode": 200}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(str(e))}
