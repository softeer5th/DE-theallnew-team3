import json
import os
import boto3
from googleapiclient.discovery import build


def lambda_handler(event, context):
    try:
        API_KEY = os.environ.get("YOUTUBE_API_KEY")

        input_date = event["input_date"]
        car_name = event["car_name"]

        if input_date == "" or car_name == "":
            return {
                "statusCode": 400,
                "body": json.dumps("input_date and car_name are required"),
            }

        if API_KEY == "":
            return {
                "statusCode": 400,
                "body": json.dumps("YOUTUBE_API_KEY is required"),
            }

        year = input_date.split("-")[0]
        month = input_date.split("-")[1]

        BUCKET_NAME = "the-all-new-bucket"
        OBJECT_KEY = f"{car_name}/{year}/{month}/youtube_target_videos.csv"

        def get_next_month(month_filter):
            year, month = map(int, month_filter.split("-"))
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1

            return f"{year}-{month:02d}"

        published_after = f"{input_date}-01T00:00:00Z"
        published_before = f"{get_next_month(input_date)}-01T00:00:00Z"

        youtube = build("youtube", "v3", developerKey=API_KEY)

        response = (
            youtube.search()
            .list(
                q=car_name,
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

        video_ids = [item["id"]["videoId"] for item in response["items"]]

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
