import boto3
import json
import random


def check_json_schema(data, expected_schema, path="root"):
    """데이터가 주어진 스키마에 맞는지 검증하는 함수"""
    if isinstance(expected_schema, dict):
        if not isinstance(data, dict):
            raise Exception(f"{path}: Expected dict, got {type(data).__name__}")

        for key, expected_type in expected_schema.items():
            if key not in data:
                raise Exception(f"{path}.{key}: Missing key")
            check_json_schema(data[key], expected_type, f"{path}.{key}")

    elif isinstance(expected_schema, list) and expected_schema:
        if not isinstance(data, list):
            raise Exception(f"{path}: Expected list, got {type(data).__name__}")

        sub_schema = expected_schema[0]
        for i, item in enumerate(data):
            check_json_schema(item, sub_schema, f"{path}[{i}]")

    else:
        if not isinstance(data, expected_schema):
            raise Exception(
                f"{path}: Expected {expected_schema.__name__}, got {type(data).__name__}"
            )


def lambda_handler(event, context):
    input_date = event["input_date"]
    car_name = event["car_name"]
    source = event["source"]

    if not input_date or not car_name:
        raise Exception("input_date and car_name are required")
    if source not in ["bobae", "clien", "youtube"]:
        raise Exception("source must be one of bobae, clien, youtube")

    year, month, day = input_date.split("-")

    BUCKET_NAME = "the-all-new-bucket"
    if source == "bobae":
        READ_OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/raw/bobae.json"
    elif source == "clien":
        READ_OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/raw/clien.json"
    elif source == "youtube":
        READ_OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/raw/youtube_1.json"

    s3 = boto3.client("s3")

    s3.download_file(
        BUCKET_NAME, READ_OBJECT_KEY, f"/tmp/{READ_OBJECT_KEY.split('/')[-1]}"
    )

    with open(f"/tmp/{READ_OBJECT_KEY.split('/')[-1]}", "r") as f:
        data = json.load(f)

    data_sample = random.sample(data, min(10, len(data)))

    expected_schema = {
        "car_name": str,
        "id": str,
        "source": str,
        "title": str,
        "nickname": str,
        "article": str,
        "like_count": int,
        "dislike_count": int,
        "view_count": int,
        "date": int,
        "comment_count": int,
        "comments": [
            {
                "comment_nickname": str,
                "comment_content": str,
                "comment_like_count": int,
                "comment_dislike_count": int,
                "comment_date": int,
            }
        ],
    }

    for item in data_sample:
        check_json_schema(item, expected_schema)

    return {"statusCode": 200}
