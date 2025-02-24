import pyarrow.parquet as pq
import pyarrow as pa
import awswrangler as wr

POST_SCHEMA = {
    "post_uuid": "string",
    "post_id": "string",
    "title": "string",
    "author": "string",
    "article": "string",
    "create_timestamp": "int",
    "like_cnt": "int",
    "dislike_cnt": "int",
    "view_cnt": "int",
    "comment_cnt": "int",
    "car_name": "string",
    "source": "string",
}

COMMENT_SCHEMA = {
    "post_uuid": "string",
    "comment_uuid": "string",
    "comment_id": "string",
    "author": "string",
    "content": "string",
    "create_timestamp": "int",
    "like_cnt": "int",
    "dislike_cnt": "int",
}

SENTENCE_SCHEMA = {
    "post_uuid": "string",
    "comment_uuid": "string",
    "sentence_uuid": "string",
    "type": "string",
    "sentence": "string",
}
CLASSIFIED_SCHEMA = {
    "sentence_uuid": "string",
    "sentiment_score": "double",
    "category": "string",
    "keyword": "string",
}


def lambda_handler(event, context):
    input_date = event.get("input_date")
    car_name = event.get("car_name")

    if not input_date or not car_name:
        raise Exception("input_date and car_name are required")

    year, month, day = input_date.split("-")

    BUCKET_NAME = "the-all-new-bucket"
    POST_OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/post_data"
    COMMENT_OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/comment_data"
    SENTENCE_OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/sentence_data"
    CLASSIFIED_OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/classified"

    for object_key, schema in [
        (POST_OBJECT_KEY, POST_SCHEMA),
        (COMMENT_OBJECT_KEY, COMMENT_SCHEMA),
        (SENTENCE_OBJECT_KEY, SENTENCE_SCHEMA),
        (CLASSIFIED_OBJECT_KEY, CLASSIFIED_SCHEMA),
    ]:
        s3_uri = f"s3://{BUCKET_NAME}/{object_key}"
        file_schema, _ = wr.s3.read_parquet_metadata(s3_uri)

        print(file_schema)

        if file_schema != schema:
            raise Exception(f"Schema mismatch for {object_key}")

    return {
        "statusCode": 200,
    }
