import json
import pyarrow.parquet as pq
import pyarrow as pa
import s3fs

POST_SCHEMA = pa.schema(
    [
        pa.field("post_uuid", pa.string()),
        pa.field("post_id", pa.string()),
        pa.field("title", pa.string()),
        pa.field("author", pa.string()),
        pa.field("article", pa.string()),
        pa.field("create_timestamp", pa.int64()),
        pa.field("like_cnt", pa.int64()),
        pa.field("dislike_cnt", pa.int64()),
        pa.field("view_cnt", pa.int64()),
        pa.field("comment_cnt", pa.int64()),
        pa.field("car_name", pa.string()),
        pa.field("source", pa.string()),
    ]
)
COMMENT_SCHEMA = pa.schema(
    [
        pa.field("post_uuid", pa.string()),
        pa.field("comment_uuid", pa.string()),
        pa.field("comment_id", pa.string()),
        pa.field("author", pa.string()),
        pa.field("content", pa.string()),
        pa.field("create_timestamp", pa.int64()),
        pa.field("like_cnt", pa.int64()),
        pa.field("dislike_cnt", pa.int64()),
    ]
)
SENTENCE_SCHEMA = pa.schema(
    [
        pa.field("post_uuid", pa.string()),
        pa.field("comment_uuid", pa.string()),
        pa.field("sentence_uuid", pa.string()),
        pa.field("type", pa.string()),
        pa.field("sentence", pa.string()),
    ]
)
CLASSIFIED_SCHEMA = pa.schema(
    [
        pa.field("sentence_uuid", pa.string()),
        pa.field("sentiment_score", pa.float64()),
        pa.field("category", pa.string()),
        pa.field("keyword", pa.string()),
    ]
)


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

        fs = s3fs.S3FileSystem()

        with fs.open(s3_uri, "rb") as f:
            parquet_file = pq.ParquetFile(f)
            file_schema = parquet_file.schema.to_arrow_schema()

            is_schema_match = file_schema.equals(schema)

            if not is_schema_match:
                raise Exception(f"Schema mismatch for {object_key}")

    return {
        "statusCode": 200,
    }
