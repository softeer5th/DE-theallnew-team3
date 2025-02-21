import json
import os
import boto3
import time
from datetime import datetime
from googleapiclient.discovery import build
from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_POPULAR

BATCH_SIZE = 10


def lambda_handler(event, context):
    API_KEY = os.environ.get("YOUTUBE_API_KEY")

    input_date = event["input_date"]
    car_name = event["car_name"]
    page = event["page"]
    if not page:
        page = 1

    if not input_date or not car_name:
        raise Exception("input_date and car_name are required")

    if not API_KEY:
        raise Exception("YOUTUBE_API_KEY is required")

    youtube = build("youtube", "v3", developerKey=API_KEY)

    year, month, day = input_date.split("-")

    BUCKET_NAME = "the-all-new-bucket"
    READ_OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/target/youtube.csv"
    WRITE_OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/raw/youtube_{page}.json"
    FAILED_OBJECT_KEY = (
        f"{car_name}/{year}/{month}/{day}/target/youtube_failed_{page}.csv"
    )

    s3 = boto3.client("s3")
    s3.download_file(
        BUCKET_NAME,
        READ_OBJECT_KEY,
        f"/tmp/youtube_{input_date}_{car_name}.csv",
    )

    with open(f"/tmp/youtube_{input_date}_{car_name}.csv", "r") as f:
        video_ids = f.readlines()
        video_ids = [video_id.strip() for video_id in video_ids]

    video_ids = video_ids[page - 1 :: BATCH_SIZE]

    stats_response = (
        youtube.videos()
        .list(part="statistics,snippet", id=",".join(video_ids))
        .execute()
    )

    # 시간 변환 함수 (UTC → timestamp)
    def convert_to_timestamp(date_str):
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
            return int(time.mktime(dt.timetuple()))
        except:
            return None

    # 영상 데이터 저장 리스트
    video_data_list = []
    failed = []

    # 댓글 다운로드 객체 생성
    downloader = YoutubeCommentDownloader()

    print("=====START=====")

    for item in stats_response["items"]:
        try:
            video_id = item["id"]
            statistics = item.get("statistics", {})
            snippet = item.get("snippet", {})

            # 영상 정보 저장 (변경된 JSON 스키마 적용)
            video_info = {
                "id": "youtube_" + video_id,
                "car_name": car_name,
                "source": "youtube",
                "title": snippet.get("title", ""),
                "nickname": snippet.get("channelTitle", ""),
                "article": snippet.get("description", ""),
                "like_count": int(statistics.get("likeCount", 0)),
                "view_count": int(statistics.get("viewCount", 0)),
                "dislike_count": 0,
                "date": convert_to_timestamp(
                    snippet.get("publishedAt", "")
                ),  # 타임스탬프로 변환
                "comment_count": int(statistics.get("commentCount", 0)),
                "comments": [],  # 댓글 리스트 초기화
            }

            # 댓글 가져오기

            comments = downloader.get_comments_from_url(
                f"https://www.youtube.com/watch?v={video_id}",
                sort_by=SORT_BY_POPULAR,
            )

            for comment in comments:
                video_info["comments"].append(
                    {
                        "comment_nickname": comment.get("author", ""),
                        "comment_content": comment.get("text", ""),
                        "comment_like_count": int(comment.get("votes", "0")),
                        "comment_dislike_count": 0,  # YouTube API에서 싫어요 수 제공 안 함
                        "comment_date": convert_to_timestamp(
                            datetime.utcfromtimestamp(comment["time_parsed"]).strftime(
                                "%Y-%m-%dT%H:%M:%SZ"
                            )
                        ),
                    }
                )

            # 최종 데이터 리스트에 추가
            video_data_list.append(video_info)
            print(f"{video_id} Success")
        except:
            print(f"{video_id} Failed")
            failed.append(video_id)

    # JSON 파일 저장 (YYYY-MM.json)
    with open(
        f"/tmp/youtube_{input_date}_{car_name}_{page}.json", "w", encoding="utf-8"
    ) as jsonfile:
        json.dump(video_data_list, jsonfile, ensure_ascii=False, indent=4)

    s3.upload_file(
        f"/tmp/youtube_{input_date}_{car_name}_{page}.json",
        BUCKET_NAME,
        WRITE_OBJECT_KEY,
    )
    print(f"{WRITE_OBJECT_KEY} 업로드 완료")

    if len(failed) > 0:
        with open(f"/tmp/youtube_{input_date}_{car_name}_failed_videos.csv", "w") as f:
            for video_id in failed:
                f.write(video_id + "\n")

        s3.upload_file(
            f"/tmp/youtube_{input_date}_{car_name}_failed_videos.csv",
            BUCKET_NAME,
            FAILED_OBJECT_KEY,
        )

        return {
            "statusCode": 200,
            "failed": failed,
        }

    return {"statusCode": 200}
