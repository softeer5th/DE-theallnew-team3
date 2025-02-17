import json
import os
import boto3
import time
from datetime import datetime
from googleapiclient.discovery import build
from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_POPULAR


def lambda_handler(event, context):
    try:
        API_KEY = os.environ.get("YOUTUBE_API_KEY")

        input_date = event["input_date"]
        car_name = event["car_name"]
        page = event["page"]
        if not page:
            page = 1

        if not input_date or not car_name:
            return {
                "statusCode": 400,
                "body": json.dumps("input_date and car_name are required"),
            }

        if not API_KEY:
            return {
                "statusCode": 400,
                "body": json.dumps("YOUTUBE_API_KEY is required"),
            }

        youtube = build("youtube", "v3", developerKey=API_KEY)

        year = input_date.split("-")[0]
        month = input_date.split("-")[1]

        BUCKET_NAME = "the-all-new-bucket"
        READ_OBJECT_KEY = f"{car_name}/{year}/{month}/youtube_target_videos.csv"
        WRITE_OBJECT_KEY = f"{car_name}/{year}/{month}/youtube_raw_{page}.json"

        s3 = boto3.client("s3")
        s3.download_file(
            BUCKET_NAME,
            READ_OBJECT_KEY,
            f"/tmp/youtube_{input_date}_{car_name}.csv",
        )

        with open(f"/tmp/youtube_{input_date}_{car_name}.csv", "r") as f:
            video_ids = f.readlines()
            video_ids = [video_id.strip() for video_id in video_ids]

        video_ids = video_ids[(page - 1) * 10 : page * 10]

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

        # 댓글 다운로드 객체 생성
        downloader = YoutubeCommentDownloader()

        for item in stats_response["items"]:
            video_id = item["id"]
            statistics = item.get("statistics", {})
            snippet = item.get("snippet", {})

            # 영상 정보 저장 (변경된 JSON 스키마 적용)
            video_info = {
                "id": video_id,
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
            try:
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
                                datetime.utcfromtimestamp(
                                    comment["time_parsed"]
                                ).strftime("%Y-%m-%dT%H:%M:%SZ")
                            ),
                        }
                    )

            except Exception as e:
                print(f"댓글 로드 실패: {video_id} - {e}")

            # 최종 데이터 리스트에 추가
            video_data_list.append(video_info)

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

        return {"statusCode": 200}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(str(e))}
