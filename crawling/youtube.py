import os
import json
import time
from datetime import datetime
from googleapiclient.discovery import build
from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_POPULAR

# API 키 입력
API_KEY = "API key"  # YouTube API 키 입력

# 검색 키워드 및 월 정보 입력
search_query = "투싼"
month_filter = "2024-02"  # YYYY-MM 형식 (검색 대상 월)

def get_next_month(month_filter):
    # 입력값을 연도와 월로 분리
    year, month = map(int, month_filter.split("-"))
    
    # 현재 날짜를 기준으로 다음 달 계산
    if month == 12:
        year += 1
        month = 1
    else:
        month += 1

    # YYYY-MM 형식의 문자열로 반환
    return f"{year}-{month:02d}"

# 검색 기간 설정
published_after = f"{month_filter}-01T00:00:00Z"  # 해당 월 1일 시작
published_before = f"{get_next_month(month_filter)}-01T00:00:00Z"  # 해당 월 말일 종료

# YouTube API 클라이언트 생성
youtube = build("youtube", "v3", developerKey=API_KEY)

# 검색 요청 (조회수 많은 순으로 정렬)
response = youtube.search().list(
    q=search_query,
    part="snippet",
    maxResults=10,  # 최대 10개 검색 (테스트 후 증가 가능)
    order="viewCount",
    type="video",
    publishedAfter=published_after,
    publishedBefore=published_before,
    regionCode="KR",
).execute()

# 영상 ID 목록
video_ids = [item["id"]["videoId"] for item in response["items"]]

# 상세 정보 요청 (조회수, 좋아요 수, 댓글 수, 본문 내용)
stats_response = youtube.videos().list(
    part="statistics,snippet",
    id=",".join(video_ids)
).execute()

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
        "title": snippet.get("title", "제목 없음"),
        "nickname": snippet.get("channelTitle", "알 수 없음"),
        "article": snippet.get("description", "설명 없음"),
        "like_count": statistics.get("likeCount", "0"),
        "view_count": statistics.get("viewCount", "0"),
        "date": convert_to_timestamp(snippet.get("publishedAt", "")),  # 타임스탬프로 변환
        "comment_count": statistics.get("commentCount", "0"),
        "comments": []  # 댓글 리스트 초기화
    }

    # 댓글 가져오기
    try:
        comments = downloader.get_comments_from_url(f"https://www.youtube.com/watch?v={video_id}", sort_by=SORT_BY_POPULAR)
        
        for comment in comments:
            video_info["comments"].append({
                "comment_nickname": comment.get("author", ""),
                "comment_content": comment.get("text", ""),
                "comment_like_count": comment.get("votes", "0"),
                "comment_dislike_count": "0",  # YouTube API에서 싫어요 수 제공 안 함
                "comment_date": convert_to_timestamp(datetime.utcfromtimestamp(comment["time_parsed"]).strftime("%Y-%m-%dT%H:%M:%SZ")),
            })

    except Exception as e:
        print(f"댓글 로드 실패: {video_id} - {e}")

    # 최종 데이터 리스트에 추가
    video_data_list.append(video_info)

# JSON 파일 저장 (YYYY-MM.json)
json_filename = f"{search_query}_{month_filter}.json"
with open(json_filename, "w", encoding="utf-8") as jsonfile:
    json.dump(video_data_list, jsonfile, ensure_ascii=False, indent=4)

print(f"\n 데이터 저장 완료: {json_filename}")
