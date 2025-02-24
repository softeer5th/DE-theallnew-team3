import json
import boto3
import requests
from bs4 import BeautifulSoup
from datetime import datetime

TARGET_URL = "https://www.clien.net/service/search"


def extract_comment(comment_div):
    comment_info_div = comment_div.find("div", class_="comment_info")

    comment_nickname = comment_info_div.find("span", class_="nickname").text.strip()
    comment_date = comment_info_div.find("span", class_="timestamp").text.strip()
    comment_symph = (
        comment_div.find("div", class_="comment_content_symph")
        .find("strong")
        .text.strip()
    )
    comment_content = comment_div.find("div", class_="comment_content").get_text(
        separator="\n", strip=True
    )

    return {
        "comment_nickname": comment_nickname,
        "comment_date": comment_date,
        "comment_symph": comment_symph,
        "comment_content": comment_content,
    }


def extract_nav_content(url, html):
    """HTML에서 class='nav-content'인 div 태그를 추출하는 함수"""
    soup = BeautifulSoup(html, "html.parser")
    content_view_div = soup.find("div", class_="content_view")

    post_title_div = content_view_div.find("div", class_="post_title")
    post_subject_div = post_title_div.find("h3", class_="post_subject")

    post_title = post_subject_div.find("span", class_=lambda c: c is None).text.strip()
    post_symph_div = post_title_div.find("a", class_="post_symph")
    post_symph = post_symph_div.find("span").text if post_symph_div else "0"

    post_author_div = content_view_div.find("div", class_="post_author")
    post_view_count = post_author_div.find("span", class_="view_count").text.strip()
    post_date = post_author_div.find("span", class_="date").text.strip()

    post_info_div = content_view_div.find("div", class_="post_info")
    post_nickname = post_info_div.find("span", "nickname").text.strip()

    post_view_div = content_view_div.find("div", class_="post_view")
    post_article = post_view_div.find("div", class_="post_article").get_text(
        separator="\n", strip=True
    )

    post_comment_div = content_view_div.find("div", class_="post_comment")
    comment_head_div = post_comment_div.find("div", class_="comment_head")

    comment_count = comment_head_div.find("strong").text.strip()

    comment_divs = post_comment_div.find("div", class_="comment").select(
        "div.comment_row:not(.blocked)"
    )

    comments = [extract_comment(comment_div) for comment_div in comment_divs]

    post = {
        "url": url,
        "post_title": post_title,
        "post_symph": post_symph,
        "post_view_count": post_view_count,
        "post_date": post_date,
        "post_nickname": post_nickname,
        "post_article": post_article,
        "comment_count": comment_count,
        "comments": comments,
    }
    return post


def send_request(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }

    response = requests.get(url, headers=headers, timeout=10)
    return extract_nav_content(url, response.text)


def unify_clien_post_content(post, car_name):
    url = post["url"]
    title = post["post_title"]
    nickname = post["post_nickname"]
    article = post["post_article"]
    like_count = int(post["post_symph"].replace(",", ""))
    view_count = int(post["post_view_count"].replace(",", ""))

    date_str = post["post_date"].split("\n")[0].strip()
    date = int(datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").timestamp())

    comment_count = int(post["comment_count"].replace(",", ""))
    comments = post["comments"]

    unified_comments = []
    for comment in comments:
        comment_nickname = comment["comment_nickname"]
        comment_content = comment["comment_content"]
        comment_like_count = int(comment["comment_symph"].replace(",", ""))
        comment_dislike_count = 0
        comment_date_str = comment["comment_date"].split("/")[0].strip()
        comment_date = int(
            datetime.strptime(comment_date_str, "%Y-%m-%d %H:%M:%S").timestamp()
        )

        unified_comment = {
            "comment_nickname": comment_nickname,
            "comment_content": comment_content,
            "comment_like_count": comment_like_count,
            "comment_dislike_count": comment_dislike_count,
            "comment_date": comment_date,
        }
        unified_comments.append(unified_comment)

    unified_post = {
        "car_name": car_name,
        "id": "clien_" + url.split("?")[0].split("/")[-1],
        "source": "clien",
        "title": title,
        "nickname": nickname,
        "article": article,
        "like_count": like_count,
        "dislike_count": 0,
        "view_count": view_count,
        "date": date,
        "comment_count": comment_count,
        "comments": unified_comments,
    }
    return unified_post


def lambda_handler(event, context):
    input_date = event["input_date"]
    car_name = event["car_name"]

    if not input_date or not car_name:
        raise Exception("input_date and car_name are required")

    year, month, day = input_date.split("-")

    BUCKET_NAME = "the-all-new-bucket"
    READ_OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/target/clien_failed.csv"
    FAILED_OBJECT_KEY = (
        f"{car_name}/{year}/{month}/{day}/target/clien_failed_failed.csv"
    )
    WRITE_OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/raw/clien_recovery.json"

    s3 = boto3.client("s3")
    s3.download_file(
        BUCKET_NAME, READ_OBJECT_KEY, f"/tmp/clien_{input_date}_{car_name}.csv"
    )

    with open(f"/tmp/clien_{input_date}_{car_name}.csv", "r", encoding="utf-8") as f:
        urls = f.readlines()

    results = []
    failed_urls = []

    for url in urls:
        try:
            post = send_request(url)
            unified_post = unify_clien_post_content(post, car_name)
            results.append(unified_post)
        except Exception as e:
            print(f"Error processing URL: {url}, Error: {e}")
            failed_urls.append(url)

    with open(f"/tmp/clien_{input_date}_{car_name}.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

    s3.upload_file(
        f"/tmp/clien_{input_date}_{car_name}.json",
        BUCKET_NAME,
        WRITE_OBJECT_KEY,
    )

    if len(failed_urls) > 0:
        with open(f"/tmp/clien_{input_date}_{car_name}_failed_urls.csv", "w") as f:
            for url in failed_urls:
                f.write(url + "\n")

        s3.upload_file(
            f"/tmp/clien_{input_date}_{car_name}_failed_urls.csv",
            BUCKET_NAME,
            FAILED_OBJECT_KEY,
        )

        return {
            "statusCode": 200,
            "failed": failed_urls,
        }

    return {"statusCode": 200}
