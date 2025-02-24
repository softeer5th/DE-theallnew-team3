import requests
import json
import datetime
from bs4 import BeautifulSoup

TARGET_URL = "https://www.clien.net/service/search"


def collect_target_urls(input_date, car_name):
    params = {
        "q": car_name,
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

    with open(f"data/clien_{input_date}_{car_name}.csv", "w", encoding="utf-8") as f:
        for url in urls:
            f.write(url + "\n")


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


def send_requests(urls):
    """각 URL에 GET 요청을 보내고 'nav-content' div를 추출하는 함수"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }

    posts = []
    for idx, url in enumerate(urls):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                posts.append(extract_nav_content(url, response.text))
            else:
                print(f"요청 실패: {url} (상태 코드: {response.status_code})")
        except requests.RequestException as e:
            print(f"요청 에러: {url} (에러: {e})")

    return posts


def unify_clien_post_content(posts):

    unified_posts = []
    for post in posts:
        title = post["post_title"]
        nickname = post["post_nickname"]
        article = post["post_article"]
        like_count = int(post["post_symph"].replace(",", ""))
        view_count = int(post["post_view_count"].replace(",", ""))

        date_str = post["post_date"].split("\n")[0].strip()
        date = int(
            datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").timestamp()
        )

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
                datetime.datetime.strptime(
                    comment_date_str, "%Y-%m-%d %H:%M:%S"
                ).timestamp()
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
            "title": title,
            "nickname": nickname,
            "article": article,
            "like_count": like_count,
            "view_count": view_count,
            "date": date,
            "comment_count": comment_count,
            "comments": unified_comments,
        }
        unified_posts.append(unified_post)

    return unified_posts


def process_urls(input_date, car_name):
    with open(f"data/clien_{input_date}_{car_name}.csv", "r", encoding="utf-8") as f:
        urls = f.readlines()

    posts = send_requests(urls)
    unified_posts = unify_clien_post_content(posts)

    with open(f"data/clien_{input_date}_{car_name}.json", "w", encoding="utf-8") as f:
        json.dump(unified_posts, f, ensure_ascii=False, indent=4)


def extract_clien(input_date, car_name):
    print("Clien - collect_target_urls")
    collect_target_urls(input_date, car_name)
    print("Clien - process_urls")
    process_urls(input_date, car_name)
