import json
import requests
from bs4 import BeautifulSoup

def fetch_urls_from_json(json_file):
    """JSON 파일에서 URL 목록을 읽어오는 함수"""
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [entry["url"] for entry in data if "url" in entry]

def extract_comment(comment_div):
    comment_info_div = comment_div.find("div", class_="comment_info")

    comment_nickname = comment_info_div.find("span", class_="nickname").text.strip()
    comment_date = comment_info_div.find("span", class_="timestamp").text.strip()
    comment_symph = comment_div.find("div", class_="comment_content_symph").find("strong").text.strip()
    comment_content = comment_div.find("div", class_="comment_content").get_text(separator="\n", strip=True)
    
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
    post_article = post_view_div.find("div", class_="post_article").get_text(separator="\n", strip=True)

    post_comment_div = content_view_div.find("div", class_="post_comment")
    comment_head_div = post_comment_div.find("div", class_="comment_head")

    comment_count = comment_head_div.find("strong").text.strip()

    comment_divs = post_comment_div.find("div", class_="comment").select("div.comment_row:not(.blocked)")

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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }
    clien_posts_json = 'data/clien_posts.json'

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

    with open(clien_posts_json, "w", encoding="utf-8") as f:
        json.dump(posts, f, ensure_ascii=False, indent=4)
    print(f"{clien_posts_json} 저장 성공")

if __name__ == '__main__':
    json_file = "data/clien_post_urls.json"
    urls = fetch_urls_from_json(json_file)
    send_requests(urls)
