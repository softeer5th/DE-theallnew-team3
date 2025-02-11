import requests
import json
from bs4 import BeautifulSoup

def clien_post_url_crawling(car_name: str, pnum: int):
    session = requests.Session()
    search_url = 'https://www.clien.net/service/search'
    params = {
        'q': car_name,
        'p': pnum,
        'sort': 'recency',
        'boardCd': '',
        'isBoard': 'false'
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }
    html_content = session.get(search_url, params=params, headers=headers).text

    soup = BeautifulSoup(html_content, "html.parser")
    
    return [{"url": 'https://www.clien.net' + a.get('href')} for a in soup.select("div.total_search a[data-role='list-title-text']") if a.get('href')]

if __name__ == '__main__':
    clien_post_url_json = 'data/clien_post_urls.json'
    href_links = []
    for i in range(50):
        href_links.extend(clien_post_url_crawling('투싼', i))
    
    with open(clien_post_url_json, "w", encoding="utf-8") as f:
        json.dump(href_links, f, ensure_ascii=False, indent=4)

    print("JSON 파일이 성공적으로 저장되었습니다.")
