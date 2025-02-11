import requests
from bs4 import BeautifulSoup

def google_search_url_crawling(q:str, tbs:str, start:str):
    session = requests.Session()
    search_url = f'https://www.google.com/search'
    params = {
        'q': q,
        'tbs': tbs,
        'start': start
    }
    cookies = {'CONSENT' : 'YES'}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.google.com/'
    }
    return session.get(search_url, params=params, headers=headers, cookies=cookies)

def clien_url_crawling(car_name:str, cd_min:str, cd_max:str, start:str):
    q = f'site:www.clien.net+{car_name}'
    tbs = f'cdr:1,cd_min:{cd_min},cd_max:{cd_max}'
    return google_search_url_crawling(q, tbs, start)


if __name__ == '__main__':
    print(clien_url_crawling('투싼', '20200101', '20201231', 0).text)