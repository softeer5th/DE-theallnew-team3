import requests
import time
from bs4 import BeautifulSoup
import json
import multiprocessing
import random
from datetime import datetime

INPUT_DATE = "2025-01"
KEYWORD = "투싼"


def collect_links(year, month, keyword):
    TARGET_URL = "https://www.bobaedream.co.kr/search"

    form_data = {
        "colle": "community",
        "searchField": "ALL",
        "page": 0,
        "sort": "DATE",
        "startDate": "",
        "keyword": keyword,
    }

    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.35 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.35"
    accept = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    host = "www.bobaedream.co.kr"
    origin = "https://www.bobaedream.co.kr"
    referer = "https://www.bobaedream.co.kr/search"

    headers = {
        "User-Agent": ua,
        "Accept": accept,
        "Host": host,
        "Origin": origin,
        "Referer": referer,
    }

    page = 1
    target_links = []

    while True:
        print(f"page: {page}")
        form_data["page"] = page
        res = requests.post(
            TARGET_URL,
            data=form_data,
            headers=headers,
        )

        soup = BeautifulSoup(res.content, "html.parser")
        links = soup.find("div", "search_Community")
        links = links.find("ul")
        links = links.find_all("li")

        stop_flag = False

        for link in links:
            date = link.find("span", "next")[1].text
            date = date.split(". ")
            target_year = int(year[2:])
            target_month = int(month)
            link_year = int(date[0])
            link_month = int(date[1])
            if link_year == target_year and link_month == target_month:
                target_links.append(link.find("a")["href"])
            elif link_year < target_year or link_month < target_month:
                stop_flag = True
                break

        page += 1
        if stop_flag:
            break
        time.sleep(0.1)

    # write file csv
    with open(f"data/bobae_{year}_{month}_{keyword}.csv", "w", encoding="utf-8") as f:
        for link in target_links:
            f.write(f"{link}\n")

    return target_links


def process_links(year, month, keyword):
    URL = "https://www.bobaedream.co.kr"

    with open(f"data/bobae_{year}_{month}_{keyword}.csv", "r", encoding="utf-8") as f:
        links = f.readlines()

    count = 0
    json_data = []

    for link in links:
        try:
            res = requests.get(URL + link)
            data = {}

            soup = BeautifulSoup(
                res.content,
                "html.parser",
            )

            profile = soup.find("div", "writerProfile")
            if profile is None:
                # 삭제된 게시글
                continue
            title = profile.find("dt").attrs["title"]
            data["title"] = title

            nickname = profile.find("a", "nickName")
            if nickname is None:
                data["nickname"] = ""
            else:
                data["nickname"] = nickname.text

            content02 = soup.find("div", "content02")
            content = content02.find("div", "bodyCont")
            if content is None:
                content = content02.find("div", "mycarCont")

            data["article"] = content.getText(separator="\n", strip=True)

            countGroup = soup.find("span", "countGroup")
            countGroup = countGroup.text
            countGroup = countGroup.split("|")

            data["view_count"] = countGroup[0].strip().split(" ")[1]
            data["like_count"] = countGroup[1].strip().split(" ")[2]

            dt = countGroup[2].strip()
            dt = dt.replace("\xa0", " ")
            dt = dt.split(" ")
            dt = dt[0] + " " + dt[2]

            data["date"] = int(datetime.strptime(dt, "%Y.%m.%d %H:%M").timestamp())

            data["comments"] = []
            comments = soup.find("div", "commentlistbox")
            comments = comments.find("ul", "basiclist")
            try:
                comments = comments.find_all("li")
            except:
                # 댓글이 없음
                comments = []

            for c in comments:
                try:
                    comment = {}
                    comment["comment_content"] = c.find("dd").text
                    dt = c.find("dt")
                    comment["comment_nickname"] = dt.find("span", "author").text
                    comment["comment_date"] = int(
                        datetime.strptime(
                            dt.find("span", "date").text, "%y.%m.%d %H:%M"
                        ).timestamp()
                    )

                    updown = c.find("div", "updownbox")
                    updown = updown.find_all("a")
                    comment["comment_likeCount"] = updown[0].text.split(" ")[1]
                    comment["comment_dislikeCount"] = updown[1].text.split(" ")[1]

                    data["comments"].append(comment)
                except:
                    pass
            data["comment_count"] = str(len(data["comments"]))

            json_data.append(data)

            count += 1
            if count % 100 == 0:
                print(f"{count}개 크롤링 완료")
        except Exception as e:
            print(count, link)
            raise e

    with open(f"data/data_{year}_{month}_{keyword}.json", "w", encoding="UTF-8") as f:
        json.dump(json_data, f, ensure_ascii=False)


def main():
    year, month = INPUT_DATE.split("-")
    collect_links(year, month, KEYWORD)
    process_links(year, month, KEYWORD)


if __name__ == "__main__":
    main()
