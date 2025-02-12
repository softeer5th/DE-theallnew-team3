import json
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime

SOURCE_URL = "https://www.bobaedream.co.kr/search"


def collect_target_links(input_date, car_name):
    TARGET_URL = "https://www.bobaedream.co.kr/search"

    form_data = {
        "colle": "community",
        "searchField": "ALL",
        "page": 0,
        "sort": "DATE",
        "startDate": "",
        "keyword": car_name,
    }

    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.35 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.35"
    host = "www.bobaedream.co.kr"
    origin = "https://www.bobaedream.co.kr"
    referer = "https://www.bobaedream.co.kr/search"

    headers = {
        "User-Agent": ua,
        "Host": host,
        "Origin": origin,
        "Referer": referer,
    }

    YEAR, MONTH = input_date.split("-")

    page = 1
    target_links = []

    while True:
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
            date = link.find_all("span", "next")[1].text
            date = date.split(". ")
            target_year = int(YEAR[2:])
            target_month = int(MONTH)
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

    with open(f"data/bobae_{input_date}_{car_name}.csv", "w") as f:
        for link in target_links:
            f.write(link + "\n")

    return


def process_links(input_date, car_name):
    TARGET_URL = "https://www.bobaedream.co.kr"

    with open(f"data/bobae_{input_date}_{car_name}.csv", "r") as f:
        links = f.readlines()
        links = [link.strip() for link in links]

    json_data = []

    for link in links:
        try:
            res = requests.get(TARGET_URL + link)
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

            dt = countGroup[-1].strip()
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

        except Exception as e:
            print("failed: ", link)
            raise e

    json_data = json.dumps(json_data, ensure_ascii=False)
    with open(f"data/bobae_{input_date}_{car_name}.json", "w") as f:
        f.write(json_data)


def extract_bobae(input_date, car_name):
    print("Bobae - collect_target_links")
    collect_target_links(input_date, car_name)
    print("Bobae - process_links")
    process_links(input_date, car_name)
