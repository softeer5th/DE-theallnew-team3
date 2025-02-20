import json
import boto3
import requests
from bs4 import BeautifulSoup
from datetime import datetime


def lambda_handler(event, context):
    TARGET_URL = "https://www.bobaedream.co.kr"

    input_date = event["input_date"]
    car_name = event["car_name"]

    if input_date == "" or car_name == "":
        return {
            "statusCode": 400,
            "body": json.dumps("input_date and car_name are required"),
        }

    year, month, day = input_date.split("-")

    BUCKET_NAME = "the-all-new-bucket"
    READ_OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/bobae_target_links.csv"
    WRITE_OBJECT_KEY = f"{car_name}/{year}/{month}/{day}/bobae_raw.json"

    s3 = boto3.client("s3")
    s3.download_file(
        BUCKET_NAME, READ_OBJECT_KEY, f"/tmp/bobae_{input_date}_{car_name}.csv"
    )

    with open(f"/tmp/bobae_{input_date}_{car_name}.csv", "r") as f:
        links = f.readlines()
        links = [link.strip() for link in links]

    json_data = []
    failed_links = []

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

            data["car_name"] = car_name
            data["source"] = "bobae"
            data["id"] = "bobae_" + link.split("=")[-1]

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

            view_count = 0
            try:
                view_count = int(countGroup[0].strip().split(" ")[1].replace(",", ""))
            except:
                view_count = 0
            data["view_count"] = view_count

            like_count = 0
            try:
                like_count = int(countGroup[1].strip().split(" ")[2].replace(",", ""))
            except:
                like_count = 0
            data["like_count"] = like_count

            data["dislike_count"] = 0

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

                    like_count = 0
                    try:
                        like_count = int(updown[0].text.split(" ")[1].replace(",", ""))
                    except:
                        like_count = 0
                    comment["comment_like_count"] = like_count

                    dislike_count = 0
                    try:
                        dislike_count = int(
                            updown[1].text.split(" ")[1].replace(",", "")
                        )
                    except:
                        dislike_count = 0
                    comment["comment_dislike_count"] = dislike_count

                    data["comments"].append(comment)
                except:
                    raise Exception("comment error")
            data["comment_count"] = len(data["comments"])

            json_data.append(data)

        except Exception as e:
            print("failed: ", link, e)
            failed_links.append(link)

    json_data = json.dumps(json_data, ensure_ascii=False)
    with open(f"/tmp/bobae_{input_date}_{car_name}.json", "w") as f:
        f.write(json_data)

    s3.upload_file(
        f"/tmp/bobae_{input_date}_{car_name}.json",
        BUCKET_NAME,
        WRITE_OBJECT_KEY,
    )

    if len(failed_links) > 0:
        with open(f"/tmp/bobae_{input_date}_{car_name}_failed_links.csv", "w") as f:
            for link in failed_links:
                f.write(link + "\n")

        s3.upload_file(
            f"/tmp/bobae_{input_date}_{car_name}_failed_links.csv",
            BUCKET_NAME,
            f"{car_name}/{year}/{month}/{day}/failed/bobae_target_links.csv",
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"failed": failed_links}),
        }

    return {"statusCode": 200}
