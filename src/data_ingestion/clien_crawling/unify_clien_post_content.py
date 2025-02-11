import json
import datetime

def unify_clien_post_content(input_json: str, output_json: str):
    with open(input_json, "r", encoding="utf-8") as f:
        posts = json.load(f)

    unified_posts = []
    for post in posts:
        title = post["post_title"]
        nickname = post["post_nickname"]
        article = post["post_article"]
        like_count = int(post["post_symph"].replace(",", ""))
        view_count = int(post["post_view_count"].replace(",", ""))

        date_str = post["post_date"].split('\n')[0].strip()
        date = int(datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").timestamp())

        comment_count = int(post["comment_count"].replace(",", ""))
        comments = post["comments"]

        unified_comments = []
        for comment in comments:
            comment_nickname = comment["comment_nickname"]
            comment_content = comment["comment_content"]
            comment_like_count = int(comment["comment_symph"].replace(",", ""))
            comment_dislike_count = 0
            comment_date_str = comment["comment_date"].split('/')[0].strip()
            comment_date = int(datetime.datetime.strptime(comment_date_str, "%Y-%m-%d %H:%M:%S").timestamp())

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

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(unified_posts, f, ensure_ascii=False, indent=4)
    print(f"{output_json} 저장 성공")


if __name__ == '__main__':
    input_file = 'data/clien_posts.json'
    output_file = 'data/unified_clien_posts.json'
    unify_clien_post_content(input_file, output_file)