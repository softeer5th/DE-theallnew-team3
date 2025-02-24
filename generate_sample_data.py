import csv
import random
import string
import uuid
from datetime import datetime, timedelta


# 랜덤 문자열 생성 함수 (예: 10~20자)
def random_string(min_len=10, max_len=20):
    length = random.randint(min_len, max_len)
    letters = string.ascii_letters + string.digits + " "  # 공백 포함
    return "".join(random.choices(letters, k=length))


# ingestion_date 범위 설정 (2024-01-01 ~ 2025-02-28)
start_date = datetime(2023, 9, 1)
end_date = datetime(2025, 2, 28)
date_range = (end_date - start_date).days

# 카테고리 및 해당 키워드 맵핑
category_keyword = {
    "디자인": [
        "고급스러움",
        "미래지향적",
        "스포티함",
        "아웃도어",
        "테일램프",
        "후면부",
    ],
    "성능": ["엔진", "주행", "연비"],
    "안전성": ["안전", "충돌 방지", "주행 보조"],
    "편의성": ["실내공간", "연결성", "조작성", "수납공간"],
    "인테리어": ["내장재", "디스플레이", "색상"],
}

# 카테고리-키워드별 sentiment score 분포 설정 (최소값, 최대값)
sentiment_distribution = {
    "디자인": {
        "고급스러움": (0.3, 0.6),
        "미래지향적": (0.6, 0.8),
        "스포티함": (0.3, 0.6),
        "아웃도어": (0.4, 0.9),
        "테일램프": (0.1, 0.5),
        "후면부": (0.1, 0.5),
    },
    "성능": {
        "엔진": (0.3, 0.9),
        "주행": (0.2, 0.8),
        "연비": (0.2, 0.9),
    },
    "안전성": {
        "안전": (0.2, 0.8),
        "충돌 방지": (0.3, 0.8),
        "주행 보조": (0.3, 0.8),
    },
    "편의성": {
        "실내공간": (0.3, 0.6),
        "연결성": (0.3, 0.6),
        "조작성": (0.3, 0.6),
        "수납공간": (0.3, 0.6),
    },
    "인테리어": {
        "내장재": (0.3, 0.7),
        "디스플레이": (0.3, 0.9),
        "색상": (0.3, 0.7),
    },
}

# 옵션 리스트 및 가중치 설정
types = ["comment", "post"]
type_weights = [0.9, 0.1]  # comment 60%, post 40%

categories = list(category_keyword.keys())
category_weights = [0.3, 0.2, 0.1, 0.2, 0.2]  # 예시 비율

car_names = "Santafe"

sources = ["youtube", "bobae", "clien"]
source_weights = [0.7, 0.2, 0.1]

ages = ["2030", "4050"]
age_weights = [0.5, 0.5]

# 생성할 데이터 개수
num_rows = 30000


# 작은 수가 더 자주 나오도록 하는 함수
def skewed_random_int(max_val, exponent=2):
    """
    exponent 값을 높이면 낮은 값이 더 자주 나오고,
    exponent 값을 낮추면 균등 분포에 가까워집니다.
    """
    return int((random.random() ** exponent) * max_val)


data = []
for _ in range(num_rows):
    post_id = str(uuid.uuid4())
    comment_id = str(uuid.uuid4())

    data_type = random.choices(types, weights=type_weights, k=1)[0]
    sentence = random_string()
    category = random.choices(categories, weights=category_weights, k=1)[0]
    keyword = random.choice(category_keyword[category])

    # 선택된 카테고리와 키워드에 맞춰 sentiment score 범위 지정
    min_sent, max_sent = sentiment_distribution[category][keyword]
    sentiment_score = round(random.uniform(min_sent, max_sent), 4)

    ingestion_date = start_date + timedelta(days=random.randint(0, date_range))

    car_name = "Santafe"
    source = random.choices(sources, weights=source_weights, k=1)[0]
    age = random.choices(ages, weights=age_weights, k=1)[0]

    like_cnt = skewed_random_int(200, exponent=2)
    view_cnt = skewed_random_int(3000, exponent=2)
    comment_cnt = skewed_random_int(200, exponent=2)

    if random.random() < 0.9:
        dislike_cnt = 0
    else:
        dislike_cnt = random.randint(1, 10)

    data.append(
        {
            "post_id": post_id,
            "comment_id": comment_id,
            "type": data_type,
            "sentence": sentence,
            "category": category,
            "keyword": keyword,
            "sentiment_score": sentiment_score,
            "ingestion_date": ingestion_date.strftime("%Y-%m-%d"),
            "car_name": car_name,
            "source": source,
            "age": age,
            "like_cnt": like_cnt,
            "dislike_cnt": dislike_cnt,
            "view_cnt": view_cnt,
            "comment_cnt": comment_cnt,
        }
    )

# CSV 파일로 저장
csv_file = "random_data.csv"
with open(csv_file, mode="a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)

print(f"CSV 파일 '{csv_file}' 생성 완료!")
