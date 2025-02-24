import csv
import random
import uuid
from datetime import datetime, timedelta


# 작은 수가 더 자주 나오도록 하는 함수
def skewed_random_int(max_val, exponent=2):
    """
    exponent 값을 높이면 낮은 값이 더 자주 나오고,
    exponent 값을 낮추면 균등 분포에 가까워집니다.
    """
    return int((random.random() ** exponent) * max_val)


# ingestion_date 범위 설정 (2024-01-01 ~ 2025-02-28)
start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 2, 28)
date_range = (end_date - start_date).days

sources = ["youtube", "bobae", "clien"]
source_weights = [0.7, 0.2, 0.1]

ages = ["2030", "4050"]
age_weights = [0.3, 0.7]

# 생성할 데이터 개수
num_rows = 150000

data = []
for _ in range(num_rows):
    # 고유 id 생성
    post_id = str(uuid.uuid4())
    comment_id = str(uuid.uuid4())

    # 작은 값이 더 자주 나오도록 숫자 필드 생성
    like_cnt = skewed_random_int(150, exponent=2)
    view_cnt = skewed_random_int(1000, exponent=2)
    comment_cnt = skewed_random_int(100, exponent=2)

    # dislike_cnt: 90% 확률로 0, 나머지 경우 1~50 사이의 값
    if random.random() < 0.9:
        dislike_cnt = 0
    else:
        dislike_cnt = random.randint(1, 10)

    # ingestion_date 생성 (랜덤 날짜)
    random_days = random.randint(0, date_range)
    ingestion_date = start_date + timedelta(days=random_days)

    car_name = "Santafe"
    source = random.choices(sources, weights=source_weights, k=1)[0]
    age = random.choices(ages, weights=age_weights, k=1)[0]

    data.append(
        {
            "post_id": post_id,
            "comment_id": comment_id,
            "like_cnt": like_cnt,
            "dislike_cnt": dislike_cnt,
            "view_cnt": view_cnt,
            "comment_cnt": comment_cnt,
            "ingestion_date": int(ingestion_date.timestamp()),
            "car_name": car_name,
            "source": source,
            "age": age,
        }
    )

# CSV 파일로 저장
csv_file = "random_data2.csv"
with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)

print(f"CSV 파일 '{csv_file}' 생성 완료!")
