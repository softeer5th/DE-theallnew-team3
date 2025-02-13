import json
import openai
import pandas as pd
import glob

from dotenv import load_dotenv
import os

API_KEY = os.getenv("OPENAI_KEY")

# .env 파일 활성화
load_dotenv()

# OpenAI API 키 설정 (본인의 키로 변경해야 함)
client = openai.OpenAI(api_key=API_KEY)
# 배치 크기 설정 (100개씩 API 호출)
BATCH_SIZE = 20


def analyze_comments_batch(comments):
    """여러 개의 코멘트를 한 번에 분석하는 배치 API 호출"""

    formatted_comments = "\n".join(
        [f"{i+1}. {comment}" for i, comment in enumerate(comments)]
    )

    prompt = f"""
    아래 자동차 관련 코멘트들의 감성을 분석하고 주제를 분류하세요:

    {formatted_comments}

    감성과 주제의 응답 형식은 다음 중 하나로 결정해주세요
    - 감성: positive / negative / neutral  
    - 주제: 디자인 / 기능 / 신뢰성 / 기타  

    결과는 항상 다음 형식으로 제공하세요:  
    1. 감성: [감성], 주제: [주제]  
    2. 감성: [감성], 주제: [주제]  
    ...
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": "You are an expert at analyzing car-related comments.",
            },
            {"role": "user", "content": prompt},
        ],
        max_tokens=500,
    )

    result_text = response.choices[0].message.content.strip()

    results = []
    lines = result_text.split("\n")
    for i, line in enumerate(lines):
        try:
            sentiment = line.split("감성:")[1].split(",")[0].strip()
            topic = line.split("주제:")[1].strip()
            results.append(
                {"comment": comments[i], "sentiment": sentiment, "topic": topic}
            )
        except Exception as e:
            print(f"파싱 오류: {e}, 원본 응답: {line}")
            results.append(
                {"comment": comments[i], "sentiment": "error", "topic": "error"}
            )

    return results


def transform_llm(input_date, car_name):

    # OpenAI API 키 설정 (본인의 키로 변경해야 함)
    client = openai.OpenAI(api_key=API_KEY)
    # 배치 크기 설정 (100개씩 API 호출)
    BATCH_SIZE = 20

    # 🔹 CSV 파일 읽기

    json_files = glob.glob(f"data/transformed/part-*.json")
    frames = [pd.read_json(json_file, lines=True) for json_file in json_files]
    df = pd.concat(frames)

    # 🔹 NaN 값을 0으로 변경
    df = df.fillna(0)

    # 🔹 댓글 가져오기
    comments = df["text"].tolist()

    # 🔹 결과를 저장할 리스트
    results = []

    # 🔹 배치로 API 호출
    for i in range(0, len(comments), BATCH_SIZE):
        batch = comments[i : i + BATCH_SIZE]
        batch_results = analyze_comments_batch(batch)
        results.extend(batch_results)
        print(f"{i + len(batch)}개 처리 완료")

    # 🔹 결과를 DataFrame으로 변환 후 기존 데이터와 합치기
    analysis_df = pd.DataFrame(results)
    df["sentiment"] = analysis_df["sentiment"]
    df["topic"] = analysis_df["topic"]

    # 🔹 새로운 CSV 파일로 저장
    csv_output = f"data/youtube_{input_date}_{car_name}_llm.csv"
    df.to_csv(csv_output, index=False, encoding="utf-8-sig")

    # 🔹 완료 메시지 출력
    print(f"분석 결과가 CSV 파일로 저장되었습니다: {csv_output}")


# 10:34 시작 1600개 배치 100개  - 3분

# 10:56 시작 400개 배치 20,
