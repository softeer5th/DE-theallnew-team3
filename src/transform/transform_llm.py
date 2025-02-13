import json
import openai
import pandas as pd
import glob

from dotenv import load_dotenv
import os

API_KEY = os.getenv("OPENAI_KEY")

# .env 파일 활성화
load_dotenv()

# OpenAI API 키 설정
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

    ### 분석 기준  
    1. 감성 분석 (Sentiment Score)
    - 감성 점수를 0~1 사이의 값으로 제공해라.  
        - `0`은 가장 부정적인 감정  
        - `1`은 가장 긍정적인 감정  
        - `0.5`는 중립  

    2. 주제 분류 (Category)
    - 해당 코멘트가 관련된 주요 주제를 하나 선택해라:  
        - `디자인` (외관, 내부 디자인, 색상 등)  
        - `기능` (운전 편의성, 연비, 조작 장치 등)  
        - `신뢰성` (내구성, 고장, 안전성, 결함 등)  
        - `기타` (위 주제에 해당하지 않는 경우)  

    3. 부주제 (Subcategory)
    - 선택한 주요 주제와 해당 코멘트와 관련된 단어 하나를 부주제로 출력해주세요.  
    - 예시:  
        - `주제: 기능 → 부주제: 연비`  
        - `주제: 디자인 → 부주제: 색상`  

    ### 출력 형식 
    항상 아래와 같은 형식으로 결과를 제공해라:  
    1. 감성: [감성 점수], 주제: [주제], 부주제: [부주제]  
    2. 감성: [감성 점수], 주제: [주제], 부주제: [부주제]  
    3. 감성: [감성 점수], 주제: [주제], 부주제: [부주제]  
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
        ]
    )

    result_text = response.choices[0].message.content.strip()

    results = []
    lines = result_text.split("\n")
    for i, line in enumerate(lines):
        try:
            parts= line.split(", ")
            sentiment_str = parts[0].split("감성:")[1].strip().replace("[", "").replace("]", "")
            sentiment = float(sentiment_str)  # 감성 점수 변환
            topic = parts[1].split("주제:")[1].strip()  # 주제
            subtopic = parts[2].split("부주제:")[1].strip()  # 부주제

            results.append({
                "comment": comments[i],
                "sentiment": sentiment,
                "topic": topic,
                "subtopic": subtopic
            })
        except Exception as e:
            print(f"파싱 오류: {e}, 원본 응답: {line}")
            results.append({
                "comment": comments[i],
                "sentiment": "error",
                "topic": "error",
                "subtopic": "error"
            })

    return results


def transform_llm(input_date, car_name):

    # OpenAI API 키 설정 (본인의 키로 변경해야 함)
    client = openai.OpenAI(api_key=API_KEY)
    # 배치 크기 설정 (100개씩 API 호출)
    BATCH_SIZE = 20

    # json 파일 읽기

    json_files = glob.glob(f"data/transformed/part-*.json")

    #json_files = glob.glob(f"transformed.json") 테스트용
    frames = [pd.read_json(json_file, lines=True) for json_file in json_files]
    df = pd.concat(frames)

    # NaN 값을 0으로 변경
    df = df.fillna(0)

    # 댓글 가져오기
    comments = df["text"].tolist()

    # 결과를 저장할 리스트
    results = []

    # 배치로 API 호출
    for i in range(0, len(comments), BATCH_SIZE):
        batch = comments[i : i + BATCH_SIZE]
        batch_results = analyze_comments_batch(batch)
        results.extend(batch_results)
        print(f"{i + len(batch)}개 처리 완료")

    # 결과를 DataFrame으로 변환 후 기존 데이터와 합치기
    analysis_df = pd.DataFrame(results)
    df["sentiment"] = analysis_df["sentiment"]
    df["topic"] = analysis_df["topic"]
    df["subtopic"] = analysis_df["subtopic"]

    # 새로운 CSV 파일로 저장
    csv_output = f"data/youtube_{input_date}_{car_name}_llm.csv"

    #csv_output = f"output.csv" 테스트용
    df.to_csv(csv_output, index=False, encoding="utf-8-sig")

    # 🔹 완료 메시지 출력
    print(f"분석 결과가 CSV 파일로 저장되었습니다: {csv_output}")

#transform_llm("input_date", "car_name")
