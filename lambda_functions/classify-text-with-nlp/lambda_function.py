import boto3
import openai
import os
import json
from io import StringIO

# S3 클라이언트 생성
s3_client = boto3.client("s3")

API_KEY = os.getenv("OPENAI_API_KEY")

# OpenAI API 키 설정
openai.api_key = API_KEY


def download_json_from_s3(bucket_name, json_key):
    """S3에서 JSON 파일 다운로드 후 데이터로 변환"""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=json_key)
        json_content = response["Body"].read().decode("utf-8")

        data = []
        for json_object in json_content.splitlines():
            if json_object.strip():
                data.append(json.loads(json_object))

        return data
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(str(e))}  # 오류 반환


def analyze_comments_batch(comments):
    """배치로 코멘트 감성 분석 및 주제 분류"""
    try:
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
        - `디자인`, `기능`, `신뢰성`, `기타` 중 하나 선택  

        3. 부주제 (Subcategory)
        - 주제와 연관된 부주제 하나 선택  

        ### 출력 형식 
        1. 감성: [감성 점수], 주제: [주제], 부주제: [부주제]  
        2. 감성: [감성 점수], 주제: [주제], 부주제: [부주제]  
        ...
        """

        response = openai.ChatCompletion.create(
            model="gpt-4o-mini", messages=[{"role": "user", "content": prompt}]
        )

        result_text = response.choices[0].message["content"].strip()
        results = []
        lines = result_text.split("\n")

        for i, line in enumerate(lines):
            try:
                parts = line.split(", ")
                sentiment = float(
                    parts[0].split("감성:")[1].strip().replace("[", "").replace("]", "")
                )
                topic = (
                    parts[1].split("주제:")[1].strip().replace("[", "").replace("]", "")
                )
                subtopic = (
                    parts[2]
                    .split("부주제:")[1]
                    .strip()
                    .replace("[", "")
                    .replace("]", "")
                )

                results.append(
                    {
                        "text": comments[i],
                        "sentiment": sentiment,
                        "topic": topic,
                        "subtopic": subtopic,
                        "date": None,
                        "like_count": None,
                        "source": "youtube",
                    }
                )
            except Exception as e:
                return {"statusCode": 500, "body": json.dumps(str(e))}  # 오류 반환

        return results

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(str(e))}  # 오류 반환


def lambda_handler(event, context):
    """AWS Lambda 핸들러"""
    try:
        BATCH_SIZE = 20

        input_date = event.get("input_date")
        car_name = event.get("car_name")
        input_file_key = event.get("input_file_key")

        if not input_date or not car_name:
            return {
                "statusCode": 400,
                "body": json.dumps("input_date and car_name are required"),
            }

        if not API_KEY:
            return {"statusCode": 400, "body": json.dumps("OPENAI_API_KEY is required")}

        year, month = input_date.split("-")[:2]

        BUCKET_NAME = "the-all-new-bucket"
        READ_OBJECT_KEY = input_file_key
        WRITE_OBJECT_KEY = f"{car_name}/{year}/{month}/classified_data.json"

        # S3에서 데이터 다운로드
        data = download_json_from_s3(BUCKET_NAME, READ_OBJECT_KEY)
        if isinstance(data, dict) and "statusCode" in data:
            return data  # 오류 발생 시 반환

        if not data:
            return {"statusCode": 500, "body": json.dumps("S3 데이터 다운로드 실패")}

        results = []

        # 배치 처리
        for i in range(0, len(data), BATCH_SIZE):
            batch = [item["text"] for item in data[i : i + BATCH_SIZE]]
            batch_results = analyze_comments_batch(batch)

            if isinstance(batch_results, dict) and "statusCode" in batch_results:
                return batch_results  # 오류 발생 시 반환

            for j, result in enumerate(batch_results):
                result["date"] = data[i + j]["date"]
                result["like_count"] = data[i + j]["like_count"]
                result["source"] = data[i + j]["source"]

            results.extend(batch_results)

        # 결과를 JSON 형식으로 변환
        json_output = ""
        for item in results:
            json_output += f'{{ "text": "{item["text"]}", "date": {item["date"]}, "like_count": "{item["like_count"]}", "source": "{item["source"]}", "sentiment": {item["sentiment"]}, "topic": "{item["topic"]}", "subtopic": "{item["subtopic"]}" }}\n'

        # 결과를 S3에 업로드
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=WRITE_OBJECT_KEY,
                Body=json_output,
                ContentType="application/json",
            )
        except Exception as e:
            return {
                "statusCode": 500,
                "body": json.dumps(str(e)),
            }  # S3 업로드 오류 반환

        return {"statusCode": 200, "body": json.dumps("Processing complete!")}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(str(e))}  # Lambda 내부 오류 반환


##sample input.json
# {
#     "input_date": "2025-01",
#     "car_name": "싼타페"
# }
