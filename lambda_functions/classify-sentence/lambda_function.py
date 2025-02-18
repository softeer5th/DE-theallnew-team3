import openai
import os
import json
import awswrangler as wr
import pandas as pd

API_KEY = os.getenv("OPENAI_API_KEY")

# OpenAI API 키 설정
openai.api_key = API_KEY


def analyze_comments_batch(comments):
    """배치로 코멘트 감성 분석 및 주제 분류"""
    try:
        formatted_comments = "\n".join(
            [f"{i+1}. {comment[1]}" for i, comment in enumerate(comments)]
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
                        "sentence_id": comments[i][0],
                        "sentiment_score": sentiment,
                        "category": topic,
                        "keyword": subtopic,
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
        object_key = event.get("object_key")

        if not input_date or not car_name:
            return {
                "statusCode": 400,
                "body": json.dumps("input_date and car_name are required"),
            }

        if not API_KEY:
            return {"statusCode": 400, "body": json.dumps("OPENAI_API_KEY is required")}

        year, month = input_date.split("-")[:2]

        BUCKET_NAME = "the-all-new-bucket"
        READ_OBJECT_KEY = f"{car_name}/{year}/{month}/sentence_data/{object_key}"
        WRITE_OBJECT_KEY = f"{car_name}/{year}/{month}/classified/{object_key}"

        df = wr.s3.read_parquet(
            path=f"s3://{BUCKET_NAME}/{READ_OBJECT_KEY}",
            columns=["sentence_id", "text"],
        )
        data = df.to_dict(orient="records")

        results = []

        # 배치 처리
        for i in range(0, len(data), BATCH_SIZE):
            batch = [
                (item["sentence_id"], item["text"]) for item in data[i : i + BATCH_SIZE]
            ]
            batch_results = analyze_comments_batch(batch)
            print(f"batch {i} complete")

            if isinstance(batch_results, dict) and "statusCode" in batch_results:
                return batch_results  # 오류 발생 시 반환

            results.extend(batch_results)

        results_df = pd.DataFrame(results)
        wr.s3.to_parquet(df=results_df, path=f"s3://{BUCKET_NAME}/{WRITE_OBJECT_KEY}")

        return {"statusCode": 200, "body": json.dumps("Processing complete!")}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(str(e))}  # Lambda 내부 오류 반환
