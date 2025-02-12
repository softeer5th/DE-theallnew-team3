import json
import csv
import openai
from datetime import datetime

# OpenAI API 키 설정 (본인의 키로 변경해야 함)
client = openai.OpenAI(api_key=" ")

def sentiment_analysis(text):
    """OpenAI API를 이용한 감성 분석"""
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that analyzes sentiment."},
            {"role": "user", "content": f"이 텍스트의 감성을 분석해줘: {text}. 긍정이면 'positive', 부정이면 'negative', 중립이면 'neutral'이라고 답해줘."}
        ]
    )
    sentiment = response.choices[0].message.content.strip().lower()
    
    # 'neutral' 이거나 분석 불가 메시지는 제외
    if sentiment not in ["\'positive\'", "\'negative\'"]:
        return None
    return sentiment

def is_related_to_tucson(text):
    """투싼의 외관 디자인 또는 기능/편의성과 관련 있는지 여부 확인"""
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that filters text related to Tucson's exterior design or functionality."},
            {"role": "user", "content": f"다음 글이 투싼의 외관 디자인이나 기능/편의성과 관련 있으면 'yes', 아니면 'no'라고 답해줘: {text}"}
        ]
    )
    return response.choices[0].message.content.strip().lower() == "yes"

def process_json_file(json_file, output_csv):
    """JSON 파일을 처리하여 주제 관련 텍스트를 감성 분석 후 CSV로 저장"""
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)  # ✅ 리스트 형식으로 로드됨
    
    rows = []
    
    for item in data:  # ✅ 리스트 순회
        full_text = item.get("title", "") + " " + item.get("article", "")
        date = datetime.fromtimestamp(item.get("date", 0)).strftime('%Y-%m-%d')
 
        if is_related_to_tucson(full_text):
            sentiment = sentiment_analysis(full_text)
            print([date, full_text, sentiment])

            if sentiment:  # ✅ 'neutral'이 아니고 분석 가능할 경우만 저장
                rows.append([date, full_text, sentiment])
                #print([date, full_text, sentiment])

            # 댓글 확인
            for comment in item.get("comments", []):
                comment_text = comment.get("comment_content", "")
                comment_date = datetime.fromtimestamp(comment.get("comment_date", 0)).strftime('%Y-%m-%d')
                sentiment = sentiment_analysis(comment_text)
                print([comment_date, comment_text, sentiment])
                if sentiment:  # ✅ 'neutral'이 아니고 분석 가능할 경우만 저장
                    rows.append([comment_date, comment_text, sentiment])
                    #print([comment_date, comment_text, sentiment])
                
                # if is_related_to_tucson(comment_text):
                #     sentiment = sentiment_analysis(comment_text)
                #     if sentiment:  # ✅ 'neutral'이 아니고 분석 가능할 경우만 저장
                #         rows.append([comment_date, comment_text, sentiment])
    
    # CSV 파일 저장
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["날짜", "텍스트", "감성"])
        writer.writerows(rows)
    
    print(f"✅ CSV 파일이 저장되었습니다: {output_csv}")

# 사용 예시
print('start')
process_json_file("투싼_2024-02.json", "tucson_analysis1.csv")
