import openai

# OpenAI API 키 설정 (본인의 키로 변경해야 함)
client = openai.OpenAI(api_key=" ")


def analyze_comment(comment):
    """사용자 코멘트에 대한 감성 분석과 주제 분류"""
    
    prompt = f"""
    아래 코멘트의 감성을 분석하고 주제를 분류하세요:

    코멘트: "{comment}"

    1. 감성 분석 결과: 
       - 긍정적인 경우 'positive'
       - 부정적인 경우 'negative'
       - 중립이거나 확실하지 않은 경우 'neutral'
    
    2. 주제 분류:
       - '디자인' (외관, 내부 디자인, 심미성 등)
       - '기능' (운전 편의성, 연비, 장치 등)
       - '신뢰성' (내구성, 고장, 안전성, 결함 등)
       - 해당되지 않는 경우 '기타'

    응답 형식: 감성: [감성 분석 결과], 주제: [주제]
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": "You are an expert at analyzing car-related comments."},
                  {"role": "user", "content": prompt}]
    )

    result = response.choices[0].message.content.strip()
    
    # 결과를 파싱하여 감성 및 주제 추출
    sentiment, topic = None, None
    if "감성:" in result and "주제:" in result:
        try:
            sentiment = result.split("감성:")[1].split(",")[0].strip()
            topic = result.split("주제:")[1].strip()
        except Exception as e:
            print(f"파싱 오류: {e}")

    return sentiment, topic

# 🔹 테스트 예제
comments = [
    "저도 작년 여름에 스케줄 땜에 한달정도 투싼 nx4를 탔었는데 17인치 휠이 들어간 깡통 모델이었는데도 요철에서의 승차감이 많이 하드했던 기억이 나네요. 도요타 전시장 가서 라브4 시승해보고 이렇게 차이가 많이 났나 싶었습니다.",
    "아하~ 실내는 쏘렌토와 똑같구나.",
    "dct때매 아쉬운 차ㅠㅜ 디자인 너무 이쁜데..",
    "후미 방향지시등 밑에 처박혀있지 안으면 스포티지 따라잡을텐데 판매량",
    "투싼에 사이드스텝 달려있는거 첨보는거같네",
    "썬팅은 뭘로되잇나요? 반사같은데",
    "천장색은 그레인가요? 블랙인가요?",
    "한국 현대공장은 유투브 보면서 하던데 탱자탱자",
    "해외는 사이드미러 안접습니다.. 땅떵이 넓어서요ㅋㅋ 열선은 그나라 기후마다ㅋㅋㅋ아 무식해",
    "투싼ix는 한국에서도 고장이 잘 안나는차로 유명하지",
    "내수용차가 저랬으면 벌써 뒤집어졌다",
    "리뷰 잘 보고 있습니다.~"
]

# 결과를 저장할 리스트
results = []

for comment in comments:
    sentiment, topic = analyze_comment(comment)
    results.append({"comment": comment, "sentiment": sentiment, "topic": topic})

# 🔹 결과 리스트 출력
print(results)
