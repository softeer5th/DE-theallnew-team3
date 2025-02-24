# DE-team3

```
project
├─ README.md
├─ airflow
│  ├─ AIRFLOW_DEV_GUIDE.md
│  ├─ Dockerfile
│  └─ docker-compose.yml
└─ src
   ├─ data_ingestion
   │  └─ __init__.py
   ├─ data_loading
   │  └─ __init__.py
   └─ data_transform
      └─ __init__.py

```

## 로컬 ETL 테스트

```
python src/etl.py '2025-01' '투싼'
```

# 주제: 페이스 리프트 및 새로 출시한 차량에 대한 소비자의 반응을 모니터링하는 Data Product

![image.png](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20README%20md%2019e2e4f81c5480978fede95a3b9c6df8/image.png)

- 소비자의 반응을 매일 수집하여 업데이트 해줍니다. 
마케팅 담당자가 마케팅 결과에 대한 즉각적인 반응을 확인할 수 있습니다.
차량 기획자가 차량에 대한 즉각적인 반응을 확인할 수 있습니다.
- 사람들의 반응을 카테고리볼 수 있어 해당 기능을 개발하는 연구원, 디자이너에게 인사이트를 제공할 수 있습니다.

### 유튜브 시연 영상

https://youtube….

### 팀원소개

이름+깃 아이디만

### 폴더 개요 - 링크달기

- src/
로컬에서 크롤링 및 transform 테스트하는 코드
- lambda_functions/
aws 람다 함수에서 실행하는 코드들 (데이터 crawling 및 문장 감성 분석)
- emr/
emr에서 실행할 스파크 코드
- airflow/
airflow 이용해 task 관리하는 폴더

# 프로젝트 소개

## 1. 프로젝트 배경 및 목표

현대자동차는 새로운 변화를 주기 위해 주기적으로 세대를 바꾸기도 하고, 페이스리프트를 진행하며 소비자들의 욕구를 보다 더 충족시키기 위해 노력합니다. 

신차 개발 이후, 세대 변경 이후, 페이스리프트 이후 등 새로운 차량이 출시되었을 때, 소비자들의 의견을 지속적으로 수집/모니터링하는 파이프라인을 구현하는 것이 이번 프로젝트의 목표입니다. 

→ 소비자들이 작성한 게시글 및 댓글들을 수집한 데이터에서 키워드를 추출하여 보여주고자 합니다. 

## 2. 시스템 아키텍처
![Image](https://github.com/user-attachments/assets/4e75aec1-37c3-4e8a-8ca8-be085f0cc95d)

![image.png](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20README%20md%2019e2e4f81c5480978fede95a3b9c6df8/image%201.png)

- aws lambda로 웹사이트를 크롤링합니다.(최초 데이터는 .json으로 저장)
- EMR에서 텍스트 전처리를 수행합니다.(post, comment 데이터 분리 및 sentence 데이터 분리)
- 이후 lambda에서 openai api를 이용해 자연어 처리를 수행합니다.
- extract 및 transform 과정에서는 parquet 형식으로 s3 스토리지에 저장합니다.
- s3에 저장된 parquet 데이터를 redshift로 copy?합니다.
- Tableau를 활용하여 redshift에 저장된 정보를 시각화합니다.

## 3. 데이터베이스 구조

- s3

![image.png](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20README%20md%2019e2e4f81c5480978fede95a3b9c6df8/image%202.png)

추출한 데이터와 transform한 데이터를 parquet형태로 s3에 저장합니다.

- Redshift

![image.png](%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3%20README%20md%2019e2e4f81c5480978fede95a3b9c6df8/image%203.png)

clear_mart.sql, init_mart.sql, clear_staging.sql, init_staging.sql로 Redshift 스키마를 설정합니다. 

Airflow에서 S3ToRedshiftOperator를 활용해 S3에 저장된 parquet 데이터를 redshift의 staging table로 copy합니다.

조회수, 댓글수, 좋아요 수 등 데이터를 여러번 수집할 때마다 바뀌는 값은 append_load_to_mart.sql을 이용해 중복해서 저장합니다.

게시글의 제목, 본문내용, 댓글내용 등 데이터를 여러 번 수집해도 바뀌지 않는 값은 upsert_load_to_mart.sql로 저장해 unique한 값을 가지게 됩니다. 
