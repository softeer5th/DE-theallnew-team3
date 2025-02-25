# The All New Team3

## 이번 페이스리프트는 성공적이었을까?

> 신규 출시 차량/페이스리프트 전/후 소비자 반응을 비교하고 분석하기 위한 데이터 파이프라인

### 데모

![Image](https://github.com/user-attachments/assets/1d745d96-ddc8-42d5-bb8f-c42657f07312)

### 유튜브 시연 영상

https://youtu.be/Fyyahbjk9A8

## 시스템 아키텍처

![Image](https://github.com/user-attachments/assets/4e75aec1-37c3-4e8a-8ca8-be085f0cc95d)

#### 주요 구성 요소

- AWS Lambda → 웹사이트 크롤링 및 데이터 수집
- AWS EMR → 텍스트 전처리 및 데이터 변환
- AWS Lambda + ChatGPT → 자연어 처리 (감성 분석 및 키워드 추출)
- Amazon S3 → Parquet 형식으로 데이터 저장
- Amazon Redshift → 분석을 위한 데이터 적재
- Tableau → 데이터 시각화

## 데이터 스키마

- s3

![Image](https://github.com/user-attachments/assets/2be1a9ff-7cfa-41ff-89b4-5bc29f571b43)

추출한 데이터와 transform한 데이터를 parquet형태로 s3에 저장합니다.

- Redshift

![Image](https://github.com/user-attachments/assets/931cb15d-410d-4d7a-9a74-52df3d123f38)

clear_mart.sql, init_mart.sql, clear_staging.sql, init_staging.sql로 Redshift 스키마를 설정합니다.

Airflow에서 S3ToRedshiftOperator를 활용해 S3에 저장된 parquet 데이터를 redshift의 staging table로 copy합니다.

조회수, 댓글수, 좋아요 수 등 데이터를 여러번 수집할 때마다 바뀌는 값은 append_load_to_mart.sql을 이용해 중복해서 저장합니다.

게시글의 제목, 본문내용, 댓글내용 등 데이터를 여러 번 수집해도 바뀌지 않는 값은 upsert_load_to_mart.sql로 저장해 unique한 값을 가지게 됩니다.

## Project Details

### [Airflow](./airflow/README.md)

Airflow(AWS MWAA)를 통해 파이프라인을 구축합니다.

### [EMR](./emr/README.md)

EMR에서 실행할 스파크 코드

### [Lambda](./lambda_functions/README.md)

AWS 람다 함수

### local

프로토타입을 위한 로컬 크롤링 및 전처리 테스트 코드를 포함합니다.
