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

### Staging Tables
> 분산 클러스터 작업과 스토리지 작업의 의존성을 낮추기 위해 데이터를 임시 테이블에 저장하는 방식으로 ELT 프로세스를 처리합니다.
>
> Parquet 형태로 S3에 저장된 데이터와 1:1로 매칭되도록 테이블을 설계하였습니다.

![Image](https://github.com/user-attachments/assets/2be1a9ff-7cfa-41ff-89b4-5bc29f571b43)

### Mart Tables

![Image](https://github.com/user-attachments/assets/931cb15d-410d-4d7a-9a74-52df3d123f38)

> 분석 목적과 데이터의 변경 주기 특성에 따라 적재 전략을 결정하였습니다.
>
> 변경 이력과 같은 시계열 정보를 저장해야 할 팩트 테이블을 파란색으로 표시하였습니다. <br>
> 변경이 거의 발생하지 않는 정보를 저장해야 할 테이블 또는 디멘전 테이블을 보라색으로 표시하였습니다. <br>
>
> 적재 전략을 반영하기 위해 팩트 테이블은 ID와 Timestamp를 기준으로 Append Load 하였고, 디멘전 테이블을 ID를 기준으로 Overwrite Load 하였습니다.

### View Tables
> 분석 단계에서 항상 최신 데이터를 활용할 수 있도록 지원하면서 쿼리 효율을 높이기 위해 Materialzied View를 정의하고, 파이프라인에 REFRESH 단계를 통합하였습니다.

![Image](https://github.com/user-attachments/assets/b261d562-ac80-43f2-a68e-9babb6e406be)

### ELT 프로세스
> 구체적인 프로세스는 `/airflow/dags/sql/README.MD`에 소개되어 있습니다. 아래 **Project Details**를 참고해주세요.

## Project Details

### [Airflow](./airflow/README.md)
> Airflow(AWS MWAA)를 통해 파이프라인을 구축합니다.

### [EMR](./emr/README.md)
> EMR에서 실행할 스파크 코드

### [Lambda](./lambda_functions/README.md)

### [ELT Process](./airflow/dags/sql/README.md)
> S3에서 Redshift로의 ELT 프로세스 및 프로세스에 포함된 SQL 스크립트

AWS 람다 함수

### local

프로토타입을 위한 로컬 크롤링 및 전처리 테스트 코드를 포함합니다.
