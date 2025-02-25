# S3 to Redshift ELT Pipeline

## 개요
분산 클러스터(EMR)에서 처리되어 AWS S3에 저장된 데이터를 Amazon Redshift로 이동시키는 ELT(Extract, Load, Transform) 프로세스를 구현합니다. 파이프라인의 신뢰성과 개발 생산성을 높이기 위해 미리 구축된 Apache Airflow에 각 단계별 SQL 스크립트를 DAG에 통합하였습니다.

## 사용 기술 및 도구
- **AWS S3**: 원본 데이터 저장소
- **Amazon Redshift**: 데이터 웨어하우스 및 최종 데이터 mart
- **Apache Airflow**: 파이프라인 오케스트레이션
  - **RedshiftDataOperator**: SQL 스크립트를 실행하여 테이블 생성, 데이터 로드, 변환 및 정리 작업 수행
  - **S3ToRedshiftOperator**: S3에서 Redshift로 데이터를 COPY 하는 역할 수행

## ELT 프로세스 상세 설명
1. **Staging 테이블 생성**
   - **Operator**: `RedshiftDataOperator`
   - **설명**: 데이터 적재 전 임시 저장소 역할을 하는 staging 테이블을 생성합니다. 분산 클러스터와 데이터 스토리지 사이의 의존성을 줄이는 역할을 합니다.

2. **S3에서 Staging 테이블로 데이터 COPY**
   - **Operator**: `S3ToRedshiftOperator`
   - **설명**: 대용량 데이터를 효율적으로 처리할 수 있도록 분산 클러스터에서 처리된 데이터를 AWS S3에서 staging 테이블로 COPY 합니다.

3. **Mart 테이블에 Append 적재**
   - **Operator**: `RedshiftDataOperator`
   - **설명**: 데이터의 변경 내역 및 이력을 보존하기 위해 (ID, Timestamp)를 기준으로 중복을 허용하는 데이터를 마트 테이블에 추가합니다.

4. **Mart 테이블에 Upsert 적재**
   - **Operator**: `RedshiftDataOperator`
   - **설명**: 저장의 효율성을 위해 거의 변경되지 않는 데이터를 (ID)를 기준으로 중복을 허용하지 않는 데이터를 마트 테이블에 업서트(Insert 또는 Update) 방식으로 적재합니다.

5. **Materialized View REFRESH**
   - **Operator**: `RedshiftDataOperator`
   - **설명**: mart 테이블과 연결된 materialized view를 새로 고침(REFRESH)하여 사용자가 항상 최신 데이터를 즉시 조회할 수 있도록 지원합니다.

6. **Staging 테이블 삭제**
   - **Operator**: `RedshiftDataOperator`
   - **설명**: 데이터 적재가 완료된 후, 임시로 생성된 staging 테이블을 삭제합니다.

## 프로세스 아키텍처
- **단계별 데이터 이동**: S3 → Staging 테이블 → Mart 테이블 → Materialized View
- **데이터 품질 및 최신성**: 데이터의 변경 주기 특성을 고려해 append 또는 overwrite 적재 전략을 혼용하였습니다.
- **즉시 서빙 가능한 데이터**: Materialized View를 REFRESH 하여 사용자가 지연 없이 데이터를 조회할 수 있습니다.

## 추가 고려 사항
- **에러 핸들링**: 각 단계에서 failover 및 retry 정책을 구현하여 파이프라인의 안정성을 보장합니다.
- **성능 최적화**: COPY 명령어와 SQL 스크립트의 최적화를 통해 데이터 로딩 및 처리 속도를 개선합니다.
- **보안**: 민감한 연결 정보와 자격증명 관리를 철저히 하여 데이터 보안을 강화합니다.