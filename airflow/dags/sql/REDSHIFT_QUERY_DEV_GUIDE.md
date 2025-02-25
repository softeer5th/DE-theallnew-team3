## 사용 방법
1. **Airflow 설정**
   - ELT 프로세스에 통합할 쿼리를 구현합니다.
   - Apachae Airflow RedshiftDataOperator를 이용해 쿼리 호출 Task를 정의합니다.
   - 파이프라인 DAG에 Task를 통합합니다.
   - {$repository_root}/airflow/dags/deploy.sh를 호출해 DAG를 배포합니다.
   - MWAA UI에 접속해 배포한 DAG를 호출하고 모니터링합니다.

2. **DAG 실행**
   - MWAA UI에 접속해 배포한 DAG를 수동으로 호출하거나, 스케줄링된 작업에 포함되도록 합니다.

3. **모니터링 및 로깅**
   - MWAA DAG Log 또는 CloudWatch의 Redshift Metric, 또는 Redshift의 시스템 로그 테이블을 확인합니다.
   - [Monitoring queries and workloads with Amazon Redshift Serverless](https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-monitoring.html)