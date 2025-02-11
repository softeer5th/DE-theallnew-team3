
# Airflow 개발환경 가이드

## Airflow 실행
```
docker compose up
```
컨테이너 띄운 후 `http://localhost:8080`로 접속해주세요.

## Airflow 로그인

루트 계정 아이디는 `admin`, 비밀번호는 `/opt/airflow/standlone_admin_password.txt` 파일을 확인해주세요. airflow 계정을 추가하고 싶다면 docker container 내부에서 아래 명령어를 실행해주세요.

```
airflow users create -u $USER -p $PASSWORD -f $FIRST_NAME -l $LAST_NAME -r User -e $EMAIL
```

## DAG 추가

dags 폴더 내부에 원하는 DAG 파일을 추가해주세요.