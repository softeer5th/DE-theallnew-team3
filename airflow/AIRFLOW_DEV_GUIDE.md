# Airflow 개발 가이드

## Airflow 실행

``` bash
docker compose up
```

컨테이너 띄운 후 `http://localhost:8080`로 접속해주세요.

## Airflow 로그인

루트 계정 아이디는 `admin`, 비밀번호는 `/opt/airflow/standlone_admin_password.txt` 파일을 확인해주세요. airflow 계정을 추가하고 싶다면 docker container 내부에서 아래 명령어를 실행해주세요.

``` bash
airflow users create -u $USER -p $PASSWORD -f $FIRST_NAME -l $LAST_NAME -r User -e $EMAIL
```

## AWS Connection 추가

Airflow에서 AWS 관련 operator를 사용하기 위해서는 `aws_default`라는 AWS Connection을 추가해야 합니다.

``` bash
airflow connections add aws_default \
    --conn-type aws \
    --conn-login $AWS_ACCESS_KEY_ID \
    --conn-password $AWS_SECRET_ACCESS_KEY \
    --conn-extra '{"region_name": "ap-northeast-2"}'
```

등록된 Connection을 확인하고 싶다면 아래 명령어를 실행해주세요.

```
airflow connections get aws_default
```

## Redshift Connection 추가

Airflow에서 AWS Redshift 관련 operator를 사용하기 위해서는 `redshift_default`라는 Connection을 추가해야 합니다.
- 아래 명령어는 Connection 자격 증명을 Database Credentials를 이용하는 방식입니다.
- 보안 상 운영 환경에서는 aws Profile을 이용해 임시 IAM Role을 생성하고 이용하도록 수정하는 것이 좋습니다.
  - Prerequisite: Airflow 컨테이너에 AWS CLI 설치 또는 MWAA를 사용
- DAG에서 Redshift Operator를 호출하기 위해서는 Redshift가 속한 VPC에 Public IP GW를 개방하고, SG의 5439(default) 포트 인바운드 규칙을 열어야 합니다.
``` bash
airflow connections add redshift_default \
    --conn-type 'redshift' \
    --conn-host $REDSHIFT_HOST \
    --conn-port $REDSHIFT_PORT \
    --conn-schema $REDSHIFT_DATABASE \
    --conn-login $REDSHIFT_USER \
    --conn-password $REDSHIFT_PASSWORD \
    --conn-extra '{ \
                    "is_serverless": true, \
                    "serverless_work_group": $REDSHIFT_SERVERLESS_WG, \
                    "role_arn": $REDSHIFT_IAM_ROLE \
                    }'
```

등록된 Connection을 확인하고 싶다면 아래 명령어를 실행해주세요.

``` bash
airflow connections get redshift_default
```

## Slack Connection 추가

Dag 모니터링을 위해 Slack Webhook을 등록합니다. Webhook URL은 @openkmj에게 문의!

``` bash
airflow connections add slack_default \
    --conn-type slackwebhook \
    --conn-password $SLACK_WEBHOOK_URL
```

## DAG 추가

dags 폴더 내부에 원하는 DAG 파일을 추가해주세요.

## MWAA 배포

MWAA 배포는 dags 폴더를 S3에 업로드하는 방식으로 진행됩니다.

``` bash
cd airflow
./deploy.sh
```


