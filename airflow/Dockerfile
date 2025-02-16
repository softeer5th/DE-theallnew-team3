FROM python:3.11

ENV AIRFLOW_HOME=/opt/airflow

ENV AIRFLOW_VERSION=2.10.4
ENV PYTHON_VERSION=3.11

ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

RUN pip install 'apache-airflow[amazon]'
RUN pip install 'apache-airflow-providers-slack'

COPY ./.env .env

RUN airflow db init

# AWS Connection
RUN airflow connections delete aws_default
RUN export $(grep -v '^#' .env | xargs) && \
    airflow connections add aws_default \
    --conn-type aws \
    --conn-login $AWS_ACCESS_KEY_ID \
    --conn-password $AWS_SECRET_ACCESS_KEY \
    --conn-extra '{"region_name": "ap-northeast-2"}'

# AWS Redshift Connection
RUN airflow connections delete redshift_default
RUN export $(grep -v '^#' .env | xargs) && \
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

# Airflow user
RUN export $(grep -v '^#' .env | xargs) && \
    airflow users create -u $USER -p $PASSWORD -f $FIRST_NAME -l $LAST_NAME -r Admin -e $EMAIL

# Slack
RUN export $(grep -v '^#' .env | xargs) && \
    airflow connections add slack_default \
    --conn-type slackwebhook \
    --conn-password $SLACK_WEBHOOK_URL

EXPOSE 8080

CMD ["airflow", "standalone"]
