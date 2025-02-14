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

RUN airflow connections delete aws_default
RUN export $(grep -v '^#' .env | xargs) && \
    airflow connections add aws_default \
    --conn-type aws \
    --conn-login $AWS_ACCESS_KEY_ID \
    --conn-password $AWS_SECRET_ACCESS_KEY \
    --conn-extra '{"region_name": "ap-northeast-2"}'
RUN export $(grep -v '^#' .env | xargs) && \
    airflow users create -u $USER -p $PASSWORD -f $FIRST_NAME -l $LAST_NAME -r Admin -e $EMAIL

EXPOSE 8080

CMD ["airflow", "standalone"]
