FROM python:3.11

ENV AIRFLOW_HOME=/opt/airflow

ENV AIRFLOW_VERSION=2.10.4
ENV PYTHON_VERSION=3.11

ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

RUN pip install 'apache-airflow[amazon]'

EXPOSE 8080

CMD ["airflow", "standalone"]