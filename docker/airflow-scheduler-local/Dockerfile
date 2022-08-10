ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.3.3-python3.9"
FROM ${AIRFLOW_BASE_IMAGE}


COPY mongo.env /tmp/mongo.env
COPY fred.env /tmp/fred.env

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

USER airflow
