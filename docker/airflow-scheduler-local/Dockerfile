FROM apache/airflow:2.3.3-python3.9

COPY mongo.env /tmp/mongo.env
COPY fred.env /tmp/fred.env

COPY requirements.txt /tmp/requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt
