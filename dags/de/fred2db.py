import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from de.fred.request import fetch_fred
from de.mongodb.data2mongo import insert_single
from de.utils.timeutils import ETZ

########################### Set Configs ###########################
SCHEDULE_INTERVAL = "0 0 * * *"  # At 00:00
## mongodb
fred_mongo_templates_dict = {
    "db_name": "test_db",
    "collection_name": "fred",
    "start_time": "{{ data_interval_end }}",
}
## FRED
fred_series_tickers = ["T5YIE", "T5YIFR", "T10YIE", "T10Y2Y", "SP500", "DJIA"]
fred_templates_dict = {
    "fred_series_tickers": fred_series_tickers,
    "start_time": "{{ data_interval_end }}",
}
################################################################

dag = DAG(
    dag_id="de-fred2db",
    description="Get fred data and news and load on MongoDB",
    start_date=dt.datetime(2020, 1, 1, 0, 0, tzinfo=ETZ),
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
    default_args={
        "depends_on_past": True,
        "retries": 3,
        "retry_delay": dt.timedelta(minutes=2),
    },
)

fetch_fred_task = PythonOperator(
    task_id="fetch_fred_task",
    python_callable=fetch_fred,
    templates_dict=fred_templates_dict,
    dag=dag,
)

insert_fred_task = PythonOperator(
    task_id="insert_fred_to_mongodb",
    python_callable=insert_single,
    templates_dict=fred_mongo_templates_dict,
    dag=dag,
)

fetch_fred_task >> insert_fred_task
