
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator

from ml.mongodb.mongo2csv import get_data_save_csv, print_csv_head
import pendulum

########################### Set Configs ########################
# SCHEDULE_INTERVAL = "0 0 * * *" 
SCHEDULE_INTERVAL = "@once"
ETZ = pendulum.timezone("US/Eastern")
################################################################

dag = DAG(
    dag_id="ml-train_arima",
    start_date=dt.datetime(2020, 1, 1, 0, 0, tzinfo=ETZ),
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
)

save_btc_csv = PythonOperator(
    task_id = "save_btc_csv",
    python_callable=get_data_save_csv,
    templates_dict={
        "start_time" : "{{ data_interval_end }}",
        "db_name" : "test_db",
        "collection_name" : "USDT-BTC",
        "query" : {},
    },
    dag=dag,
)

print_btc = PythonOperator(
    task_id = "print_btc_head",
    python_callable=print_csv_head,
    templates_dict={
        "start_time" : "{{ data_interval_end }}",
        "db_name" : "test_db",
        "collection_name" : "USDT-BTC",
    },
    dag=dag,
)

save_btc_csv >> print_btc
