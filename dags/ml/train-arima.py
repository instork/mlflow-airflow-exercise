
import datetime as dt

from airflow.decorators import dag

import pendulum

########################### Set Configs ########################
# SCHEDULE_INTERVAL = "0 0 * * *" 
SCHEDULE_INTERVAL = "@once"
ETZ = pendulum.timezone("US/Eastern")
################################################################

@dag(
    dag_id="ml-train_arima",
    start_date=dt.datetime(2020, 1, 1, 0, 0, tzinfo=ETZ),
    schedule_interval=SCHEDULE_INTERVAL,
)
def train_arima():
    from ml.mongodb.mongo2csv import get_data_save_csv, print_csv_head

    print_csv_head(get_data_save_csv("test_db","USDT-BTC",{}))

train_arima_dag = train_arima()