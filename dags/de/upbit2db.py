import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from de.mongodb.data2mongo import insert_ohlcvs
from de.upbit.request import fetch_minute_ohlcvs
from de.utils.timeutils import UTC

########################### Set Configs ###########################
SCHEDULE_INTERVAL = "0 * * * *"  # every hour
## mongodb
mongo_templates_dict = {"db_name": "test_db", "start_time": "{{ data_interval_end }}"}
## upbit
MINUTE_INTERVAL = 60
GET_CNT = 1
fetch_base_template_dict = {
    "minute_interval": MINUTE_INTERVAL,
    "get_cnt": GET_CNT,
    "start_time": "{{ data_interval_end }}",
}
tickers = ["USDT-BTC", "USDT-ETH"]
fetch_template_dicts = {}
for ticker in tickers:
    fetch_base_template_dict.update(coin_ticker=ticker)
    fetch_template_dicts[ticker] = fetch_base_template_dict.copy()
################################################################

dag = DAG(
    dag_id="de-upbit2db",
    description="Get ohlcv data using upbit API",
    start_date=dt.datetime(2022, 5, 31, 9, 0, tzinfo=UTC),
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=4,
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=2),
    },
)

fetch_usdt_btc = PythonOperator(
    task_id="fetch_usdt_btc",
    python_callable=fetch_minute_ohlcvs,
    templates_dict=fetch_template_dicts["USDT-BTC"],
    dag=dag,
)

fetch_usdt_eth = PythonOperator(
    task_id="fetch_usdt_eth",
    python_callable=fetch_minute_ohlcvs,
    templates_dict=fetch_template_dicts["USDT-ETH"],
    dag=dag,
)

insert_btc_ohlcvs_task = PythonOperator(
    task_id="insert_ohlcvs_btc",
    python_callable=insert_ohlcvs,
    dag=dag,
    templates_dict=mongo_templates_dict,
)

insert_eth_ohlcvs_task = PythonOperator(
    task_id="insert_ohlcvs_eth",
    python_callable=insert_ohlcvs,
    dag=dag,
    templates_dict=mongo_templates_dict,
)

fetch_usdt_btc >> insert_btc_ohlcvs_task
fetch_usdt_eth >> insert_eth_ohlcvs_task
