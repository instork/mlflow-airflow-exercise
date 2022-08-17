
import datetime as dt
from airflow.decorators import dag
import pendulum

########################### Set Configs ########################
# SCHEDULE_INTERVAL = "0 0 * * *" 
SCHEDULE_INTERVAL = "@daily"
ETZ = pendulum.timezone("US/Eastern")
configs = {"db_name": "test_db",
            "coin_name": "USDT-BTC",
            "day_before" :121, 
            "p": 1, 
            "d": 0,
            "q": 1,
            "trend":'c'}
################################################################

@dag(
    dag_id="ml-train_arima",
    start_date=dt.datetime(2021, 12, 31, 0, 0, tzinfo=ETZ),
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
)
def arima_pipeline(db_name, coin_name, day_before, p, d, q, trend, **kwargs):
    from ml.mongodb.mongo2csv import get_data_save_csv
    from ml.preprocess.preprocess import make_daily_csv, make_log_normal
    from ml.validation.validation import check_server_maintenance_time, check_stats
    from ml.models.arima import train_arima

    df_loc = get_data_save_csv(db_name, coin_name, day_before)
    df_loc = check_server_maintenance_time(df_loc, "ARIMA_missing_prop")
    daily_df_loc = make_daily_csv(df_loc)
    daily_df_loc = make_log_normal(daily_df_loc)
    daily_df_loc = check_stats(daily_df_loc, "ARIMA_stats")
    train_arima(daily_df_loc, p, d, q, trend)
    
train_arima_dag = arima_pipeline(db_name="test_db",
                                coin_name="USDT-BTC",
                                day_before=121, 
                                p=1, d=0, q=1,trend="c")
