
import datetime as dt
from airflow.decorators import dag
import pendulum

########################### Set Configs ########################
# SCHEDULE_INTERVAL = "0 0 * * *" 
SCHEDULE_INTERVAL = "@daily"
ETZ = pendulum.timezone("US/Eastern")
################################################################

@dag(
    dag_id="ml-train_arima",
    start_date=dt.datetime(2021, 12, 31, 0, 0, tzinfo=ETZ),
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
)
def train_arima(db_name, coin_name, day_before):
    from ml.mongodb.mongo2csv import get_data_save_csv
    from ml.preprocess.preprocess import make_daily_csv, make_log_normal
    from ml.validation.validation import check_server_maintenance_time, check_stats
    
    df_loc = get_data_save_csv(db_name, coin_name, day_before)
    df_loc = check_server_maintenance_time(df_loc, "ARIMA_missing_prop")
    daily_df_loc = make_daily_csv(df_loc)
    daily_df_loc = make_log_normal(daily_df_loc)
    daily_df_loc = check_stats(daily_df_loc, "ARIMA_stats")


train_arima_dag = train_arima("test_db","USDT-BTC", 120)