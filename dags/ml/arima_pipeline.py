import datetime as dt

import pendulum
from airflow.decorators import dag

########################### Set Configs ########################
# SCHEDULE_INTERVAL = "0 0 * * *"
SCHEDULE_INTERVAL = "0 5 * * *"
UCT = pendulum.timezone("UCT")
start_date = pendulum.datetime(2021, 12, 31, 5, 0, tz=UCT)
################################################################


@dag(
    dag_id="ml-arima_pipeline",
    start_date=start_date,
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
)
def arima_pipeline(
    db_name, coin_name, train_size, p, d, q, trend, start_date, **kwargs
):
    from ml.models.arima import delete_registered, test_arima, train_arima
    from ml.models.auto_arima import train_auto_arima
    from ml.mongodb.mongo import get_data_save_csv, get_test_data
    from ml.preprocess.preprocess import save_daily_df, save_log_diff
    from ml.validation.validation import (check_server_maintenance_time,
                                          check_stats)

    df_loc = get_data_save_csv(db_name, coin_name, train_size)
    df_loc = check_server_maintenance_time(df_loc, "ARIMA_missing_prop")
    daily_df_loc = save_daily_df(df_loc)
    daily_df_loc = save_log_diff(daily_df_loc)
    daily_df_loc = check_stats(daily_df_loc, "ARIMA_stats")
    # ARIMA train today's data, test yeseterday version
    arima_exp_name = train_arima(daily_df_loc, p, d, q, trend)
    arima_y_true = get_test_data(db_name, coin_name, start_date, arima_exp_name)
    arima_old_version = test_arima(arima_exp_name, arima_y_true, start_date)
    delete_registered(arima_exp_name, arima_old_version, start_date)

    # auto-ARIMA train today's data, test yeseterday version
    auto_exp_name = train_auto_arima(daily_df_loc)
    auto_y_true = get_test_data(db_name, coin_name, start_date, auto_exp_name)
    auto_old_version = test_arima(auto_exp_name, auto_y_true, start_date)
    delete_registered(auto_exp_name, auto_old_version, start_date)


train_arima_dag = arima_pipeline(
    db_name="test_db",
    coin_name="USDT-BTC",
    train_size=121,
    p=1,
    d=0,
    q=1,
    trend="c",
    start_date=start_date,
)
