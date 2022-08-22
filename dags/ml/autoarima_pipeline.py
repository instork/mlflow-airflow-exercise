import pendulum
from airflow.decorators import dag
from pendulum.datetime import DateTime

# ======================= Set Configs =======================
SCHEDULE_INTERVAL = "0 5 * * *"
UCT = pendulum.timezone("UCT")
start_date = pendulum.datetime(2021, 12, 31, 5, 0, tz=UCT)
# ==========================================================


@dag(
    dag_id="ml-auto_arima_pipeline",
    start_date=start_date,
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
)
def auto_arima_pipeline(
    db_name: str, coin_name: str, train_size: int, start_date: DateTime, **kwargs
):
    """Pipeline for auto ARIMA."""
    from ml.models.arima import delete_registered, test_arima
    from ml.models.auto_arima import train_auto_arima
    from ml.mongodb.mongo import get_data_save_csv, get_test_data
    from ml.preprocess.preprocess import save_daily_df, save_log_diff
    from ml.validation.validation import (
        check_missing,
        check_server_maintenance_time,
        check_stats,
    )

    df_loc = get_data_save_csv(db_name, coin_name, train_size)
    df_loc = check_missing(df_loc, "utc_time", "1H")
    df_loc = check_server_maintenance_time(df_loc, "auto_ARIMA_missing_prop")
    daily_df_loc = save_daily_df(df_loc)
    daily_df_loc = save_log_diff(daily_df_loc)
    daily_df_loc = check_stats(daily_df_loc, "auto_ARIMA_stats")
    exp_name = train_auto_arima(daily_df_loc)
    y_true = get_test_data(db_name, coin_name, start_date)

    old_version = test_arima(exp_name, y_true, start_date)
    delete_registered(exp_name, old_version, start_date)


train_arima_dag = auto_arima_pipeline(
    db_name="test_db", coin_name="USDT-BTC", train_size=91, start_date=start_date
)
