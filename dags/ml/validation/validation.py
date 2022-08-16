from asyncio.log import logger
import enum
from airflow.decorators import task

def check_missing(df, time_col, freq):
    """ check if data has missing rows"""
    import logging
    import pandas as pd

    logger = logging.getLogger(__name__)

    min_time = df[time_col].min()
    max_time = df[time_col].max()
    date_index = pd.date_range(start=min_time,
                               end=max_time, freq=freq)
    logger.info(f"expected time range: {date_index[0]} ~ {date_index[-1]}")
    logger.info(f"data time range: {min_time} ~ {max_time}")
    logger.info(len(date_index), len(df))
    assert len(date_index) == len(df)


@task()
def check_server_maintenance_time(file_loc, exp_name, **kwargs):
    import os
    import pandas as pd
    import datetime as dt
    import logging
    from mlflow import mlflow, log_metric, log_param
    
    logger = logging.getLogger(__name__)
    
    cur_time = kwargs["data_interval_end"]
    etz_time = cur_time.subtract(hours=5)

    mlflow.set_experiment(exp_name)
    
    df = pd.read_csv(file_loc, parse_dates=["utc_time", "candle_date_time_utc"])
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    missing_df = df[df.candle_date_time_utc != df.utc_time - dt.timedelta(hours=1)]
    prop = len(missing_df)/len(df)

    with mlflow.start_run() as run:
        log_param(f"date", str(etz_time))
        log_metric(f"{exp_name}", prop)

    return file_loc    

@task()
def check_stats(file_loc, exp_name, **kwargs):
    import os
    import pandas as pd
    from mlflow import mlflow, log_metrics, log_param
    import statsmodels.tsa.api as tsa
    from scipy.stats import describe
    
    cur_time = kwargs["data_interval_end"]
    etz_time = cur_time.subtract(hours=5)

    df = pd.read_csv(file_loc, parse_dates=["etz_date"])

    mlflow.set_experiment(exp_name)

    series = df.trade_price
    ld_series = df.log_diff_trade_price.iloc[1:]
    
    stats = describe(series)
    ld_stats = describe(ld_series)

    stats = [stats.mean, stats.variance, stats.skewness, stats.kurtosis]
    ld_stats = [ld_stats.mean, ld_stats.variance, ld_stats.skewness, ld_stats.kurtosis]

    acfs = tsa.acf(ld_series, nlags=14)[1:]
    pacfs = tsa.pacf(ld_series, nlags=14)[1:]

    kpss_stat, kpass_pval, kpss_lag, kpss_crit = tsa.kpss(ld_series)
    adf_stat, adf_pval, adf_usedlag, adf_obs, adf_cvals, adf_icbest = tsa.adfuller(ld_series)

    stat_names = ['mean', 'var', 'skew', 'kurt', 
                'ld_mean', 'ld_var', 'ld_skew', 'ld_kurt', 
                'kpss_stat', 'kpass_pval', 'adf_stat', 'adf_pval']

    acf_names = [f"acf_lag{i}" for i in range(1,15)]
    pacf_names = [f"pacf_lag{i}" for i in range(1,15)]


    stat_names = stat_names + acf_names + pacf_names
    stat_values = stats + ld_stats + [kpss_stat, kpass_pval, adf_stat, adf_pval] + list(acfs) + list(pacfs)

    metrics = dict(zip(stat_names, stat_values))

    with mlflow.start_run() as run:
        log_param(f"date", str(etz_time))
        log_metrics(metrics)

    return file_loc
