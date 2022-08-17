from airflow.decorators import task

@task
def train_arima(daily_df_loc, p, d, q, trend, **kwargs):
    import pandas as pd
    import mlflow
    import mlflow.statsmodels
    import numpy as np
    from numpy.linalg import LinAlgError
    from statsmodels.tsa.arima.model import ARIMA

    mlflow.set_experiment(f"ARIMA({p},{d+1},{q})_{trend}")

    cur_time = kwargs["data_interval_end"]
    etz_time = cur_time.subtract(hours=5)

    df = pd.read_csv(daily_df_loc, parse_dates=['etz_date'])
    df = df.set_index('etz_date')
    daily_btc_series = df.log_diff_trade_price.dropna()

    if not daily_btc_series.index.is_monotonic_increasing:
        daily_btc_series = daily_btc_series.sort_index()
    
    mlflow.statsmodels.autolog()
    convergence_error, stationarity_error = 0, 0
    with mlflow.start_run():
        mlflow.log_params(dict(p=p,d=d,q=q,trend=trend,etz_date=str(etz_time)))
        try:
            model = ARIMA(endog=daily_btc_series, order=(p, d, q), trend=trend).fit()
            y_pred = model.forecast(steps=1).values[0]
            aic = model.aic
            bic = model.bic
            results = dict(y_pred=y_pred,aic=aic,bic=bic,
                        convergence_error=convergence_error,stationarity_error=stationarity_error)
            results.update(model.pvalues)
            mlflow.log_metrics(results)
        except LinAlgError:
            results = dict(convergence_error=1,stationarity_error=0)
            mlflow.log_metrics(results)
        except ValueError:
            results = dict(convergence_error=0,stationarity_error=1)
            mlflow.log_metrics(results)
    