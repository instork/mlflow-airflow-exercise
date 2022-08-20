from airflow.decorators import task


@task()
def train_arima(daily_df_loc, p, d, q, trend, **kwargs):
    import logging
    import os

    import pandas as pd
    from numpy.linalg import LinAlgError
    from statsmodels.tsa.arima.model import ARIMA

    import mlflow

    exp_name = f"ARIMA({p},{d+1},{q})_{trend}"
    mlflow.set_experiment(exp_name)

    cur_time = kwargs["data_interval_end"]
    etz_time = cur_time.subtract(hours=5)

    df = pd.read_csv(daily_df_loc, parse_dates=["etz_date"])
    df = df.set_index("etz_date")
    daily_btc_series = df.log_diff_trade_price.dropna()

    if not daily_btc_series.index.is_monotonic_increasing:
        daily_btc_series = daily_btc_series.sort_index()

    # mlflow.statsmodels.autolog()
    convergence_error, stationarity_error = 0, 0
    logged_result = ""
    with mlflow.start_run() as run:
        mlflow.log_params(
            dict(p=p, d=d, q=q, trend=trend, trained_etz_date=str(etz_time))
        )
        try:
            model = ARIMA(endog=daily_btc_series, order=(p, d, q), trend=trend).fit()
            model_summary = model.summary().as_text()
            with open(f"model_summary_{exp_name}.txt", "w") as f:
                f.write(model_summary)

            y_pred = model.forecast(steps=1).values[0]
            aic = model.aic
            bic = model.bic
            results = dict(
                y_pred=y_pred,
                aic=aic,
                bic=bic,
                convergence_error=convergence_error,
                stationarity_error=stationarity_error,
            )
            results.update(model.pvalues)
            mlflow.log_metrics(results)
            logged_result = mlflow.statsmodels.log_model(
                model, artifact_path=exp_name, registered_model_name=exp_name
            )
            mlflow.log_artifact(f"model_summary_{exp_name}.txt")
            os.remove(f"model_summary_{exp_name}.txt")
            logger = logging.getLogger(__name__)
            logger.info(logged_result)
            return exp_name
        except LinAlgError:
            results = dict(convergence_error=1, stationarity_error=0)
            mlflow.log_metrics(results)
            return ""
        except ValueError:
            results = dict(convergence_error=0, stationarity_error=1)
            mlflow.log_metrics(results)
            return ""


@task()
def test_arima(exp_name, y_true, start_date, **kwargs):
    import logging

    from mlflow import MlflowClient, log_metric, log_param, mlflow

    logger = logging.getLogger(__name__)

    cur_time = kwargs["data_interval_end"]
    start_date = start_date.add(
        days=1
    )  # date_interval_end is actual logical start time

    if str(cur_time) == str(start_date):
        return ""

    client = MlflowClient()
    model_info = client.get_latest_versions(exp_name)[0]
    latest_version = int(model_info.version)
    old_version = latest_version - 1

    run_id = client.get_model_version(exp_name, f"{old_version}").run_id
    data = client.get_run(run_id).data.to_dictionary()
    y_pred = data["metrics"]["y_pred"]

    with mlflow.start_run(run_id) as run:
        rmse = ((y_true - y_pred) ** (2)) ** (1 / 2)
        log_metric("y_true", y_true)
        log_metric("rmse", rmse)
        etz_time = cur_time.subtract(hours=5)
        log_param("tested_etz_date", str(etz_time))

    return old_version


@task()
def delete_registered(exp_name, version, start_date, **kwargs):
    from mlflow import MlflowClient

    cur_time = kwargs["data_interval_end"]
    start_date = start_date.add(days=1)
    if str(cur_time) == str(start_date):
        return

    client = MlflowClient()
    client.delete_model_version(exp_name, f"{version}")
