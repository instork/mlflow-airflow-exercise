from airflow.decorators import task

@task()
def train_auto_arima(daily_df_loc, **kwargs):
    import logging
    import pandas as pd
    import mlflow
    import os
    import sys
    from numpy.linalg import LinAlgError
    from pmdarima.arima import auto_arima

    exp_name = "auto_ARIMA"
    mlflow.set_experiment(exp_name)

    cur_time = kwargs["data_interval_end"]
    etz_time = cur_time.subtract(hours=5)

    df = pd.read_csv(daily_df_loc, parse_dates=['etz_date'])
    df = df.set_index('etz_date')
    daily_btc_series = df.log_diff_trade_price.dropna()

    if not daily_btc_series.index.is_monotonic_increasing:
        daily_btc_series = daily_btc_series.sort_index()
    
    # mlflow.statsmodels.autolog()
    
    logged_result = ""
    with mlflow.start_run() as run:
        # https://github.com/alkaline-ml/pmdarima/issues/64
        orig_stdout = sys.stdout
        f = open('trace.txt', 'w')
        sys.stdout = f
        model = auto_arima(y=daily_btc_series, start_p=1, start_q=1, start_P=1, start_Q=1,
                     max_p=5, max_q=5, max_P=5, max_Q=5, stepwise=True, seasonal=True,
                   trace=True)
        sys.stdout = orig_stdout
        f.close()
        mlflow.log_artifact("trace.txt")
        os.remove("trace.txt")

        model_summary = model.summary().as_text()
        with open("model_summary_auto.txt", 'w') as f:
            f.write(model_summary)
        mlflow.log_artifact("model_summary_auto.txt")
        os.remove("model_summary_auto.txt")

        model_results = model.to_dict()
        p, d, q = model_results['order']
        mlflow.log_params(dict(p=p, d=d, q=q, trained_etz_date=str(etz_time)))
        
        y_pred = model.predict(n_periods=1)[0]
        aic = model_results['aic']
        bic = model_results['bic']
        mlflow.log_metrics(dict(y_pred=y_pred, aic=aic, bic=bic))

        mlflow.pmdarima.log_model(model, artifact_path=exp_name, 
                                  registered_model_name=exp_name)
    return exp_name
