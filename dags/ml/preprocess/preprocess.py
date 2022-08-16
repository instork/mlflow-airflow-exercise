from airflow.decorators import task

@task()
def make_daily_csv(file_loc):
    import os
    import pandas as pd
    import datetime as dt
    
    conditions = {
            "etz_time": "first",
            "opening_price": "first",
            "high_price": "max", 
            "low_price": "min",  
            "trade_price": "last",
            "candle_acc_trade_price": "sum",
            "candle_acc_trade_volume": "sum",
    }

    df = pd.read_csv(file_loc, parse_dates=["utc_time"])
    df['etz_time'] = df['utc_time'] + dt.timedelta(hours=-5)
    df['etz_date'] = df['etz_time'].apply(lambda x: x.date())
    
    daily_df = df.groupby('etz_date').agg(conditions)
    daily_df = daily_df.reset_index()

    folder, file_name = os.path.split(file_loc)
    new_folder = os.path.join(os.path.dirname(folder), '1D')
    os.makedirs(new_folder, exist_ok=True)
    daily_df_loc = os.path.join(new_folder, file_name)

    daily_df.to_csv(daily_df_loc, index=False)

    return daily_df_loc

@task()
def make_log_normal(file_loc):
    import pandas as pd
    import numpy as np

    df = pd.read_csv(file_loc, parse_dates=["etz_date"])

    df = df.sort_values("etz_date")
    df['log_diff_trade_price'] = np.log(df["trade_price"]).diff()
    
    df.to_csv(file_loc, index=False)

    return file_loc
