from airflow.decorators import task


def _get_mongo_client():
    """Get mongo client."""
    import os

    from pymongo import MongoClient

    user = os.getenv("MONGODB_USER")
    pwd = os.getenv("MONGODB_PWD")
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")
    client = MongoClient(f"mongodb://{user}:{pwd}@{host}:{port}")
    return client


@task()
def get_data_save_csv(db_name, coin_name, day_before, **kwargs):
    """Get Data from MongoDB and save as csv file."""
    import logging
    import os

    import pandas as pd
    from ml.validation.validation import check_missing

    logger = logging.getLogger(__name__)

    folder = "/data/csvs/train/1H"

    # pendulum datetime in UTC
    cur_time = kwargs["data_interval_end"]
    start_time = cur_time.subtract(days=day_before)

    client = _get_mongo_client()
    db = client[db_name]
    df = pd.DataFrame(
        list(db[coin_name].find({"utc_time": {"$gte": start_time, "$lt": cur_time}}))
    )
    client.close()

    os.makedirs(folder, exist_ok=True)
    file_loc = os.path.join(
        folder, f"{db_name}_{coin_name}_{cur_time}_{day_before}.csv"
    )
    df = df.loc[:, df.columns != "_id"]

    check_missing(df, "utc_time", "1H")
    df.to_csv(file_loc, index=False)

    return file_loc


@task()
def get_test_data(db_name, coin_name, dag_start_time, exp_name, **kwargs):
    import logging

    import numpy as np
    import pandas as pd
    from ml.preprocess.preprocess import make_daily_df

    logger = logging.getLogger(__name__)
    # UTC 현재시간
    cur_time = kwargs["data_interval_end"]
    dag_start_time = dag_start_time.add(days=1)
    if str(cur_time) == str(dag_start_time):
        return ""

    query_start_time = cur_time.subtract(days=2)
    client = _get_mongo_client()
    db = client[db_name]
    cur_time = kwargs["data_interval_end"]
    df = pd.DataFrame(
        list(
            db[coin_name].find(
                {"utc_time": {"$gte": query_start_time, "$lt": cur_time}}
            )
        )
    )

    logger.info(df.head())

    daily_df = make_daily_df(df)
    daily_df = daily_df.sort_values("etz_date")
    daily_df["log_diff_trade_price"] = np.log(daily_df["trade_price"]).diff()
    logger.info(daily_df["etz_date"])
    y_true = daily_df.log_diff_trade_price.values[1]

    return y_true


@task()
def print_csv_head(file_loc, **kwargs):
    import logging

    import pandas as pd

    logger = logging.getLogger(__name__)

    df = pd.read_csv(file_loc)
    logger.info(df.head())
