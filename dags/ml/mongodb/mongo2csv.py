from asyncio.log import logger
from airflow.decorators import task
import pandas as pd

def _get_mongo_client():
    """Get mongo client."""
    import os
    from dotenv import load_dotenv
    from pymongo import MongoClient

    load_dotenv("/tmp/mongo.env")
    user = os.getenv("MONGODB_USER")
    pwd = os.getenv("MONGODB_PWD")
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")
    client = MongoClient(f"mongodb://{user}:{pwd}@{host}:{port}")
    return client

@task()
def get_data_save_csv(db_name, collection_name, day_before, **kwargs):
    """Get Data from MongoDB and save as csv file."""
    import logging
    import pandas as pd
    import os
    from ml.validation.validation import check_missing

    logger = logging.getLogger(__name__)

    folder = "/data/csvs/train/1H"

    # pendulum datetime in UTC
    cur_time = kwargs["data_interval_end"]
    start_time = cur_time.subtract(days=day_before)

    client = _get_mongo_client()
    db = client[db_name]
    df = pd.DataFrame(list(db[collection_name].find({"utc_time":{"$gte":start_time, "$lt":cur_time}})))
    client.close()

    
    os.makedirs(folder, exist_ok=True)
    file_loc = os.path.join(folder, f"{db_name}_{collection_name}_{cur_time}_{day_before}.csv")
    df = df.loc[:,df.columns!="_id"]

    check_missing(df, "utc_time" , "1H")
    df.to_csv(file_loc, index=False)

    return file_loc
    

@task()
def print_csv_head(file_loc, **kwargs):
    import pandas as pd
    import logging
    logger = logging.getLogger(__name__)
    
    df = pd.read_csv(file_loc)
    logger.info(df.head())
