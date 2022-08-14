from airflow.decorators import task

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
def get_data_save_csv(db_name, collection_name, query, **kwargs):
    """Get Data from MongoDB and save as csv file."""
    import pandas as pd
    import os
    
    start_time = kwargs["data_interval_end"]
    client = _get_mongo_client()
    db = client[db_name]
    result_df = pd.DataFrame(list(db[collection_name].find(query)))
    client.close()
    os.makedirs('/data/csvs/', exist_ok=True)
    file_loc = f"/data/csvs/{db_name}_{collection_name}_{start_time}.csv"
    result_df.loc[:,result_df.columns!='_id'].to_csv(file_loc, index=False)
    return file_loc

@task()
def print_csv_head(file_loc, **kwargs):
    import pandas as pd
    import logging
    logger = logging.getLogger(__name__)
    
    result_df = pd.read_csv(file_loc)
    logger.info(result_df.head())
