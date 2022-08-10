
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

def get_data_save_csv(templates_dict):
    """Get Data from MongoDB and save as csv file."""
    import pandas as pd
    import os
    
    start_time = templates_dict["start_time"]
    db_name = templates_dict["db_name"]
    collection_name = templates_dict["collection_name"]
    query = templates_dict["query"]

    client = _get_mongo_client()
    db = client[db_name]
    result_df = pd.DataFrame(list(db[collection_name].find(query)))
    client.close()
    os.makedirs('/data/csvs/', exist_ok=True)
    result_df.loc[:,result_df.columns!='_id'].to_csv(f"/data/csvs/{db_name}_{collection_name}_{start_time}.csv", index=False)


def print_csv_head(templates_dict):
    import pandas as pd
    import logging
    logger = logging.getLogger(__name__)

    start_time = templates_dict["start_time"]
    db_name = templates_dict["db_name"]
    collection_name = templates_dict["collection_name"]
    
    result_df = pd.read_csv(f"/data/csvs/{db_name}_{collection_name}_{start_time}.csv")
    logger.info(result_df.head())
