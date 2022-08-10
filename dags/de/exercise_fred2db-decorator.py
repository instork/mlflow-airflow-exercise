import datetime as dt

from airflow.decorators import dag, task
from de.utils.timeutils import ETZ

########################### Set Configs ###########################
SCHEDULE_INTERVAL = "0 0 * * *"  # At 00:00
INDEX_UNIQUE = False
################################################################
# https://github.com/apache/airflow/blob/main/airflow/example_dags/tutorial_taskflow_api_etl_virtualenv.py


@dag(
    dag_id="fred2db-decorator",
    description="Get fred data and news and load on MongoDB",
    start_date=dt.datetime(2020, 1, 1, 0, 0, tzinfo=ETZ),
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
    default_args={
        "depends_on_past": True,
        "retries": 3,
        "retry_delay": dt.timedelta(minutes=2),
    },
)
def fred2db():
    fred_series_tickers = ["T5YIE", "T5YIFR", "T10YIE", "T10Y2Y", "SP500", "DJIA"]
    fred_templates_dict = {
        "fred_series_tickers": fred_series_tickers,
        "start_time": "{{ ts_nodash }}",
    }
    fred_mongo_templates_dict = {
        "db_name": "test_db",
        "collection_name": "fred",
        "start_time": "{{ ts_nodash }}",
    }

    @task.virtualenv(
        task_id="insert_fred_to_mongodb",
        requirements=["fredapi==0.4.3", "python-dotenv == 0.20.0"],
        system_site_packages=False,
    )
    def fetch_fred(templates_dict, **context):
        import os
        import random
        import time

        import pendulum
        from dotenv import load_dotenv
        from fredapi import Fred

        # from de.utils.timeutils import get_str_date_before_from_ts

        load_dotenv("/tmp/fred.env")
        fred_api_key = os.getenv("FRED_API_KEY")
        fred = Fred(api_key=fred_api_key)

        fred_series_tickers = templates_dict["fred_series_tickers"]
        start_time = templates_dict["start_time"]
        # start_date = get_str_date_before_from_ts(start_time, date_format="%Y-%m-%d")
        start_time = pendulum.from_format(
            start_time, "YYYYMMDDTHHmmss", tz=pendulum.timezone("UTC")
        )
        start_time = (
            pendulum.timezone("US/Eastern").convert(start_time).subtract(minutes=1)
        )
        start_date = start_time.strftime("%Y-%m-%d")

        fred_data = {}

        for ticker in fred_series_tickers:
            time.sleep(random.random())
            cur_data = fred.get_series(
                ticker, observation_start=start_date, observation_end=start_date
            )
            if len(cur_data) == 1:
                fred_data[ticker] = float(cur_data.values[0])
            else:
                fred_data[ticker] = None

        return fred_data

    @task(
        task_id="fetch_fred_task",
        templates_dict=fred_templates_dict,
    )
    def insert_single(single_dict, templates_dict=fred_mongo_templates_dict, **context):
        import logging

        from de.utils.timeutils import get_datetime_from_ts
        from mongodb import get_mongo_client

        logger = logging.getLogger(__name__)

        start_time = templates_dict["start_time"]
        db_name = templates_dict["db_name"]
        collection_name = templates_dict["collection_name"]

        etz_time = get_datetime_from_ts(start_time, get_day_before=True)

        # prev_task_id = next(iter(context["task"].upstream_task_ids))
        # single_dict = context["task_instance"].xcom_pull(task_ids=prev_task_id)
        single_dict.update(etz_time=etz_time)
        # Get database
        mongo_client = get_mongo_client()
        db = mongo_client[db_name]
        if collection_name not in db.list_collection_names():
            try:
                db.create_collection(collection_name)
                db[collection_name].create_index([("etz_time", 1)], unique=INDEX_UNIQUE)
            except Exception as e:
                logger.info(e)

        db[collection_name].insert_one(single_dict)

    # task flow
    fred_data = fetch_fred(templates_dict=fred_templates_dict)
    insert_single(single_dict=fred_data, templates_dict=fred_mongo_templates_dict)


fred2db_decorator_dag = fred2db()
