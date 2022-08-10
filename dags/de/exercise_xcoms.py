import logging

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Reference: https://github.com/dydwnsekd/airflow_example/blob/main/dags/xcom.py

# KST = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id="xcoms_exercise",
    schedule_interval="@once",
    start_date=pendulum.datetime(2022, 7, 20, tz="Asia/Seoul"),
)


def return_xcom():
    return "xcom!"


def xcom_push_test(**context):
    logger = logging.getLogger(__name__)
    xcom_value = "xcom_push_value"
    context["task_instance"].xcom_push(key="xcom_push_value", value=xcom_value)

    # TypeError: Object of type DateTime is not JSON serializable
    # xcom_value2 = {'a':'b', 'd': pendulum.datetime(2022, 7, 20, tz="Asia/Seoul")}
    xcom_value2 = {"a": "b", "d": "c"}
    context["task_instance"].xcom_push(key="xcom_push_value2", value=xcom_value2)
    logger.info("#" * 200)
    logger.info(context)
    logger.info("#" * 200)
    return "xcom_return_value", {"a": "b"}


def xcom_pull_test(**context):
    logger = logging.getLogger(__name__)
    xcom_return = context["task_instance"].xcom_pull(task_ids="return_xcom")
    xcom_push_value = context["ti"].xcom_pull(key="xcom_push_value")
    xcom_push_return_value = context["ti"].xcom_pull(task_ids="xcom_push_task")

    logger.info("#" * 200)
    logger.info("xcom_return : {}".format(xcom_return))
    logger.info("xcom_push_value : {}".format(xcom_push_value))
    # logger.info("xcom_push_return_value : {}".format(xcom_push_return_value))
    logger.info("#" * 200)
    logger.info("1 : {}".format(context))
    logger.info("2 : {}".format(context.keys()))
    # logger.info("3 : {}".format(context["task_instance"].keys()))
    logger.info("4 : {}".format(context["task_instance"]))
    logger.info("5 : {}".format(context["ti"]))
    logger.info("#" * 200)
    xcom_push_value2 = context["ti"].xcom_pull(key="xcom_push_value2")
    logger.info("6 : {}".format(xcom_push_value2))
    logger.info("7 : {}".format(xcom_push_return_value))
    temp = next(iter(context["task"].upstream_task_ids))
    logger.info("8 : {}".format(temp))
    logger.info("#" * 200)


return_xcom_task = PythonOperator(
    task_id="return_xcom", python_callable=return_xcom, dag=dag
)

xcom_push_task = PythonOperator(
    task_id="xcom_push_task", python_callable=xcom_push_test, dag=dag
)

xcom_pull_task = PythonOperator(
    task_id="xcom_pull_task", python_callable=xcom_pull_test, dag=dag
)

return_xcom_task >> xcom_push_task >> xcom_pull_task
