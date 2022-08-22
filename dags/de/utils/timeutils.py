import datetime as dt
from typing import Dict, List

import pendulum
from pendulum.datetime import DateTime
from pendulum.tz.timezone import Timezone

KST = pendulum.timezone("Asia/Seoul")
ETZ = pendulum.timezone("US/Eastern")  # EST/EDT
UTC = pendulum.timezone("UTC")


def str2datetime(s: str, format: str):
    """string to datetime."""
    return dt.datetime.strptime(s, format)


def json_strptime(
    json_dicts: List[Dict],
    dict_keys: List[str] = ["candle_date_time_utc", "candle_date_time_kst"],
):
    """dictionary's string datetime to datetime datetime."""
    for key in dict_keys:
        for data in json_dicts:
            new_time = str2datetime(data[key], "%Y-%m-%dT%H:%M:%S")
            data.update({key: new_time})
    return json_dicts


def pend2datetime(p_time: DateTime):
    """pedulum datetime to datetime datetime."""
    datetime_string = p_time.to_datetime_string()
    dt_datetime = dt.datetime.fromisoformat(datetime_string)
    return dt_datetime


def get_str_date_before_from_ts(
    ts: str, date_format: str = "%Y-%m-%d", tz: Timezone = ETZ
) -> str:
    """
    Get string datetime from ts(start_time). start time automatically converted to UCT.
    Chagege to tz(ETZ, default) and returns to string date_format.
    Using on de/fred/request.py , de/googlenews/request.py
    """
    # https://github.com/sdispater/pendulum/blob/master/docs/docs/string_formatting.md
    start_time = pendulum.from_format(ts, "YYYY-MM-DDTHH:mm:ssZ")
    etz_time = tz.convert(start_time).subtract(minutes=1)  # to get data day before
    start_date = etz_time.strftime(date_format)
    return start_date


def get_datetime_from_ts(
    ts: str, get_day_before=False, tz: Timezone = ETZ
) -> dt.datetime:
    """
    Get dt.datetime form ts(start_time).
    Using on data2mongo.py
    """
    cur_time = pendulum.from_format(ts, "YYYY-MM-DDTHH:mm:ssZ")
    converted_time = tz.convert(cur_time)
    if get_day_before:
        converted_time = converted_time.subtract(minutes=1)
    converted_time = pend2datetime(converted_time)
    return converted_time
