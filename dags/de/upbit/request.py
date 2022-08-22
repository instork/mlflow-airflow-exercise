from typing import Dict, List


def _get_minutes_ohlcvs(
    interval: int,
    ticker: str,
    to: str,
    count: int,
) -> List[Dict]:
    """Get ohlcvs until datetime 'to'."""

    import json
    import random
    import time

    import requests

    url = f"https://api.upbit.com/v1/candles/minutes/{interval}?market={ticker}&to={to}&count={count}"
    headers = {"Accept": "application/json"}
    # time.sleep(req_time_interval)
    time.sleep(random.random())
    response = requests.get(url, headers=headers)
    response = json.loads(response.text)

    return response


def fetch_minute_ohlcvs(templates_dict, **context):
    """Get ohlcvs and save."""
    import logging

    import pendulum

    logger = logging.getLogger(__name__)
    minute_interval = templates_dict["minute_interval"]
    get_cnt = templates_dict["get_cnt"]
    coin_ticker = templates_dict["coin_ticker"]

    # 이미 UTC로 변환됨, 2020-01-02T05:00:00+00:00
    start_time = templates_dict["start_time"]
    logger.info(start_time)
    start_time = pendulum.from_format(start_time, "YYYY-MM-DDTHH:mm:ssZ")
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")

    ohlcvs = _get_minutes_ohlcvs(minute_interval, coin_ticker, start_time, get_cnt)
    return ohlcvs
