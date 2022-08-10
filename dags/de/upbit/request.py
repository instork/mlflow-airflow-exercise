import json
import logging
import random
import time
from typing import Dict, List

import requests



def _get_minutes_ohlcvs(
    interval: int,
    ticker: str,
    to: str,
    count: int,
) -> List[Dict]:
    """Get ohlcvs until datetime 'to'."""

    url = f"https://api.upbit.com/v1/candles/minutes/{interval}?market={ticker}&to={to}&count={count}"
    headers = {"Accept": "application/json"}
    # time.sleep(req_time_interval)
    time.sleep(random.random())
    response = requests.get(url, headers=headers)
    response = json.loads(response.text)

    return response


def fetch_minute_ohlcvs(templates_dict, **context):
    """Get ohlcvs and save."""
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


# import requests
# import datetime as dt
# import pendulum
# import json
# ETZ = pendulum.timezone("US/Eastern")
# cur_time = dt.datetime(2022, 6, 1, 0, 0, tzinfo=ETZ)
# utc_timezone = pendulum.timezone("UTC")

# interval = 60
# count = 1
# ticker = 'USDT-BTC'

# to = utc_timezone.convert(cur_time).strftime("%Y-%m-%d %H:%M:%S")

# url = f"https://api.upbit.com/v1/candles/minutes/{interval}?market={ticker}&to={to}&count={count}"

# headers = {"Accept": "application/json"}
# response = requests.request("GET", url, headers=headers)
# data = json.loads(response.text)

# [{'market': 'USDT-BTC',
#   'candle_date_time_utc': '2022-06-01T03:00:00', # 시봉 시작시간
#   'candle_date_time_kst': '2022-06-01T12:00:00',
#   'opening_price': 31577.814,
#   'high_price': 31577.814,
#   'low_price': 31524.802,
#   'trade_price': 31565.865,
#   'timestamp': 1654054297711,
#   'candle_acc_trade_price': 32358.17887078,
#   'candle_acc_trade_volume': 1.02595656,
#   'unit': 60}]

# logger.info(f"=" * 100)
# logger.info(f"{type(response)}")
# logger.info(f"{response.text}")
# logger.info(f"=" * 100)
# https://docs.upbit.com/reference/%EC%A0%84%EC%B2%B4-%EA%B3%84%EC%A2%8C-%EC%A1%B0%ED%9A%8C
