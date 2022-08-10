def fetch_fred(templates_dict, **context):
    import logging
    import os
    import random
    import time

    from de.utils.timeutils import get_str_date_before_from_ts
    from dotenv import load_dotenv
    from fredapi import Fred

    load_dotenv("/tmp/fred.env")

    logger = logging.getLogger(__name__)
    fred_api_key = os.getenv("FRED_API_KEY")
    fred = Fred(api_key=fred_api_key)

    fred_series_tickers = templates_dict["fred_series_tickers"]
    start_time = templates_dict["start_time"]
    logger.info(start_time)
    start_date = get_str_date_before_from_ts(start_time, date_format="%Y-%m-%d")

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


# {'T5YIE': 2.56,
#  'T5YIFR': 2.12,
#  'T10YIE': 2.34,
#  'T10Y2Y': -0.21,
#  'SP500': 3961.63,
#  'DJIA': 31899.29,
# }
