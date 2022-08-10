def fetch_news(templates_dict, **context):
    import logging
    import random
    import time

    from de.utils.timeutils import get_str_date_before_from_ts
    from GoogleNews import GoogleNews

    logger = logging.getLogger(__name__)

    queries = templates_dict["queries"]
    start_time = templates_dict["start_time"]
    #  mm/dd/yyyy
    logger.info(start_time)
    start_date = get_str_date_before_from_ts(start_time, "%m/%d/%Y")

    results = {k: [] for k in queries.keys()}
    for key in queries.keys():
        for q in queries[key]:
            time.sleep(random.randint(30, 60) + random.random())
            googlenews = GoogleNews(start=start_date, end=start_date)
            googlenews.search(q)
            result = googlenews.get_texts()
            googlenews.clear()
            results[key] += result

        results[key] = list(set(results[key]))
        if len(results[key]) == 0:
            logger.info(results)
            assert len(results[key]) != 0

    return results


# {'BTC-News':
# ["Tesla Sold Most of Its Bitcoin to Shore Up Carmaker's Liquidity",
#  'What to do with bitcoin: Tesla dumps a big chunk if its holdings - should you?',
# ....]
#  'ETH-News':
#  ["Will 'Ethereum Killers' takeover ETH in 2022?",
#  'Ethereum will reach 100,000 transactions per second',
# ...]
#  }
