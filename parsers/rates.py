from datetime import timedelta, datetime
import pandas as pd
from dateutil import parser

def fit_rates(rate1: pd.DataFrame, rate2: pd.DataFrame, period=2, minutes=10):
    end_time = min(rate1.index[-1], rate2.index[-1]) - timedelta(minutes=minutes)
    start_time = end_time - timedelta(hours=period)
    target_frame: pd.DataFrame = rate2.truncate(before=start_time, after=end_time, copy=True)
    source_frame: pd.DataFrame = rate1.truncate(before=start_time, after=end_time, copy=True)

    source_frame = source_frame.resample('1T').mean().interpolate()
    target_frame = target_frame.resample('1T').mean().interpolate()
    source_mean = source_frame['price'].mean()
    target_mean = target_frame['price'].mean()
    return source_mean / target_mean

def orderbook_to_record(orderbook, coeff=1.0):
    res = dict(timestamp=None, bid=0.0, ask=0.0, bid_volume=0.0, bid_weight=0.0, ask_volume=0.0, ask_weight=0.0)
    if len(orderbook['asks']) == 0 or len(orderbook['bids']) == 0:
        return res
    try:
        res['ask'] = min([float(x['price']) for x in orderbook['asks']]) * coeff
        res['bid'] = max([float(x['price']) for x in orderbook['bids']]) * coeff

        volume = 0
        weight = 0
        for el in orderbook['asks']:
            volume += float(el['remaining_volume'])
            weight += float(el['remaining_volume']) * (res['ask'] - float(el['price']) * coeff)
        res['ask_volume'] = volume
        res['ask_weight'] = weight

        volume = 0
        weight = 0
        for el in orderbook['bids']:
            volume += float(el['remaining_volume'])
            weight += float(el['remaining_volume']) * (float(el['price']) - res['bid'] * coeff)
        res['bid_volume'] = volume
        res['bid_weight'] = weight

        res['timestamp'] = int(datetime.now().timestamp())
        return res
    except BaseException as e:
        print(e)
        return res