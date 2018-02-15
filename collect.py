import json

import pandas as pd
from time import sleep
from bokeh.plotting import figure, show, output_file, save
from bokeh.models import LinearAxis, Range1d, LogAxis
from bokeh.palettes import Spectral4

from constants import currencies, periods, remote
from sources.historical import CryptowatchSource, KunaIoSource
from parsers.rates import fit_rates, orderbook_to_record

def init_frame(file_name, columns=None):
    print("[info] Loading data for {}".format(file_name))
    try:
        res = pd.read_pickle(file_name)
        res.sort_index(inplace=True)
        print("[info] Picked up data from {}".format(file_name))
    except BaseException as e:
        print(e)
        res = pd.DataFrame(columns=columns)
        res['Time'] = pd.to_datetime(res.timestamp, unit="s")
        res.set_index('Time', inplace=True)
        res.sort_index(inplace=True)
    return res

def update_df(df, data, duplicates):
    tmp = pd.DataFrame(data, columns=['timestamp', 'id', 'created_at', 'price', 'volume'])
    tmp['Time'] = pd.to_datetime(tmp.timestamp, unit="s")
    tmp.set_index('Time', inplace=True)
    rv = pd.concat([df, tmp]).drop_duplicates(subset=duplicates)
    rv.sort_index(inplace=True)
    return rv


if __name__ == "__main__":

    # Arbitrage theory testing
    # 1. get data from sources 1 and 2
    # 1a. Convert UAH
    # 2. Convert to dataframe with 'source1', 'source2'
    # 3. Plot
    # Provision rates
    print("[info] Starting up.")

    source_rates = CryptowatchSource(currency=currencies.BTC)
    target_rates = KunaIoSource(currency=currencies.BTC)

    # 1. Pick up where we left off, if any

    source_df: pd.DataFrame = init_frame("source_df.pkl", columns=['timestamp', 'id', 'created_at', 'price', 'volume'])
    target_df: pd.DataFrame = init_frame("target_df.pkl", columns=['timestamp', 'id', 'created_at', 'price', 'volume'])
    order_df: pd.DataFrame = init_frame("order_df.pkl", columns=['timestamp', 'bid', 'ask', 'bid_volume', 'bid_weight', 'ask_volume', 'ask_weight'])

    while True:
        source_data = source_rates.fetch_latest_trades(limit=100)
        target_data = target_rates.fetch_latest_trades()
        order_book = target_rates.fetch_order_book()

        # Process source to dataframe
        source_df = update_df(source_df, source_data, 'timestamp')

        # Process target to DataFrame
        target_df = update_df(target_df, target_data, 'id')

        # Add new order book call
        record = orderbook_to_record(order_book)
        if record['timestamp'] is not None:
            tmp_book = pd.Series(record)
            order_df.loc[pd.to_datetime(tmp_book.timestamp, unit="s")] = tmp_book

        pd.to_pickle(source_df, 'source_df.pkl')
        pd.to_pickle(target_df, 'target_df.pkl')
        pd.to_pickle(order_df, 'order_df.pkl')
        sleep(0)
