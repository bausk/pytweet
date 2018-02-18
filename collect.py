import pandas as pd
from time import sleep

from constants import currencies, periods, remote
from sources.historical import CryptowatchSource, KunaIoSource
from parsers.rates import orderbook_to_record
from persistence.postgre import TimeSeriesStore
from constants import formats
from datetime import timedelta


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
    print("[info] Starting up...")

    source_rates = CryptowatchSource(currency=currencies.BTC)
    target_rates = KunaIoSource(currency=currencies.BTC)

    # 1. Pick up where we left off, if any
    source_store = TimeSeriesStore(name="bitfinex_btcusd", columns=formats.history_format, time_unit="s", time_field="timestamp", update_period=timedelta(seconds=30))
    target_store = TimeSeriesStore(name="kuna_btcuah", columns=formats.history_format, time_unit="s", update_period=timedelta(seconds=30))
    orderbook_store = TimeSeriesStore(name="kuna_orderbook", columns=formats.orderbook_format, time_unit="s", update_period=timedelta(seconds=30))


    while True:
        source_store.append(source_rates.fetch_latest_trades(limit=100))
        target_store.append(target_rates.fetch_latest_trades())
        order_book = target_rates.fetch_order_book()

        # Add new order book call
        record = orderbook_to_record(order_book)
        if record['timestamp'] is not None:
            tmp_book = pd.Series(record)
            orderbook_store.append(tmp_book)

        sleep(1)
        # print(source_store)
