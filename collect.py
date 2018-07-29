import pandas as pd
from time import sleep

from constants import currencies
from sources.historical import CryptowatchSource, KunaIoSource
from parsers.rates import orderbook_to_record
from models.timeseries import TimeSeriesStore
from constants import formats
from datetime import timedelta


if __name__ == "__main__":

    print("[info] Starting up...")

    source_rates = CryptowatchSource(currency=currencies.BTC)
    target_rates = KunaIoSource(currency=currencies.BTC)

    # 1. Pick up where we left off, if any
    source_store = TimeSeriesStore(name="bitfinex_btcusd", columns=formats.history_format, time_unit="s", time_field="timestamp", update_period=timedelta(seconds=30))
    target_store = TimeSeriesStore(name="kuna_btcuah", columns=formats.history_format, time_unit="s", update_period=timedelta(seconds=30))
    orderbook_store = TimeSeriesStore(name="kuna_orderbook", columns=formats.orderbook_format, time_unit="s", update_period=timedelta(seconds=30))

    while True:
        source_store.write(source_rates.fetch_latest_trades(limit=100))
        target_store.write(target_rates.fetch_latest_trades())
        order_book = target_rates.fetch_order_book()

        # Add new order book call
        record = orderbook_to_record(order_book)
        if record['timestamp'] is not None:
            tmp_book = pd.Series(record)
            orderbook_store.write(tmp_book)
        sleep(1)
