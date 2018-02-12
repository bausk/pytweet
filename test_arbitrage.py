import json
from functools import singledispatch
from datetime import datetime

import pandas as pd
from time import sleep
from bokeh.plotting import figure, show, output_file, save
from bokeh.models import LinearAxis, Range1d, LogAxis
from bokeh.palettes import Spectral4

from constants import currencies, periods, remote
from sources.historical import CryptowatchSource, KunaIoSource
from models.candle import Candle
from parsers import words


if __name__ == "__main__":

    # Arbitrage theory testing
    # 1. get data from sources 1 and 2
    # 1a. Convert UAH
    # 2. Convert to dataframe with 'source1', 'source2'
    # 3. Plot
    # Provision rates

    source_rates = CryptowatchSource(currency=currencies.BTC)
    target_rates = KunaIoSource(currency=currencies.BTC)

    source_data = source_rates.fetch_latest_trades(limit=50)
    target_data = target_rates.fetch_latest_trades()

    source_df = pd.DataFrame(source_data)
    source_df['Time'] = pd.to_datetime(source_df.timestamp, unit="s")
    source_df.set_index('Time', inplace=True)
    source_df.sort_index(inplace=True)


    target_df = pd.DataFrame(target_data)
    target_df['Time'] = pd.to_datetime(target_df.timestamp, unit="s")
    target_df.set_index('Time', inplace=True)
    target_df.sort_index(inplace=True)

    operand = source_df if (source_df.index[0] < target_df.index[0]) else target_df
    start_time = max(source_df.index[0], target_df.index[0])
    if len(operand.truncate(start_time)) < 1:
        start_time = operand.index[-1]

    source_df = source_df.truncate(start_time)
    target_df = target_df.truncate(start_time)

    target_coeff = float(source_df['price'].iloc[0] / target_df['price'].iloc[0])
    target_df['price'] = target_df['price'] * target_coeff

    while True:
        source_data = source_rates.fetch_latest_trades(limit=50)
        target_data = target_rates.fetch_latest_trades()

        # Process source to dataframe
        source_tmp = pd.DataFrame(source_data, columns=['timestamp', 'id', 'created_at', 'price', 'volume'])
        source_tmp['Time'] = pd.to_datetime(source_tmp.timestamp, unit="s")
        source_tmp.set_index('Time', inplace=True)
        source_df = pd.concat([source_df, source_tmp]).drop_duplicates(subset='timestamp')
        source_df.sort_index(inplace=True)
        source_df = source_df.truncate(start_time)

        # Process target to DataFrame
        target_tmp = pd.DataFrame(target_data, columns=['timestamp', 'id', 'created_at', 'price', 'volume'])
        target_tmp['Time'] = pd.to_datetime(target_tmp.timestamp, unit="s")
        target_tmp.set_index('Time', inplace=True)
        target_tmp['price'] = target_tmp['price'] * target_coeff
        target_df = pd.concat([target_df, target_tmp]).drop_duplicates(subset='id')
        target_df.sort_index(inplace=True)
        target_df = target_df.truncate(start_time)

        TOOLS = "pan,wheel_zoom,xwheel_zoom,box_zoom,reset,save"
        p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=1000, title="Arbitrage", y_range=(8000, 9000))

        p.grid.grid_line_alpha = 0.3

        p.line(source_df.index, source_df['price'], line_width=3, color=Spectral4[0], alpha=0.8, legend="Gax")
        p.line(target_df.index, target_df['price'], line_width=3, color=Spectral4[1], alpha=0.8, legend="Kuna")


        output_file("arbitrage.html", title="Arbitrage between cryptowat.ch and kuna.io")
        save(p)
        sleep(0.1)
