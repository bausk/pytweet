import json
from functools import singledispatch
from datetime import datetime, timedelta

import pandas as pd
from time import sleep
from bokeh.plotting import figure, show, output_file, save
from bokeh.models import LinearAxis, Range1d, LogAxis
from bokeh.palettes import Spectral4

from constants import currencies, periods, remote
from sources.historical import CryptowatchSource, KunaIoSource
from models.candle import Candle
from parsers import words

from plots.bokeh import DatetimePlot

if __name__ == "__main__":

    # Arbitrage theory testing 2
    # Load two chart files
    # resample to equal dates, using only recent data
    # try to fit using formulas

    # 1. Load
    target_df = pd.read_pickle('target_df.pkl')
    target_df.sort_index(inplace=True)

    source_df = pd.read_pickle('source_df.pkl')
    source_df.sort_index(inplace=True)

    # 2. Take recent data: la
    end_time = min(source_df.index[-1], target_df.index[-1]) - timedelta(minutes=10)
    start_time = end_time - timedelta(hours=2)
    target_frame = target_df.truncate(before=start_time, after=end_time, copy=True)
    source_frame = source_df.truncate(before=start_time, after=end_time, copy=True)

    # 2a. Plot
    p = DatetimePlot(title="Arbitrage check")
    p.line(source_frame.index, source_frame['price'], legend="Gax")
    p.line(target_frame.index, target_frame['price'], legend="Kuna.io")
    p.circle(target_frame.index, target_frame['price'], size=target_frame['volume'].multiply(100), legend="Kuna.io volumes")
    output_file('curvefit1.html', title='Curvefitting test')
    save(p._obj)
    # show(p._obj)

    # 3. Let's try to find mean
    source_frame = source_frame.resample('1T').mean().interpolate()
    target_frame = target_frame.resample('1T').mean().interpolate()
    source_mean = source_frame['price'].mean()
    target_mean = target_frame['price'].mean()
    coeff = source_mean / target_mean


    rough_s_mean = source_df['price'].resample('5T').mean().interpolate().mean()
    rough_t_mean = target_df['price'].resample('5T').mean().interpolate().mean()
    rough_coeff = rough_s_mean / rough_t_mean

    target_df['price_coeff'] = target_df['price'].multiply(coeff)
    target_df['price_rcoeff'] = target_df['price'].multiply(rough_coeff)

    # target_frame['price_coeff'] = target_frame['price'].multiply(coeff)
    # target_frame['price_rcoeff'] = target_frame['price'].multiply(rough_coeff)


    p2 = DatetimePlot(title="Arbitrage check, fitted")
    p2.line(source_df.index, source_df['price'], legend="Gax")

    # p2.line(target_frame.index, target_frame['price_coeff'], legend="Kuna.io trunc coeff")
    # p2.circle(target_frame.index, target_frame['price_coeff'], size=target_frame['volume'].multiply(100), legend="Kuna.io volumes")

    p2.line(target_df.index, target_df['price_coeff'], legend="Kuna.io overall coeff")
    p2.circle(target_df.index, target_df['price_coeff'], size=target_df['volume'].multiply(100), legend="Kuna.io volumes")
    output_file('curvefit2.html', title='Curvefitting test - Results')
    save(p2._obj)
    # show(p2._obj)
