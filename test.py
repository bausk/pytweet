import json

from functools import singledispatch
from datetime import datetime

from twitter import User

from constants import currencies, periods
from source_twitter.datasources import get_users, get_user_tweets
from sources.historical import BitfinexSource
from models.candle import Candle

from parsers import words

import pandas as pd


from bokeh.plotting import figure, show, output_file
from bokeh.models import LinearAxis, Range1d, LogAxis

from bokeh.palettes import viridis

@singledispatch
def to_serializable(val):
    """Used by default."""
    return str(val)

@to_serializable.register(datetime)
def ts_datetime(val):
    """Used if *val* is an instance of datetime."""
    return val.isoformat() + "Z"

if __name__ == "__main__":

    # Provision users
    users = []
    try:
        with open('users.json', 'r') as infile:
            users = json.load(infile)
            users = [User(**x) for x in users]
    except BaseException:
        users = get_users("bitcoin", 1000)
        with open('users.json', 'w') as outfile:
            json.dump([u._json for u in users], outfile)

    # Provision tweets
    all_tweets = []
    try:
        with open('tweets.json', 'r') as infile:
            all_tweets = json.load(infile)
    except BaseException:
        for user in users:
            tweets = get_user_tweets(user.screen_name, method="scraper", count=20000, since="2018-01-15", until="2018-01-17")
            all_tweets.extend(tweets)

        with open('tweets.json', 'w') as outfile:
            if len(all_tweets) > 0 and hasattr(all_tweets[0], "_json"):
                json.dump([t._json for t in all_tweets], outfile)
            else:
                json.dump([d.__dict__ for d in all_tweets], outfile, default=to_serializable)

    # Provision rates
    rates = []
    try:
        with open('rates.json', 'r') as infile:
            rates = json.load(infile)
    except BaseException:
        currency = BitfinexSource(currency=currencies.BTC, period=periods.P30M)
        results = currency.fetch_historical(after="2018-01-15", before="2018-01-17")
        with open('rates.json', 'w') as outfile:
            json.dump(results, outfile)

    candles = [Candle(x) for x in rates]
    trades = [tuple([int(x[0]/1000)] + x[1:]) for x in rates]

    # Process trades to dataframe
    trades_df = pd.DataFrame.from_records(trades, columns=[
        "timestamp", "open", "close", "high", "low", "volume"
    ])
    trades_df['Time'] = pd.to_datetime(trades_df.timestamp, unit="s")
    trades_df.set_index('Time', inplace=True)

    # Process tweets to DataFrame
    tweets_df = pd.DataFrame(all_tweets)
    tweets_df['Time'] = pd.to_datetime(tweets_df.date)
    tweets_df.set_index('Time', inplace=True)
    tweets_df = tweets_df.tz_localize("EET").tz_convert("UTC")
    tweets_agg = tweets_df.resample("5T").count()


    inc = trades_df.close > trades_df.open
    dec = trades_df.open > trades_df.close
    w = 60 / 4 * 60 * 1000 # 1/4 Hour in ms

    TOOLS = "pan,wheel_zoom,xwheel_zoom,box_zoom,reset,save"
    p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=1000, title="MSFT Candlestick", y_range=(10000, 15000))

    p.grid.grid_line_alpha = 0.3
    p.segment(trades_df.index, trades_df.high, trades_df.index, trades_df.low, color="black")
    p.vbar(trades_df.index[inc], w, trades_df.open[inc], trades_df.close[inc], fill_color="#D5E1DD", line_color="black")
    p.vbar(trades_df.index[dec], w, trades_df.open[dec], trades_df.close[dec], fill_color="#F2583E", line_color="black")

    p.extra_y_ranges = {
        "foo": Range1d(start=0, end=1000),
        'wordlines': Range1d(start=-10, end=10)
    }
    p.line(tweets_agg.index, tweets_agg.id, color='firebrick', alpha=0.8, y_range_name="foo")
    p.add_layout(LinearAxis(y_range_name="foo"), 'left')
    p.add_layout(LinearAxis(y_range_name="wordlines"), 'left')

    keywords = [
        'btc',
        'bitcoin',
        'buy',
        'sell',
        'bad',
        'good',
        'up',
        'down'
    ]
    word_counts = words.nltk_calculate_words(tweets_df.text)
    word_frequencies = words.nltk_word_frequencies(tweets_df, keywords)

    for word, color in zip(keywords, viridis(len(keywords))):
        p.line(word_frequencies.index, word_frequencies[word],
               line_width=2, color=color, alpha=0.8, legend=word,
               y_range_name='wordlines'
               )
    p.legend.location = 'top_left'
    p.legend.click_policy = 'hide'


    output_file("candlestick.html", title="candlestick.py example")
    show(p)  # open a browser

    print("Bye")



