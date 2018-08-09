from datetime import timedelta, datetime

from constants.constants import DECISIONS
from models.algorithm import BaseAlgorithm
from models.signal import Signal
from persistence.mixins import WithConsole


class ArbitrageAlgorithm(WithConsole, BaseAlgorithm):
    rate: timedelta = timedelta(milliseconds=1000)
    cutaway: timedelta = timedelta(minutes=20)
    step: str = '1T'
    rolling_window: str = '2h'
    min_ask_bid_ratio = 0.0035
    buy_threshold: int = 8
    sell_threshold: int = -5
    bars_shift: int = 10

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.latest_dataframe = None

    def _data_ok(self, source_df, order_book):
        if len(order_book) < 20:
            return False
        return True

    def signal(self, source_df, order_book):
        print("[info] Performing analysis with Simple analyzer...")
        current_datetime = datetime.now()

        if not self._data_ok(source_df, order_book):
            return Signal(**{
                'buy': 0.0,
                'sell': 0.0,
                'buy_datetime': current_datetime,
                'sell_datetime': current_datetime,
                'decision': DECISIONS.NO_DATA
            })

        src_df = source_df.resample(self.step).mean().interpolate()
        ord_df = order_book.resample(self.step).mean().interpolate()

        # Curvefitting of both charts
        shifted_target = ((ord_df['ask'] + ord_df['bid']) / 2).rolling(self.rolling_window).mean().shift(
            self.bars_shift)
        shifted_source = src_df['price'].rolling(self.rolling_window).mean().shift(self.bars_shift)
        coeffs = shifted_target / shifted_source
        self.log("Curvefitting for {} datapoints...".format(len(coeffs)))
        normalized_ask_bid_distance = ord_df['ask'] - ord_df['bid']
        minimum_ask_bid_distance = ord_df['bid'] * self.min_ask_bid_ratio

        # Hedge against commission
        normalized_ask_bid_distance.loc[
            normalized_ask_bid_distance < minimum_ask_bid_distance
            ] = minimum_ask_bid_distance

        # Calculate inter-market slip difference
        arbitrage_difference = ((src_df['price'] * coeffs) - ord_df['ask'])
        # Get indicator
        arbitrage_indicator = (arbitrage_difference / normalized_ask_bid_distance).dropna()
        # filter indicator for values more than 1.5
        arbitrage_indicator.loc[arbitrage_indicator < 1.5] = 0
        weighted_arbitrage_indicator = arbitrage_indicator.rolling('180s').sum()
        sell_difference = ((src_df['price'] * coeffs) - ord_df['bid'])
        sell_indicator = (sell_difference / normalized_ask_bid_distance).dropna()
        sell_indicator.loc[sell_indicator > -0.8] = 0
        weighted_sell_indicator = sell_indicator.rolling('180s').sum()
        self.latest_dataframe = weighted_arbitrage_indicator
        self.log("[info] Calculated indicators")
        if len(weighted_arbitrage_indicator) == 0 or len(weighted_sell_indicator) == 0:
            return Signal(**{
                'buy': 0.0,
                'sell': 0.0,
                'buy_datetime': current_datetime,
                'sell_datetime': current_datetime,
                'decision': DECISIONS.NO_DATA
            })
        return Signal(**{
            'buy': round(weighted_arbitrage_indicator.iloc[-1], 2),
            'sell': round(weighted_sell_indicator.iloc[-1], 2),
            'buy_datetime': weighted_arbitrage_indicator.index[-1],
            'sell_datetime': weighted_sell_indicator.index[-1],
            'decision': self._decide(
                weighted_arbitrage_indicator.iloc[-1].item(),
                weighted_sell_indicator.iloc[-1].item()
            )
        })

    def _decide(self, buy, sell):
        result = DECISIONS.AMBIGUOUS
        if buy > self.buy_threshold:
            result = DECISIONS.BUY_ALL
        if sell < self.sell_threshold:
            # Sell signal takes priority over buy signal, better bail out of risky position
            result = DECISIONS.SELL_ALL
        return result
