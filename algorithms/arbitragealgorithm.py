from datetime import timedelta, datetime

from constants.constants import DECISIONS
from models.algorithm import BaseAlgorithm
from persistence.mixins import WithConsole


class ArbitrageAlgorithm(WithConsole, BaseAlgorithm):
    rate: timedelta = timedelta(milliseconds=10000)
    cutaway: timedelta = timedelta(minutes=20)
    step: str = '1T'
    rolling_window: str = '2h'
    min_ask_bid_ratio = 0.0035
    buy_threshold: int = 8
    sell_threshold: int = -5
    bars_shift: int = 10

    def _data_ok(self, source_df, order_book):
        if len(order_book) < 10:
            return False
        return True

    def signal(self, source_df, order_book):
        print("[info] Performing analysis with Simple analyzer...")
        current_datetime = datetime.now()

        if not self._data_ok(source_df, order_book):
            return {
                'buy': 0.0,
                'sell': 0.0,
                'buy_datetime': current_datetime,
                'sell_datetime': current_datetime,
                'decision': DECISIONS.NO_DATA
            }

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
        self.log("[info] Calculated indicators")
        return {
            'buy': weighted_arbitrage_indicator.iloc[-1],
            'sell': weighted_sell_indicator.iloc[-1],
            'buy_datetime': weighted_arbitrage_indicator.index[-1],
            'sell_datetime': weighted_sell_indicator.index[-1],
            'decision': self._decide(weighted_arbitrage_indicator, weighted_sell_indicator)
        }

    def _decide(self, buy, sell):
        result = DECISIONS.AMBIGUOUS
        if buy > self.buy_threshold:
            result = DECISIONS.BUY_ALL
        if sell < self.sell_threshold:
            # Sell signal takes priority over buy signal, better bail out of risky position
            result = DECISIONS.SELL_ALL
        return result

    def _calculate_profit(self, weighted_arbitrage_indicator, weighted_sell_indicator, ord_df):
        buy_condition = (weighted_arbitrage_indicator > self.buy_threshold) & (
                weighted_arbitrage_indicator.shift(1) <= self.buy_threshold)
        sell_condition = (weighted_sell_indicator < self.sell_threshold) & (
                weighted_sell_indicator.shift(1) >= self.sell_threshold)

        buy_condition.name = "buy"
        sell_condition.name = "sell"
        combined = pd.concat([ord_df, buy_condition, sell_condition], axis=1)
        deals = []
        new_deal = {
            'status': None,
            'buytime': None,
            'selltime': None,
            'buyprice': None,
            'sellprice': None,
            'profit': 0.0
        }
        current_deal = {}
        current_deal.update(new_deal)
        for idx, row in combined.iterrows():
            buy = row.buy
            sell = row.sell
            if buy and current_deal['status'] is None:
                # Open the deal
                current_deal['status'] = True
                current_deal['buytime'] = idx
                current_deal['buyprice'] = row.ask
            if sell and current_deal['status'] is not None:
                # Close the deal and wipe current
                current_deal['selltime'] = idx
                current_deal['sellprice'] = row.bid
                current_deal['profit'] = (current_deal['sellprice'] - current_deal['buyprice'] - current_deal[
                    'buyprice'] * 0.005) / current_deal['buyprice'] * 100
                # self._console.text += str(current_deal['profit']) + "%\n"
                deals.append(current_deal)
                current_deal = {}
                current_deal.update(new_deal)

        return deals
