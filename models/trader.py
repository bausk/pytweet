from math import floor
from datetime import datetime, timedelta
import pandas as pd

from models.algorithm import BaseAlgorithm
from models.exchange import BaseExchangeInterface
from models.trade import Trade
from models.signal import Signal
from persistence.simple_store import InMemoryStore
from persistence.mixins import PrepareDataMixin, WithConsole
from parsers.rates import orderbook_to_series
from constants.formats import orderbook_format
from constants.constants import DECISIONS


class LiveTrader(WithConsole, PrepareDataMixin, InMemoryStore):
    current_status = DECISIONS.NO_DATA
    current_trade = None
    trade_history = []
    current_equity = 250

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._trade_api = None
        self._source_api = None
        self._target_api = None
        self._algorithm: BaseAlgorithm = None
        self._source_df = None
        self._orderbook = None
        self._target_trades = None
        self._cutoff = timedelta(hours=8)

    def _get_cutoff(self):
        if self._algorithm is not None and getattr(self._algorithm, 'cutaway', None):
            return self._algorithm.cutaway
        else:
            return self._cutoff

    def get_rate(self):
        return self._algorithm.rate.seconds

    def add_trader_api(self, api):
        self._trade_api: BaseExchangeInterface = api

    def add_source_api(self, api):
        self._source_api = api

    def add_target_api(self, api):
        self._target_api = api

    def add_algorithm(self, algorithm):
        self._algorithm = algorithm

    def signal_callback(self, *args, **kwargs):
        current_time = datetime.now()
        cutoff_delta = self._get_cutoff().seconds
        cutoff_timestamp = int(current_time.timestamp()) - cutoff_delta

        raw_source_data = self._source_api.fetch_latest_trades(limit=100)
        source_df = self._prepare_data(raw_source_data)
        self._source_df = pd.concat([self._source_df, source_df]).drop_duplicates(subset='timestamp')
        self._source_df.drop(source_df.index[source_df['timestamp'] < cutoff_timestamp], inplace=True)

        raw_orderbook = orderbook_to_series(self._target_api.fetch_order_book())
        orderbook = self._prepare_data(
            raw_orderbook,
            {'time_field': 'timestamp', 'columns': orderbook_format, 'time_unit': 's'}
        )
        self._orderbook = pd.concat([self._orderbook, orderbook]).drop_duplicates(subset='timestamp')
        self._orderbook.drop(orderbook.index[orderbook['timestamp'] < cutoff_timestamp], inplace=True)

        # Apply algorithm from analyzer
        signal_object: Signal = self._algorithm.signal(self._source_df, self._orderbook)
        self.log("[{}] {}/{} from {}/{} measurements".format(
            current_time,
            signal_object.buy,
            signal_object.sell,
            len(self._orderbook),
            len(self._source_df)
        ))
        return self._execute_signal(signal_object)

    def _execute_signal(self, signal: Signal):
        """
        State machine to move around the trader current deal from inactive to active and back
        :param signal:
        :return:
        """
        current_market = self._orderbook.dropna(subset=['timestamp']).iloc[-1]
        bid = current_market['bid'].item()
        ask = current_market['ask'].item()
        if signal.decision == DECISIONS.BUY_ALL and self.current_status == DECISIONS.NO_DATA:
            # perform trade
            self.current_trade = Trade(
                open=datetime.now(),
                close=None,
                volume=self.current_equity / ask,
                profit=None,
                open_price=ask,
                close_price=None
            )
            self.current_status = DECISIONS.BUY_ALL
        if signal.decision == DECISIONS.SELL_ALL and self.current_status == DECISIONS.BUY_ALL:
            profit = self.current_trade.volume * (bid - 1.005 * self.current_trade.open_price)
            closed_trade = Trade(
                open=self.current_trade.open,
                close=datetime.now(),
                volume=self.current_trade.volume,
                profit=profit,
                open_price=self.current_trade.open_price,
                close_price=bid
            )
            self.current_equity += profit
            self.current_status = DECISIONS.NO_DATA
            self.trade_history.append(closed_trade)

    def buy_all(self, minimum=20.0, maximum=50000.0):
        # 1. Figure out how much we can try to buy
        status = self._trade_api.status()
        if status is None:
            return False
        account = next(x for x in status['accounts'] if x['currency'] == 'uah')
        amount_available = floor(float(account['balance']))
        if amount_available < minimum:
            return False
        if amount_available > maximum:
            amount_available = maximum
        current_rate = float(min([x['price'] for x in self._trade_api.latest_orderbook()['asks']]))
        amount_in_btc = floor((amount_available / current_rate) * 1000000) / 1000000
        order = self._trade_api.order('buy', current_rate, amount_in_btc)
        return order

    def sell_all(self, minimum=0.000002, maximum=1):
        # 1. Figure out how much we can try to buy
        status = self._trade_api.status()
        if status is None:
            return False
        account = next(x for x in status['accounts'] if x['currency'] == 'btc')
        amount_available = floor(float(account['balance']) * 1000000) / 1000000
        if amount_available < minimum:
            return False
        if amount_available > maximum:
            amount_available = maximum
        current_rate = float(max([x['price'] for x in self._trade_api.latest_orderbook()['bids']]))
        amount_in_btc = amount_available
        order = self._trade_api.order('sell', current_rate, amount_in_btc)
        return order

    def cancel_all(self):
        # 1. get orders.
        # 2. cancel each order.
        orders = self._trade_api.orders()
        for order in orders:
            res = self._trade_api.delete(order['id'])
        return self._trade_api.status()
