from math import floor
from datetime import datetime, timedelta
import pandas as pd

from models.algorithm import BaseAlgorithm
from models.simulator import BaseSimulator
from models.exchange import BaseExchangeInterface
from models.trade import Trade
from models.signal import Signal
from persistence.simple_store import InMemoryStore
from persistence.mixins import PrepareDataMixin, WithConsole
from parsers.rates import orderbook_to_series
from constants.formats import orderbook_format, signal_format
from constants.constants import DECISIONS
from logger.hdf_logger import hdf_log


class LiveTrader(WithConsole, PrepareDataMixin, InMemoryStore):
    current_status = DECISIONS.NO_DATA
    current_trade = None
    current_equity = 250

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trade_history = []
        self.signal_history = []
        self.algorithm: BaseAlgorithm = None
        self.simulator: BaseSimulator = None
        self._trade_api = None
        self._source_api = None
        self._target_api = None
        self._source_df = None
        self._orderbook = None
        self._target_trades = None
        self._cutoff = timedelta(hours=8)
        self._emulate_signals = False

    def _get_cutoff(self):
        if self.algorithm is not None and getattr(self.algorithm, 'cutaway', None):
            return self.algorithm.cutaway
        else:
            return self._cutoff

    def get_rate(self):
        return self.algorithm.rate.seconds

    def add_trader_api(self, api):
        self._trade_api: BaseExchangeInterface = api

    def add_source_api(self, api):
        self._source_api = api

    def add_target_api(self, api):
        self._target_api = api

    def add_algorithm(self, algorithm):
        self.algorithm = algorithm

    def add_simulator(self, simulator):
        self.simulator = simulator

    def emulate_signals(self, status=False):
        self._emulate_signals = status

    def simulate(self, preprocessor=None, **kwargs):
        self.signal_history = []
        for name, filename in kwargs.items():
            df = pd.DataFrame.from_csv(filename)
            self.simulator.add_dataframe(df, name)
        if preprocessor is None:
            preprocessor = self.simulator.get_preprocessor()
        for x in self.simulator:
            signal_history = self.signal_callback(data_dict=x, preprocessor=preprocessor)
            for k, df in x.items():
                self.log("{}: {}\n".format(k, len(df)))
        print(self.signal_history)

    def _get_api_data(self):
        raw_source_data = self._source_api.fetch_latest_trades(limit=10)
        source_df = self._prepare_data(raw_source_data)
        raw_orderbook = orderbook_to_series(self._target_api.fetch_order_book())
        orderbook = self._prepare_data(
            raw_orderbook,
            {'time_field': 'timestamp', 'columns': orderbook_format, 'time_unit': 's'}
        )
        return {
            'original_source': source_df,
            'original_orderbook': orderbook
        }


    @hdf_log('signal.csv', 'signal_history')
    def signal_callback(self, data_dict=None, preprocessor=None):
        current_time = datetime.utcnow()
        cutoff_delta = self._get_cutoff().seconds
        cutoff_timestamp = int(current_time.timestamp()) - cutoff_delta
        source = "simulator"
        if data_dict is None:
            # Work on default dataframes from _get_api_data
            data_dict = self._get_api_data()
            source = "live"

        if self._emulate_signals:
            data_dict['original_source'], data_dict['original_orderbook'] = \
                self.algorithm.emulate(data_dict['original_source'], data_dict['original_orderbook'])

        if source == "live":
            data_dict['original_source'] = pd.concat(
                [self._source_df, data_dict['original_source']]
            ).drop_duplicates(keep='last', subset='timestamp')
            data_dict['original_source'].drop(
                data_dict['original_source'].index[data_dict['original_source']['timestamp'] < cutoff_timestamp],
                inplace=True
            )

            data_dict['original_orderbook'] = pd.concat(
                [self._orderbook, data_dict['original_orderbook']]
            ).drop_duplicates(keep='last', subset='timestamp')
            data_dict['original_orderbook'].drop(
                data_dict['original_orderbook'].index[data_dict['original_orderbook']['timestamp'] < cutoff_timestamp],
                inplace=True
            )

        true_source = None
        true_orderbook = None
        if 'normalized_source' in data_dict:
            true_source = data_dict['normalized_source']
        else:
            true_source = data_dict['original_source']
        if 'normalized_source' in data_dict:
            true_orderbook = data_dict['normalized_orderbook']
        else:
            true_orderbook = data_dict['original_orderbook']

        # Apply algorithm from analyzer
        signal_object: Signal = self.algorithm.signal(true_source, true_orderbook, preprocessor)
        self.log("[{}] {}/{} from {}/{} measurements".format(
            current_time,
            signal_object.buy,
            signal_object.sell,
            len(true_orderbook),
            len(true_source)
        ))
        result = self._execute_signal(signal_object, orderbook=true_orderbook)
        historical_signal = signal_object._asdict()
        historical_signal['result'] = result
        historical_signal['logged_time'] = current_time
        self.signal_history.append(historical_signal)

        if source == "live":
            self._source_df = data_dict['original_source']
            self._orderbook = data_dict['original_orderbook']
        return self.signal_history

    def _execute_signal(self, signal: Signal, orderbook=None):
        """
        State machine to move around the trader current deal from inactive to active and back
        :param signal:
        :return:
        """
        if orderbook is None:
            orderbook = self._orderbook
        if orderbook is None or len(orderbook.dropna(subset=['timestamp'])) == 0:
            return DECISIONS.NO_DATA
        current_market = orderbook.dropna(subset=['timestamp']).iloc[-1]
        bid = current_market['bid'].item()
        ask = current_market['ask'].item()
        if signal.decision == DECISIONS.BUY_ALL and self.current_status == DECISIONS.NO_DATA:
            # perform trade
            self.current_trade = Trade(
                open=datetime.utcnow(),
                close=None,
                volume=self.current_equity / ask,
                profit=None,
                open_price=ask,
                close_price=None
            )
            self.current_status = DECISIONS.BUY_ALL
            return DECISIONS.BUY_ALL
        if signal.decision == DECISIONS.SELL_ALL and self.current_status == DECISIONS.BUY_ALL:
            profit = self.current_trade.volume * (bid - 1.005 * self.current_trade.open_price)
            closed_trade = Trade(
                open=self.current_trade.open,
                close=datetime.utcnow(),
                volume=self.current_trade.volume,
                profit=profit,
                open_price=self.current_trade.open_price,
                close_price=bid
            )
            self.current_equity += profit
            self.current_status = DECISIONS.NO_DATA
            self.trade_history.append(closed_trade)
            return DECISIONS.SELL_ALL
        return DECISIONS.NO_DATA

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

    def _to_dataframe(self, data, index_field=None):
        rv = pd.DataFrame.from_records(data)
        if index_field is not None:
            rv.set_index(index_field, inplace=True)
            rv.sort_index(inplace=True)
