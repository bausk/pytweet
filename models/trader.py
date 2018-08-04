from math import floor

from constants.formats import trading_records
from models.exchange import BaseExchangeInterface
from persistence.postgre import BaseSQLStore
from parsers.rates import orderbook_to_series


class LiveTrader(BaseSQLStore):

    def __init__(self, name):
        super().__init__(name, format=trading_records)
        self._trade_api = None
        self._source_api = None
        self._target_api = None
        self._source_store = None

    def add_trader_api(self, api):
        self._trade_api: BaseExchangeInterface = api

    def add_source_api(self, api):
        self._source_api = api

    def add_target_api(self, api):
        self._target_api = api

    def signal_callback(self, *args, **kwargs):
        data1 = self._source_api.fetch_latest_trades(limit=100)
        data2 = self._target_api.fetch_latest_trades()
        # Now convert them to dataframes and apply algorithm from analyzer
        src_df = src_store.read_latest(trunks=2)
        ord_store.read_latest(trunks=2)
        source_src.data = dict(Time=src_df.index, price=src_df.price)
        console.text = "[{}] Performing live trading. Latest data:\n".format(datetime.now())
        console.text += "[{}] {}\n".format(datetime.now(), src_store._df.iloc[-1])
        # Task for tomorrow:
        # Figure out how simulator works. Implement same logic here in live trade or in another file.
        # Task: make dataframe retrieval fully DB-independent

    def check_status(self):
        # Need 2 dataframes here, same as in analyzer
        self._source_store.write(self._source_api.fetch_latest_trades(limit=50))
        orderbook = orderbook_to_series(self._target_api.fetch_order_book())
        if orderbook.get('timestamp') is not None:
            self._target_store.write(orderbook)

    def buy_all(self, minimum=20.0, maximum=50000.0):
        conn = self._ensure_connection(ensure_table=True)
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
        conn = self._ensure_connection(ensure_table=True)
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
        conn = self._ensure_connection(ensure_table=True)
        # 1. get orders.
        # 2. cancel each order.
        orders = self._trade_api.orders()
        for order in orders:
            res = self._trade_api.delete(order['id'])
        return self._trade_api.status()
