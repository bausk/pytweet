import hmac
import hashlib
import requests
from collections import OrderedDict
from math import floor

from persistence.postgre import BaseSQLStore
from constants.formats import trading_records
from urllib.parse import urlencode
from datetime import datetime

class PandasTrader(BaseSQLStore):

    def __init__(self, name):
        super().__init__(name, format=trading_records)

    def add_trader_api(self, api):
        self._api: KunaIOTrader = api

    def buy_all(self, minimum=20.0, maximum=50000.0):
        conn = self._ensure_connection(ensure_table=True)
        # 1. Figure out how much we can try to buy
        status = self._api.status()
        if status is None:
            return False
        account = next(x for x in status['accounts'] if x['currency'] == 'uah')
        amount_available = floor(float(account['balance']))
        if amount_available < minimum:
            return False
        if amount_available > maximum:
            amount_available = maximum
        current_rate = float(min([x['price'] for x in self._api.latest_orderbook()['asks']]))
        amount_in_btc = floor((amount_available / current_rate) * 1000000) / 1000000
        order = self._api.order('buy', current_rate, amount_in_btc)
        return order

    def sell_all(self, minimum=0.000002, maximum=1):
        conn = self._ensure_connection(ensure_table=True)
        # 1. Figure out how much we can try to buy
        status = self._api.status()
        if status is None:
            return False
        account = next(x for x in status['accounts'] if x['currency'] == 'btc')
        amount_available = floor(float(account['balance']) * 1000000) / 1000000
        if amount_available < minimum:
            return False
        if amount_available > maximum:
            amount_available = maximum
        current_rate = float(max([x['price'] for x in self._api.latest_orderbook()['bids']]))
        amount_in_btc = amount_available
        order = self._api.order('sell', current_rate, amount_in_btc)
        return order

    def cancel_all(self):
        conn = self._ensure_connection(ensure_table=True)
        # 1. get orders.
        # 2. cancel each order.
        orders = self._api.orders()
        for order in orders:
            res = self._api.delete(order['id'])
        return self._api.status()


class KunaIOTrader(object):
    def __init__(self, public, secret):
        self._public_key_input = public
        self._secret_key_input = secret
        self._base_uri = "https://kuna.io"
        self._urls = dict(
            base_url="https://kuna.io",
            userinfo_url="/api/v2/members/me",
            orders_url="/api/v2/orders",
            trade_url="/api/v2/orders",
            delete_url="/api/v2/order/delete",
            orderbook_url="/api/v2/order_book",
        )

    def status(self):
        res = self._get(self._urls['userinfo_url'])
        return self._check_error(res.json(), None)

    def orders(self):
        params = dict(market='btcuah')
        res = self._get(self._urls['orders_url'], params)
        return self._check_error(res.json(), [])

    def delete(self, ident):
        params = dict(id=ident)
        res = self._post(self._urls['delete_url'], params)
        return self._check_error(res.json(), None)

    def order(self, side, price, volume):
        params = dict(
            market='btcuah',
            side=side,
            price=price,
            volume=volume
        )
        res = self._post(self._urls['trade_url'], params)
        return self._check_error(res.json(), None)

    def latest_orderbook(self):
        params = dict(market='btcuah')
        return requests.get(self._urls['base_url'] + self._urls['orderbook_url'], params=params).json()

    def _get_params(self):
        return dict(
            access_key = str(self._public_key_input.value),
            tonce=self._tonce()
        )

    def _sign(self, verb, uri, params=""):
        message = "{verb}|{uri}|{params}".format(verb=verb, uri=uri, params=params)
        key = self._secret_key_input.value
        h = hmac.new(key.encode(), message.encode(), hashlib.sha256)
        return h.hexdigest()

    def _post(self, uri, params=None):
        if params is None:
            params = {}
        params.update(self._get_params())
        params = OrderedDict(sorted(params.items()))
        enc_params = urlencode(params)
        signature = self._sign("POST", uri, enc_params)
        prepared_params = dict(**params, signature=signature)
        return requests.post(self._urls['base_url'] + uri, params=prepared_params)

    def _get(self, uri, params=None):
        if params is None:
            params = {}
        params.update(self._get_params())
        params = OrderedDict(sorted(params.items()))
        enc_params = urlencode(params)
        signature = self._sign("GET", uri, enc_params)
        prepared_params = dict(**params, signature=signature)
        return requests.get(self._urls['base_url'] + uri, params=prepared_params)

    def _check_error(self, response, default_value=None):
        if 'error' in response:
            return default_value
        return response

    def _tonce(self):
        return int(round(datetime.now().timestamp() * 1000))