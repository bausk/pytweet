import hmac
import hashlib
import requests
from collections import OrderedDict

from persistence.postgre import BaseSQLStore
from constants.formats import trading_records
from urllib.parse import urlencode
from datetime import datetime

class PandasTrader(BaseSQLStore):

    def __init__(self, name):
        super().__init__(name, format=trading_records)

    def add_trader_api(self, api):
        self._api: KunaIOTrader = api

    def buy_all(self, minimum=10.0, maximum=50000.0):
        conn = self._ensure_connection(ensure_table=True)
        return self._api.status()

    def sell_all(self, minimum=0.000001, maximum=0.2):
        conn = self._ensure_connection(ensure_table=True)
        return self._api.status()

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
            delete_url="/api/v2/order/delete"
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
        return res.json()

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