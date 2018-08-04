import hashlib
import hmac
from collections import OrderedDict
from datetime import datetime
from urllib.parse import urlencode

import requests


class BaseExchangeInterface(object):
    urls = dict()

    def __init__(self, public, secret):
        self._public_key_input = public
        self._secret_key_input = secret
        self._check_consisntence()

    def _check_consisntence(self):
        if self.urls.get('base_url') is None:
            raise TypeError('self.urls.base_url absent from Exchange interface initialization')

    def status(self):
        res = self._get(self.urls['userinfo_url'])
        return self._check_error(res.json(), None)

    def orders(self):
        params = dict(market='btcuah')
        res = self._get(self.urls['orders_url'], params)
        return self._check_error(res.json(), [])

    def delete(self, ident):
        params = dict(id=ident)
        res = self._post(self.urls['delete_url'], params)
        return self._check_error(res.json(), None)

    def order(self, side, price, volume):
        params = dict(
            market='btcuah',
            side=side,
            price=price,
            volume=volume
        )
        res = self._post(self.urls['trade_url'], params)
        return self._check_error(res.json(), None)

    def latest_orderbook(self):
        params = dict(market='btcuah')
        return requests.get(self.urls['base_url'] + self.urls['orderbook_url'], params=params).json()

    def _get_params(self):
        return dict(
            access_key=str(self._public_key_input.value),
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
        return requests.post(self.urls['base_url'] + uri, params=prepared_params)

    def _get(self, uri, params=None):
        if params is None:
            params = {}
        params.update(self._get_params())
        params = OrderedDict(sorted(params.items()))
        enc_params = urlencode(params)
        signature = self._sign("GET", uri, enc_params)
        prepared_params = dict(**params, signature=signature)
        return requests.get(self.urls['base_url'] + uri, params=prepared_params)

    def _check_error(self, response, default_value=None):
        if 'error' in response:
            return default_value
        return response

    def _tonce(self):
        return int(round(datetime.now().timestamp() * 1000))
