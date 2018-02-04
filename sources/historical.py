from types import SimpleNamespace
from string import Template
from datetime import timedelta
from dateutil import parser
import requests

from constants import currencies, periods


class CurrencySource(SimpleNamespace):
    source = Template("")
    name = ""
    period = ""

    def _get_after_time(self, aftertime):
        return int(parser.parse(aftertime, ignoretz=True).timestamp())

    def _get_before_time(self, beforetime):
        return int((parser.parse(beforetime, ignoretz=True) + timedelta(days=1)).timestamp())

    def fetch_data(self, after="2000-01-01", before="2001-01-01", **kwargs):
        after = self._get_after_time(after)
        before = self._get_before_time(before)
        endpoint = self.source.substitute(
            currency=self.currencies[self.currency],
            period=self.periods[self.period],
            after=after,
            before=before,
            **kwargs)
        result = requests.get(endpoint)
        return result.json()


class CryptowatchSource(CurrencySource):
    source = Template(
        'https://api.cryptowat.ch/markets/gdax/$currency/ohlc?periods=$period&before=$before&after=$after'
    )

    currencies = {
        currencies.BTC: "btc",
        currencies.ETH: "eth"
    }

    periods = {
        periods.P5M: 5 * 60,
        periods.P15M: 15 * 60,
        periods.P30M: 30 * 60
    }


class BitfinexSource(CurrencySource):
    source = Template(
        'https://api.bitfinex.com/v2/candles/trade:$period:t$currency/hist?start=$after&end=$before'
    )

    currencies = {
        currencies.BTC: "BTCUSD",
        currencies.ETH: "ETHUSD"
    }

    periods = {
        periods.P5M: "5m",
        periods.P15M: "15m",
        periods.P30M: "30m"
    }

    def _get_after_time(self, aftertime):
        return int(parser.parse(aftertime, ignoretz=True).timestamp()) * 1000

    def _get_before_time(self, beforetime):
        return int((parser.parse(beforetime, ignoretz=True) + timedelta(days=1)).timestamp()) * 1000

