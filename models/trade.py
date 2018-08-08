from collections import namedtuple

Trade = namedtuple('Trade', [
    'open',
    'close',
    'volume',
    'profit',
    'open_price',
    'close_price'
])