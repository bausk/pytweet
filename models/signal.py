from collections import namedtuple

Signal = namedtuple('Signal', [
    'buy',
    'sell',
    'buy_datetime',
    'sell_datetime',
    'decision'
])
