
history_format = ['timestamp', 'id', 'created_at', 'price', 'volume']
orderbook_format = ['timestamp', 'bid', 'ask', 'bid_volume', 'bid_weight', 'ask_volume', 'ask_weight']

datastore_records = dict(
    created_at="timestamp with time zone NOT NULL",
    collected_at="timestamp with time zone NOT NULL",
    data="jsonb",
    metadata="jsonb",
    id="bigserial NOT NULL",
)

trading_records = dict(
    id="bigserial NOT NULL",
    created_at="timestamp with time zone",
    price="real NOT NULL",
    volume="double precision NOT NULL"
)
