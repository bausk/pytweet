from models.exchange import BaseExchangeInterface


class KunaExchange(BaseExchangeInterface):
    urls = dict(
        base_url="https://kuna.io",
        userinfo_url="/api/v2/members/me",
        orders_url="/api/v2/orders",
        trade_url="/api/v2/orders",
        delete_url="/api/v2/order/delete",
        orderbook_url="/api/v2/order_book",
    )

