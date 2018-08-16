from datetime import timedelta

class BaseAlgorithm:
    rate: timedelta = timedelta(milliseconds=1000)
    cutaway: timedelta = timedelta(hours=1)

    def emulate(self, source_df, orderbook_series):
        raise NotImplementedError('Implement self.emulate()')

    def signal(self, *args, **kwargs):
        raise NotImplementedError('Implement self.signal()')
