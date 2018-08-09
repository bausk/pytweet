from datetime import timedelta

class BaseAlgorithm:
    rate: timedelta = timedelta(milliseconds=1000)
    cutaway: timedelta = timedelta(hours=1)

    def signal(self, *args, **kwargs):
        raise NotImplementedError('Implement self.signal if inheriting from BaseAlgorithm.')
