from datetime import timedelta

class BaseAlgorithm:
    rate: timedelta = timedelta(milliseconds=1000)
    cutaway: timedelta = timedelta(hours=1)

    @staticmethod
    def signal(*args, **kwargs):
        return True
