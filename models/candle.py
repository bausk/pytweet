from datetime import datetime


class Candle:
    def __init__(self, data):
        self.timestamp = int(data[0] / 1000)
        self.datetime = datetime.fromtimestamp(self.timestamp)
        self.o = data[1]
        self.c = data[2]
        self.h = data[3]
        self.l = data[4]
        self.v = data[5]
