from datetime import datetime, timedelta
from dateutil import tz

class VisualLogger:
    def __init__(self):
        self._last_checked_time = datetime.utcnow().replace(tzinfo=tz.tzutc())

    def timeout(self):
        current_time = datetime.utcnow().replace(tzinfo=tz.tzutc())
        if self._last_checked_time + timedelta(minutes=10) < current_time:
            self._last_checked_time = current_time
            return True
        return False
