from datetime import datetime, timedelta
from dateutil import parser, tz
from typing import Tuple, Union, Any


def user_input_to_utc_time(time_string: str) -> Tuple[Union[datetime, Any], float]:
    """
    Returns datetime converted from time_string argument.
    :param time_string: a string representing either a float (treated as number of hours before current moment)
    or a date (in a format parseable by dateutil).
    :return: datetime
    """
    now = datetime.utcnow().replace(tzinfo=tz.tzutc())
    try:
        hour = float(time_string)
        date = now - timedelta(hours=hour)
    except:
        try:
            date = parser.parse(time_string).replace(tzinfo=tz.tzutc())
        except:
            return None, 0.0
    diff = (now - date).days * 24.0 + (now - date).seconds / 3600
    return date, diff
