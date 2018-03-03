from bokeh.models import BoxAnnotation
from bokeh.plotting import Figure
from datetime import datetime, timedelta
from dateutil import parser
from typing import Tuple, Union, Any

from ..objects.dataframes import create_empty_dataframe
from ..constants.constants import INDICATOR_NAMES as NAMES


def get_time(time_string: str) -> Tuple[Union[datetime, Any], float]:
    """
    Returns datetime converted from time_string argument.
    :param time_string: a string representing either a float (treated as number of hours before current moment)
    or a date (in a format parseable by dateutil).
    :return: datetime
    """
    now = datetime.utcnow()
    try:
        hour = float(time_string)
        date = now - timedelta(hours=hour)
    except:
        date = parser.parse(time_string)
    diff = (now - date).seconds / 3600
    return date, diff


class BaseArbitrageAnalyzer:

    def __init__(self, input_object: dict, output_object: Figure, start=None, end=None, console=None) -> None:
        if console is None:
            console = dict(text="")
        self._input = input_object
        self._output = output_object
        self._annotations = []
        self._start = start
        self._end = end
        self._console = console

    def analyze(self):
        """
        Basic arbitrage analyzer based on self._input Pandas datastores
        :return: Dataframe consistent with self._input dates, in the form of [index:'Time', 'indicator']
        """
        result = create_empty_dataframe(['created_at', 'indicator'])
        print("[info] Performing analysis with Simple analyzer...")
        self._console.text += "[info] Starting up analysis\n"
        start_date, start_diff = get_time(self._start.value)
        end_date, end_diff = get_time(self._end.value)

        src_df, ord_df = self._ensure_data(start_diff, end_diff)
        self._console.text += "[info] Loaded data\n"

        for renderer in self._annotations:
            if renderer in self._output.renderers:
                self._output.renderers[self._output.renderers.index(renderer)].left = 0
                self._output.renderers[self._output.renderers.index(renderer)].right = 0
                renderer.left = 0
                renderer.right = 0
        annotation = BoxAnnotation(left=datetime.utcnow() - timedelta(hours=2), right=datetime.utcnow())
        self._output.add_layout(annotation)
        self._annotations.append(annotation)
        return result

    def _ensure_data(self, start_diff, end_diff):
        src_df = self._input['src_store'].read_latest(start=start_diff, end=end_diff)
        ord_df = self._input['ord_store'].read_latest(start=start_diff, end=end_diff)
        return src_df, ord_df

    def _line(self, data):
        line = self._output.select_one(NAMES.SIMPLE)
        if line is None:
            pass
        else:
            return line
