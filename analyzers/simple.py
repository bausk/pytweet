from bokeh.models import BoxAnnotation, ColumnDataSource
from bokeh.plotting import Figure
from bokeh.palettes import Spectral10, Category10
from datetime import datetime, timedelta
from dateutil import parser
from typing import Tuple, Union, Any


from objects.dataframes import create_empty_dataframe
from constants.constants import INDICATOR_NAMES as NAMES


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
    diff = (now - date).days * 24.0 + (now - date).seconds / 3600
    return date, diff


class BaseArbitrageAnalyzer:

    def __init__(self, input_object: dict, output_object: Figure, start=None, end=None, console=None) -> None:
        if console is None:
            console = dict(text="")
        self._input = input_object
        self._plot = output_object
        self._annotations = []
        self._start = start
        self._end = end
        self._console = console
        self._min_ask_bid_ratio = 0.0035

    def analyze(self):
        """
        Basic arbitrage analyzer based on self._input Pandas datastores
        :return: Dataframe consistent with self._input dates, in the form of [index:'Time', 'indicator']
        """
        result = create_empty_dataframe(['created_at', 'indicator'])
        # result.loc[datetime.utcnow()] = [0, 1.0]
        print("[info] Performing analysis with Simple analyzer...")
        self._console.text += "[info] Starting up analysis\n"
        start_date, start_diff = get_time(self._start.value)
        end_date, end_diff = get_time(self._end.value)
        src_df, ord_df = self._ensure_data(start_diff, end_diff)
        self._console.text += "[info] Loaded data\n"
        src_df = src_df.truncate(before=start_date, after=end_date).resample('1T').mean().interpolate()
        ord_df = ord_df.truncate(before=start_date, after=end_date).resample('1T').mean().interpolate()
        coeffs = ((ord_df['ask'] + ord_df['bid']) / 2).rolling('2h').mean().shift(10) / src_df['price'].rolling('2h').mean().shift(10)
        normalized_ask_bid_distance = ord_df['ask'] - ord_df['bid']
        minimum_ask_bid_distance = ord_df['bid'] * self._min_ask_bid_ratio
        self._console.text += "[info] Calculated index\n"
        normalized_ask_bid_distance.loc[normalized_ask_bid_distance < minimum_ask_bid_distance] = minimum_ask_bid_distance
        arbitrage_difference = ((src_df['price'] * coeffs) - ord_df['ask'])
        # Get indicator
        arbitrage_indicator = arbitrage_difference / normalized_ask_bid_distance
        # filter indicator for values more than 1.5
        arbitrage_indicator.loc[(arbitrage_indicator < 1.5) & (arbitrage_indicator > -0.8)] = 0
        weighted_arbitrage_indicator = arbitrage_indicator.rolling('180s').sum()
        self._console.text += "[info] Calculated indicators\n"
        line = self._line(NAMES.SIMPLE, arbitrage_indicator, color=Category10[10][3])
        line2 = self._line(NAMES.WEIGTHED, weighted_arbitrage_indicator, color=Category10[10][4])

        for renderer in self._annotations:
            if renderer in self._plot.renderers:
                self._plot.renderers[self._plot.renderers.index(renderer)].left = 0
                self._plot.renderers[self._plot.renderers.index(renderer)].right = 0
                renderer.left = 0
                renderer.right = 0
                renderer.visible = False
        annotation = BoxAnnotation(left=datetime.utcnow() - timedelta(hours=2), right=datetime.utcnow())
        self._plot.add_layout(annotation)
        self._annotations.append(annotation)
        return arbitrage_indicator, src_df, ord_df

    def _ensure_data(self, start_diff, end_diff):
        src_df = self._input['src_store'].read_latest(start=start_diff, end=end_diff)
        ord_df = self._input['ord_store'].read_latest(start=start_diff, end=end_diff)
        return src_df, ord_df

    def _line(self, name, series=None, color=Spectral10[8]):
        # For now, do not delete existing data on the line
        line = self._plot.select_one(name)
        if line is None:
            line_cds = ColumnDataSource(data=dict(Time=series.index, indicator=series.values))
            line = self._plot.line('Time', 'indicator', source=line_cds, line_width=2, color=color, alpha=0.9, legend="Indicator " + name,
                                   y_range_name="UNITLESS", line_join='round', name=name)
        else:
            line_df = line.data_source.to_df()
            line_df['indicator'].update(series)
            new_df = line_df
            line.data_source.data = (dict(Time=new_df.index, indicator=new_df.indicator))
        return line
