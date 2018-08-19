from bokeh.models import ColumnDataSource, Range1d, LinearAxis, DatetimeTicker, CustomJS
from bokeh.plotting import figure
from bokeh.palettes import Spectral10, Spectral4
from constants.constants import MONITOR_CHART_NAMES as GLYPHNAMES

DEFAULT_COEFF = 27.0
USD_LOW = 10000
USD_HIGH = 12000


class ArbitragePlotter:

    def __init__(self):
        self._plot = self.init_plot()
        self._source_src = ColumnDataSource(data=dict(Time=[], price=[]))
        self._source_ord = ColumnDataSource(data=dict(Time=[], ask=[], bid=[]))
        self._plot.line('Time', 'price', source=self._source_src, line_width=3, color=Spectral4[0], alpha=0.8,
                        legend="SRC",
                        line_join='round', name=GLYPHNAMES.SOURCE)
        self._plot.line('Time', 'bid', source=self._source_ord, line_width=1, color=Spectral4[2], alpha=1, legend="Bid",
                        y_range_name="BTCUAH", line_join='round', name=GLYPHNAMES.TGT_BID)
        self._plot.line('Time', 'ask', source=self._source_ord, line_width=1, color=Spectral4[3], alpha=1, legend="Ask",
                        y_range_name="BTCUAH", line_join='round', name=GLYPHNAMES.TGT_ASK)

    def init_plot(self, y_range=(USD_LOW, USD_HIGH)):
        tools = "pan,wheel_zoom,xwheel_zoom,box_zoom,reset,save"
        p = figure(x_axis_type="datetime", tools=tools, plot_width=1200, title="Arbitrage Controller", y_range=y_range)
        p.ygrid.minor_grid_line_alpha = 0.2
        p.ygrid.minor_grid_line_color = 'gray'
        p.ygrid.grid_line_alpha = 0.4
        p.xgrid.minor_grid_line_alpha = 0.2
        p.xgrid.grid_line_alpha = 0.5
        p.xaxis.ticker = DatetimeTicker(desired_num_ticks=18)
        p.extra_y_ranges = {
            "BTCUAH": Range1d(start=y_range[0] * DEFAULT_COEFF, end=y_range[1] * DEFAULT_COEFF),
            "UNITLESS": Range1d(start=-15.0, end=25.0),
        }
        p.add_layout(LinearAxis(y_range_name="BTCUAH"), 'left')
        p.add_layout(LinearAxis(y_range_name="UNITLESS"), 'right')
        self._plot = p
        return p

    def refresh(self, **dataframes):
        src_df = [value for key, value in dataframes.items() if 'source' in key.lower()][0]
        ord_df = [value for key, value in dataframes.items() if 'orderbook' in key.lower()][0]
        self._source_src.data = dict(Time=src_df.index, price=src_df.price)
        self._source_ord.data = dict(Time=ord_df.index, ask=ord_df.ask, bid=ord_df.bid)

    def get_plot(self):
        return self._plot

    def refresh_indicator(self, name, indicator, col=None):
        self._line(name, indicator, col=col, color=Spectral10[9])

    def _line(self, name, series=None, col=None, color=Spectral10[8]):
        # For now, do not delete existing data on the line
        if col is None:
            col = 'indicator'
        line = self._plot.select_one(name)
        if line is None:
            line_cds = ColumnDataSource(data=dict(Time=series.index, indicator=series[col]))
            line = self._plot.line('Time', 'indicator', source=line_cds, line_width=2, color=color, alpha=0.9,
                                   legend="Indicator " + name,
                                   y_range_name="UNITLESS", line_join='round', name=name)
        else:
            line.data_source.data = (dict(Time=series.index, indicator=series[col]))
        return line
