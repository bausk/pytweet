from bokeh.layouts import row, widgetbox
from bokeh.models import ColumnDataSource, Range1d, LinearAxis, DatetimeTicker, DatetimeTickFormatter
from bokeh.models.widgets import TextInput, Button
from bokeh.plotting import figure
from bokeh.palettes import Spectral4
import math

from parsers.rates import get_rate
from persistence.pandas import PandasReader
from constants import formats

DEFAULT_COEFF = 27.0
USD_LOW = 10000
USD_HIGH = 12000


def init_plot(y_range=(USD_LOW, USD_HIGH)):
    tools = "pan,wheel_zoom,xwheel_zoom,box_zoom,reset,save"
    p = figure(x_axis_type="datetime", tools=tools, plot_width=1000, title="Arbitrage Controller", y_range=y_range)
    p.ygrid.minor_grid_line_alpha = 0.2
    p.ygrid.minor_grid_line_color = 'gray'
    p.ygrid.grid_line_alpha = 0.4
    p.xgrid.minor_grid_line_alpha = 0.2
    p.xgrid.grid_line_alpha = 0.5
    p.xaxis.ticker = DatetimeTicker(desired_num_ticks=18)
    p.extra_y_ranges = {
        "BTCUAH": Range1d(start=y_range[0] * DEFAULT_COEFF, end=y_range[1] * DEFAULT_COEFF),
    }
    p.add_layout(LinearAxis(y_range_name="BTCUAH"), 'left')
    return p



def serve_frontend(doc):
    src_store = PandasReader(name="bitfinex_btcusd", columns=formats.history_format, time_unit="s", time_field="timestamp")
    tgt_store = PandasReader(name="kuna_btcuah", columns=formats.history_format, time_unit="s", x_shift_hours=0)
    ord_store = PandasReader(name="kuna_orderbook", columns=formats.orderbook_format, time_unit="s")

    src_df = src_store.read_latest()
    tgt_df = tgt_store.read_latest()
    ord_df = ord_store.read_latest()
    source_src = ColumnDataSource(data=dict(Time=src_df.index, price=src_df.price))
    source_tgt = ColumnDataSource(data=dict(Time=tgt_df.index, price=tgt_df.price, volume=tgt_df['volume'].multiply(100)))
    source_ord = ColumnDataSource(data=dict(Time=ord_df.index, ask=ord_df.ask, bid=ord_df.bid))

    plot = init_plot()

    plot.line('Time', 'price', source=source_src, line_width=3, color=Spectral4[0], alpha=0.8, legend="SRC", line_join='round')
    plot.line('Time', 'price', source=source_tgt, line_width=3, color=Spectral4[1], alpha=0.8, legend="TGT", y_range_name="BTCUAH", line_join='round')
    plot.circle('Time', 'price', source=source_tgt,  size='volume', legend="TGT", alpha=0.6, line_width=1, y_range_name="BTCUAH")
    plot.line('Time', 'bid', source=source_ord, line_width=1, color=Spectral4[2], alpha=1, legend="Bid", y_range_name="BTCUAH", line_join='round')
    plot.line('Time', 'ask', source=source_ord, line_width=1, color=Spectral4[3], alpha=1, legend="Ask", y_range_name="BTCUAH", line_join='round')

    wgt_refresh = Button(label='Refresh', button_type='success')
    def on_refresh(button):
        nonlocal src_df
        nonlocal tgt_df
        nonlocal ord_df
        try:
            src_df = src_store.read_latest()
            tgt_df =  tgt_store.read_latest()
            ord_df = ord_store.read_latest()
            source_src.data = dict(Time=src_df.index, price=src_df.price)
            source_tgt.data = dict(Time=tgt_df.index, price=tgt_df.price, volume=tgt_df['volume'].multiply(100))
            source_ord.data = dict(Time=ord_df.index, ask=ord_df.ask, bid=ord_df.bid)
        except BaseException as e:
            print(e)
    wgt_refresh.on_click(lambda: on_refresh(wgt_refresh))

    wgt_text = TextInput(title="Manual Rate:", value=str(DEFAULT_COEFF))
    def on_coeff_change(attrname, old, new):
        print(attrname, old, new)
        plot.extra_y_ranges['BTCUAH'].start = plot.y_range.start * float(new)
        plot.extra_y_ranges['BTCUAH'].end = plot.y_range.end * float(new)
    wgt_text.on_change('value', on_coeff_change)

    wgt_autoscale = Button(label='Autoscale')
    def on_autoscale(button):
        nonlocal src_df
        nonlocal ord_df
        rate = get_rate(src_df, ord_df)
        if math.isnan(rate):
            pass
        else:
            wgt_text.value = "{0:.3f}".format(1 / rate)
    wgt_autoscale.on_click(lambda: on_autoscale(on_autoscale))

    inputs = widgetbox(wgt_refresh, wgt_text, wgt_autoscale)
    doc.add_root(row(inputs, plot, width=1500))
    doc.title = "Arbitrage Controller"
