from bokeh.layouts import row, widgetbox
from bokeh.models import ColumnDataSource, Range1d, LinearAxis
from bokeh.models.widgets import TextInput, Button
from bokeh.plotting import figure
from bokeh.palettes import Spectral4

from persistence.postgre import TimeSeriesStore
from constants import formats

DEFAULT_COEFF = 27.0
USD_LOW = 10000
USD_HIGH = 12000


def init_plot(y_range=(USD_LOW, USD_HIGH)):
    tools = "pan,wheel_zoom,xwheel_zoom,box_zoom,reset,save"
    p = figure(x_axis_type="datetime", tools=tools, plot_width=1000, title="Arbitrage Controller", y_range=y_range)
    p.grid.grid_line_alpha = 0.3
    p.extra_y_ranges = {
        "BTCUAH": Range1d(start=y_range[0] * DEFAULT_COEFF, end=y_range[1] * DEFAULT_COEFF),
    }
    p.add_layout(LinearAxis(y_range_name="BTCUAH"), 'left')
    return p


def serve_frontend(doc):
    src_store = TimeSeriesStore(name="bitfinex_btcusd", columns=formats.history_format, time_unit="s", time_field="timestamp")
    tgt_store = TimeSeriesStore(name="kuna_btcuah", columns=formats.history_format, time_unit="s")
    ord_store = TimeSeriesStore(name="kuna_orderbook", columns=formats.orderbook_format, time_unit="s")

    src_df = src_store.read_latest()
    tgt_df = tgt_store.read_latest()
    ord_df = ord_store.read_latest()
    source_src = ColumnDataSource(data=src_df)
    source_tgt = ColumnDataSource(data=tgt_df)
    source_ord = ColumnDataSource(data=ord_df)

    plot = init_plot()
    # plot2 = init_plot()

    plot.line('Time', 'price', source=source_src, line_width=3, color=Spectral4[0], alpha=0.8, legend="SRC")
    plot.line('Time', 'price', source=source_tgt, line_width=3, color=Spectral4[1], alpha=0.8, legend="TGT", y_range_name="BTCUAH")
    plot.line('Time', 'bid', source=source_ord, line_width=1, color=Spectral4[2], alpha=1, legend="Bid", y_range_name="BTCUAH")
    plot.line('Time', 'ask', source=source_ord, line_width=1, color=Spectral4[3], alpha=1, legend="Ask", y_range_name="BTCUAH")
    #
    # plot2.line('Time', 'price', source=source_src, line_width=3, color=Spectral4[0], alpha=0.8, legend="SRC")
    # plot2.line('Time', 'price', source=source_tgt, line_width=3, color=Spectral4[1], alpha=0.8, legend="TGT", y_range_name="BTCUAH")
    # plot2.line('Time', 'bid', source=source_ord, line_width=1, color=Spectral4[2], alpha=1, legend="Bid", y_range_name="BTCUAH")
    # plot2.line('Time', 'ask', source=source_ord, line_width=1, color=Spectral4[3], alpha=1, legend="Ask", y_range_name="BTCUAH")
    #
    # output_file("arbitrage3.html", title="Arbitrage between cryptowat.ch and kuna.io")
    # save(plot2)
    # Set up widgets
    wgt_refresh = Button(label='Refresh')

    def on_refresh(button):
        button.label = 'OK...'

    wgt_refresh.on_click(lambda: on_refresh(wgt_refresh))

    wgt_text = TextInput(title="Manual Rate", value=str(DEFAULT_COEFF))

    def on_coeff_change(attrname, old, new):
        print(attrname, old, new)

    wgt_text.on_change('value', on_coeff_change)

    inputs = widgetbox(wgt_refresh, wgt_text)

    doc.add_root(row(inputs, plot, width=1500))
    doc.title = "Arbitrage Controller"
