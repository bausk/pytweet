import os
import math
import json
from datetime import timedelta, datetime

import pandas as pd

from bokeh.layouts import row, widgetbox, column
from bokeh.models import ColumnDataSource, Range1d, LinearAxis, DatetimeTicker, CustomJS
from bokeh.models.widgets import TextInput, Button, PreText, Toggle, DataTable, TableColumn, DateFormatter, \
    StringFormatter
from bokeh.plotting import figure
from bokeh.palettes import Spectral4

from analyzers.simple import BaseArbitrageAnalyzer
from algorithms.arbitragealgorithm import ArbitrageAlgorithm
from constants import currencies
from sources.historical import CryptowatchSource, KunaIoSource
from parsers.rates import get_rate
from persistence.pandas import PandasReader
from models.trader import LiveTrader
from models.simulator import BaseSimulator
from models.arbitrageplotter import ArbitragePlotter
from implementations.kuna import KunaExchange
from models.status_store import StatusStore
from constants import formats
from constants.constants import MONITOR_CHART_NAMES as GLYPHNAMES

DEFAULT_COEFF = 27.0
USD_LOW = 10000
USD_HIGH = 12000

KUNA_AUTH = json.loads(os.getenv("KUNA_AUTH", "{}"))


def format_columns(columns):
    rv = []
    for x in columns:
        if 'time' in x or 'date' in x:
            rv.append(TableColumn(field=x, title=x, formatter=DateFormatter(format="%m-%d %H:%M:%S")))
        else:
            rv.append(TableColumn(field=x, title=x, formatter=StringFormatter()))
    return rv


def init_plot(y_range=(USD_LOW, USD_HIGH)):
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
    return p


def serve_frontend(doc):
    current_time = datetime.utcnow()
    statuses = StatusStore("statuses_store")
    console = PreText(text="#>\n", width=500, height=100)

    src_store = PandasReader(name="bitfinex_btcusd", columns=formats.history_format, time_unit="s",
                             time_field="timestamp")
    tgt_store = PandasReader(name="kuna_btcuah", columns=formats.history_format, time_unit="s", x_shift_hours=0)
    ord_store = PandasReader(name="kuna_orderbook", columns=formats.orderbook_format, time_unit="s")

    trader = LiveTrader(
        name="bitfinex_kuna_arbitrage_trades",
        console=console,
        columns=formats.history_format,
        time_unit="s",
        time_field="timestamp"
    )

    src_df = src_store.read_latest(trunks=1)
    tgt_df = tgt_store.read_latest(trunks=1)
    ord_df = ord_store.read_latest(trunks=1)
    source_src = ColumnDataSource(data=dict(Time=src_df.index, price=src_df.price))
    source_tgt = ColumnDataSource(
        data=dict(Time=tgt_df.index, price=tgt_df.price, volume=tgt_df['volume'].multiply(100)))
    source_ord = ColumnDataSource(data=dict(Time=ord_df.index, ask=ord_df.get('ask', []), bid=ord_df.get('bid', [])))

    plotter = ArbitragePlotter()
    trader.add_graphics(plotter)

    wgt_start_date = TextInput(title="Start at hours before now:", value='5')
    wgt_end_date = TextInput(title="End at hours before now:", value='0')

    wgt_analyze_start = TextInput(title="Start at hours before now:", value='2018-02-27 10:00')
    wgt_analyze_end = TextInput(title="End at hours before now:", value='2018-02-27 14:00')

    wgt_autoscale_starthours = TextInput(title="Start at hours before end hour:", value='3')
    wgt_autoscale_endhours = TextInput(title="Start at hours before end hour:", value=str(10 / 60))

    wgt_refresh = Button(label='Refresh', button_type='success')

    def on_refresh():
        nonlocal src_df
        nonlocal tgt_df
        nonlocal ord_df
        try:
            src_df = src_store.read_latest(start=float(wgt_start_date.value), end=float(wgt_end_date.value))
            source_src.data = dict(Time=src_df.index, price=src_df.price)

            tgt_df = tgt_store.read_latest(start=float(wgt_start_date.value), end=float(wgt_end_date.value))
            ord_df = ord_store.read_latest(start=float(wgt_start_date.value), end=float(wgt_end_date.value))

            source_tgt.data = dict(Time=tgt_df.index, price=tgt_df.price, volume=tgt_df['volume'].multiply(100))
            source_ord.data = dict(Time=ord_df.index, ask=ord_df.ask, bid=ord_df.bid)
        except BaseException as e:
            print(e)

    wgt_refresh.on_click(lambda: on_refresh())

    wgt_manualrate = TextInput(title="Manual Rate:", value=str(DEFAULT_COEFF))

    def on_coeff_change(attrname, old, new):
        print(attrname, old, new)
        plotter._plot.extra_y_ranges['BTCUAH'].start = plotter._plot.y_range.start * float(new)
        plotter._plot.extra_y_ranges['BTCUAH'].end = plotter._plot.y_range.end * float(new)

    wgt_manualrate.on_change('value', on_coeff_change)

    wgt_autoscale = Button(label='Autoscale')

    def on_autoscale(button):
        nonlocal src_df
        nonlocal ord_df
        rate = get_rate(src_df, ord_df,
                        start=timedelta(hours=(float(wgt_autoscale_starthours.value) + float(wgt_end_date.value))),
                        end=timedelta(hours=(float(wgt_autoscale_endhours.value) + float(wgt_end_date.value)))
                        )
        if math.isnan(rate):
            pass
        else:
            wgt_manualrate.value = "{0:.3f}".format(1 / rate)

    wgt_autoscale.on_click(lambda: on_autoscale(wgt_autoscale))

    wgt_analyze = Button(label='Analyze', button_type='primary')
    # analyzer = BaseArbitrageAnalyzer(
    #     {'src_store': src_store, 'tgt_store': tgt_store, 'ord_store': ord_store},
    #     plot,
    #     start=wgt_analyze_start,
    #     end=wgt_analyze_end,
    #     console=console
    # )

    # def on_analyze(button):
    #     _, src_df, ord_df = analyzer.analyze()
    #     source_ord.data = dict(Time=ord_df.index, ask=ord_df.ask, bid=ord_df.bid)
    #     source_src.data = dict(Time=src_df.index, price=src_df.price)
    #
    # wgt_analyze.on_click(lambda: on_analyze(wgt_analyze))

    # fixate = Toggle(label='Fix Y Axis')
    #
    # def on_fix(button, state):
    #     console.text += "Changing fix to: " + str(state) + "\n"
    #     if state:
    #         jscode = """
    #                 range.start = parseInt({start});
    #                 range.end = parseInt({end});
    #             """
    #         plot.extra_y_ranges['UNITLESS'].callback = CustomJS(
    #             args=dict(range=plot.extra_y_ranges['UNITLESS']),
    #             code=jscode.format(start=plot.extra_y_ranges['UNITLESS'].start,
    #                                end=plot.extra_y_ranges['UNITLESS'].end)
    #         )
    #     else:
    #         plot.extra_y_ranges['UNITLESS'].callback = None
    #
    # fixate.on_click(lambda x: on_fix(fixate, x))

    wgt_livesim = Button(label='Simulate', button_type='primary')
    wgt_livesim_start = TextInput(
        title="Live simulation start:", value="2018-08-11 06:00"
    )
    wgt_livesim_end = TextInput(
        title="Livesimulation end:", value="2018-08-11 10:00"
    )
    wgt_livesim_freq = TextInput(
        title="Simulation frequency (seconds):", value="60"
    )

    def on_simulate():
        trader.simulate(
            normalized_orderbook='normalized_orderbook.csv',
            normalized_source='normalized_source.csv'
        )

    wgt_livesim.on_click(lambda: on_simulate())

    analyzer_inputs = widgetbox(
        wgt_refresh,
        wgt_start_date,
        wgt_end_date,
        wgt_autoscale,
        wgt_autoscale_starthours,
        wgt_autoscale_endhours,
        wgt_manualrate,
        wgt_analyze,
        wgt_analyze_start,
        wgt_analyze_end,
        # fixate,
        wgt_livesim,
        wgt_livesim_start,
        wgt_livesim_end,
        wgt_livesim_freq,
        console
    )

    text_publickey = TextInput(title="Public Key:", value=str(KUNA_AUTH.get('public_key')))
    text_secretkey = TextInput(title="Secret Key:", value=str(KUNA_AUTH.get('secret_key')))

    trader.add_trader_api(KunaExchange(text_publickey, text_secretkey))
    trader.add_source_api(CryptowatchSource(currency=currencies.BTC))
    trader.add_target_api(KunaIoSource(currency=currencies.BTC))
    trader.add_algorithm(ArbitrageAlgorithm(console=console))
    trader.add_simulator(BaseSimulator(
        after=wgt_livesim_start,
        before=wgt_livesim_end,
        freq=wgt_livesim_freq
    ))

    buy_all_button = Button(label='Buy All', button_type='success')
    sell_all_button = Button(label='Sell All', button_type='danger')
    cancel_all_button = Button(label='Cancel All', button_type='warning')

    button_livetrade = Toggle(label='[Live Trade]', button_type='primary')
    button_liveexecution = Toggle(label='☑ Execute', button_type='danger')
    algo_cycle_rate = trader.get_rate() * 1000

    def on_trade_toggle(button, state):
        statuses.set_value("SCRIPT_IS_LIVE", state)
        trader.log("Changing live trading to: " + str(state))
        if state:
            doc.add_periodic_callback(trader.signal_callback, algo_cycle_rate)
        else:
            doc.remove_periodic_callback(trader.signal_callback)

    button_livetrade.on_click(lambda x: on_trade_toggle(button_livetrade, x))
    if statuses.get_value("SCRIPT_IS_LIVE"):
        button_livetrade.active = True

    def on_buy(button):
        console.text = "[{}] BUY order placed:\n".format(datetime.now())
        console.text += json.dumps(trader.buy_all(), indent=2) + "\n"

    buy_all_button.on_click(lambda: on_buy(buy_all_button))

    def on_sell(button):
        console.text = "[{}] SELL order placed for {}\n".format(datetime.now(), 1.1203)
        console.text += json.dumps(trader.sell_all(), indent=2) + "\n"

    sell_all_button.on_click(lambda: on_sell(sell_all_button))

    def on_cancel(button):
        console.text = "[{}] Cancelled orders\n".format(datetime.now())
        console.text += json.dumps(trader.cancel_all(), indent=2) + "\n"

    cancel_all_button.on_click(lambda: on_cancel(cancel_all_button))

    testbutton_debugsignal = Toggle(label='☑ Debug Signal')

    def on_debugsignal(state):
        trader.log('[debug] signal emulation: ' + str(state))
        trader.emulate_signals(state)

    testbutton_debugsignal.on_click(lambda x: on_debugsignal(x))

    trader_inputs = widgetbox(
        button_livetrade,
        text_publickey,
        text_secretkey,
        testbutton_debugsignal,
        button_liveexecution,
        buy_all_button,
        sell_all_button,
        cancel_all_button
    )

    table1_source = ColumnDataSource(data=pd.DataFrame(columns=formats.signal_format))
    table1 = DataTable(source=table1_source, columns=format_columns(formats.signal_format), width=900)
    table2_source = ColumnDataSource(data=pd.DataFrame(columns=['timestamp', 'indicator']))
    table2 = DataTable(source=table2_source, columns=format_columns(['timestamp', 'indicator']), width=900)
    # Data feed
    execute_datafeed_button = Button(label='Load', button_type='primary')
    lines_to_load = TextInput(title="No. of lines", value='50')

    wide_formats = column(
        plotter.get_plot(),
        row(execute_datafeed_button, lines_to_load),
        table1,
        table2
    )

    def on_update_tables(button):
        count_to_load = int(lines_to_load.value)
        data = pd.DataFrame.from_records(trader.signal_history).tail(count_to_load)
        table1_source.data = data.to_dict(orient='list')
        if trader.algorithm.latest_dataframe is not None:
            series = trader.algorithm.latest_dataframe.tail(count_to_load).to_dict()
            table2_source.data = {'timestamp': list(series.keys()), 'indicator': list(series.values())}

    execute_datafeed_button.on_click(lambda: on_update_tables(execute_datafeed_button))

    doc.add_root(row(analyzer_inputs, trader_inputs, wide_formats, width=1850))
    doc.title = "Arbitrage Controller"
