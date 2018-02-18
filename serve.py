import numpy as np
import pandas as pd

from bokeh.io import curdoc
from bokeh.layouts import row, widgetbox
from bokeh.models import ColumnDataSource
from bokeh.models.widgets import Slider, TextInput
from bokeh.plotting import figure
from bokeh.palettes import Spectral4

from constants import currencies, periods, remote
from sources.historical import CryptowatchSource, KunaIoSource
from persistence.postgre import TimeSeriesStore

# Set up data
N = 200
x = np.linspace(0, 4*np.pi, N)
y = np.sin(x)
source = ColumnDataSource(data=dict(x=x, y=y))


def refresh_data():
    target_df = pd.read_pickle('target_df.pkl')
    target_df.sort_index(inplace=True)

    source_df = pd.read_pickle('source_df.pkl')
    source_df.sort_index(inplace=True)

    orders_df = pd.read_pickle('order_df.pkl')
    orders_df.sort_index(inplace=True)
    return source_df, target_df, orders_df


def init_plot(source_df, target_df, orders_df, y_range=(8000, 9000)):
    TOOLS = "pan,wheel_zoom,xwheel_zoom,box_zoom,reset,save"
    p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=1000, title="Arbitrage Controller", y_range=y_range)

    p.grid.grid_line_alpha = 0.3

    p.line(source_df.index, source_df['price'], line_width=3, color=Spectral4[0], alpha=0.8, legend="Gax")
    p.line(target_df.index, target_df['price'], line_width=3, color=Spectral4[1], alpha=0.8, legend="Kuna")
    p.line(orders_df.index, orders_df['bid'], line_width=1, color=Spectral4[2], alpha=0.5, legend="Bid")
    p.line(orders_df.index, orders_df['ask'], line_width=1, color=Spectral4[3], alpha=0.5, legend="Ask")

    output_file("arbitrage2.html", title="Arbitrage between cryptowat.ch and kuna.io")
    save(p)
    pd.to_pickle(source_df, 'source_df.pkl')
    pd.to_pickle(target_df, 'target_df.pkl')
    sleep(1)



# Set up callbacks
def update_title(attrname, old, new):
    plot.title.text = text.value

text.on_change('value', update_title)

def update_data(attrname, old, new):
    # Get the current slider values
    a = amplitude.value
    b = offset.value
    w = phase.value
    k = freq.value

    # Generate the new curve
    x = np.linspace(0, 4*np.pi, N)
    y = a*np.sin(k*x + w) + b

    source.data = dict(x=x, y=y)

for w in [offset, amplitude, phase, freq]:
    w.on_change('value', update_data)

# Set up layouts and add to document
inputs = widgetbox(text, offset, amplitude, phase, freq)

curdoc().add_root(row(inputs, plot, width=800))
curdoc().title = "Sliders"