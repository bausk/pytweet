
from bokeh.plotting import figure, show, output_file, save
from bokeh.palettes import Category10

class DatetimePlot(object):

    def __init__(self, **kw):
        self.color = 0
        self.colors = Category10[10]
        tools = "pan,wheel_zoom,xwheel_zoom,box_zoom,reset,save"
        self._obj = figure(x_axis_type="datetime", tools=tools, plot_width=1000, y_range=(8000, 9000), **kw)
        self._obj.grid.grid_line_alpha = 0.3
        self._obj.legend.location = "top_left"
        self._obj.legend.click_policy = "hide"

    def line(self, *arg, **kw):
        color = self.colors[self.color]
        self.color += 1
        res = self._obj.line(*arg, line_width=3, color=color, alpha=0.8, **kw)
        return res

    def circle(self, *arg, **kw):
        color = self.colors[self.color]
        self.color += 1
        res = self._obj.circle(*arg, line_width=1, color=color, alpha=0.6, **kw)
        return res
