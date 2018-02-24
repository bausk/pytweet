from bokeh.io import curdoc
from frontend.server import serve_frontend


serve_frontend(curdoc())
