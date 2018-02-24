
from tornado.ioloop import IOLoop
from bokeh.application.handlers import FunctionHandler
from bokeh.application import Application
from bokeh.server.server import Server

from frontend.server import serve_frontend


if __name__ == '__main__':
    io_loop = IOLoop.current()
    bokeh_app = Application(FunctionHandler(serve_frontend))
    server = Server({'/': bokeh_app}, io_loop=io_loop)
    server.start()
    print('Opening Bokeh application on http://localhost:5006/')
    io_loop.add_callback(server.show, "/")
    io_loop.start()