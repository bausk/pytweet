python collect.py > stdout.txt &
bokeh serve --port $PORT --host=* --address=0.0.0.0 --use-xheaders testserve.py
