# Hypotheis tester on naive data mining for stock prediction in Python 3.7+

## Install

Git clone, run `pip install -r requirements.txt`.

## Environment

### Runtime


### Environment variables

KUNA_AUTH --

## Run

Several run modes are implemented.

### Heroku database collector/frontend executor

Heroku deployment is served via `./run.sh` script.

### Bokeh interactive frontend

`frontend/server.py` implements the interactive frontend. There are two ways to start the server.

1. Via `bokeh` command:

`bokeh serve --port $PORT --host=* --address=0.0.0.0 --use-xheaders serve.py`

2. As a standalone Python process:

`python standalone.py`


## Develop