import os
import sys
from functools import wraps
import hashlib
import json
import pandas as pd


def hdf_log(filename='log.csv', key=None):
    store_path = os.path.dirname(os.path.realpath(sys.argv[0])) + "/" + filename
    if key is None:
        hashed = json.dumps(sys.argv) + json.dumps(store_path)
        key = hashlib.sha224(hashed).hexdigest()

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            rv = func(*args, **kwargs)
            try:
                pd.DataFrame.from_records(rv).to_csv(store_path)
            except Exception as e:
                print('Error during logging:\n', str(e))
            return rv

        return wrapper

    return decorator
