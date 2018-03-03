import pandas as pd

def create_empty_dataframe(columns):
    _df = pd.DataFrame(columns=columns)
    _df['Time'] = pd.to_datetime(_df.created_at)
    _df.set_index('Time', inplace=True)
    return _df