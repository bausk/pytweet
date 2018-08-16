from datetime import datetime, timedelta
from utils.timing import user_input_to_utc_time
from types import SimpleNamespace
import itertools

class BaseSimulator:
    normalized_names = ['normalized_orderbook', 'normalized_source']
    orignial_names = ['original_orderbook', 'original_source']

    def __init__(self, before=None, after=None, freq='10'):
        self._before = before
        self._after = after
        self._freq = freq
        self._options = SimpleNamespace(
            before=None, after=None, freq=None
        )
        self._current_caret_start = None
        self._start = None
        self._named_dataframes = {}

    def add_dataframe(self, df, name=None):
        if name is None:
            name = self._new_name()
        self._named_dataframes[name] = df

    def get_preprocessor(self):
        if all(x in self._named_dataframes for x in self.normalized_names):
            return lambda *x: x
        return None

    def _new_name(self):
        for x in itertools.count(0):
            if x not in self._named_dataframes:
                return x

    def _prepare_params(self):
        rv = SimpleNamespace()
        before = str(self._before.value if getattr(self._before, 'value') else self._before)
        after = str(self._after.value if getattr(self._after, 'value') else self._after)
        rv.before, _ = user_input_to_utc_time(before)
        rv.after, _ = user_input_to_utc_time(after)
        try:
            rv.freq = timedelta(seconds=float(self._freq.value if getattr(self._freq, 'value') else self._freq))
        except:
            rv.freq = timedelta(seconds=10)
        earliest_start = min(df.index[0] for i, df in self._named_dataframes.items())
        self._current_caret_start = max(earliest_start, rv.after) if rv.after is not None else earliest_start
        self._start = self._current_caret_start
        return rv

    def _stop_condition(self):
        condition = all(len(df[df.index > self._current_caret_start]) == 0 for i, df in self._named_dataframes.items())
        return condition

    def __iter__(self):
        self._options = self._prepare_params()
        return self

    def __next__(self):
        if self._options.freq is None:
            raise TypeError('Option `freq` should be of timedelta type for Simulator object')
        if self._stop_condition():
            raise StopIteration
        rv = {}
        end_date = self._current_caret_start + self._options.freq
        if self._options.before is not None and end_date > self._options.before:
            end_date = self._options.before
        for key, df in self._named_dataframes.items():
            view = df.loc[(df.index >= self._start) & (df.index < end_date)]
            rv[key] = view
        self._current_caret_start += self._options.freq
        return rv
