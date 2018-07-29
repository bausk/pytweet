from datetime import timedelta, datetime
from sys import getsizeof

import pandas as pd
from pandas.core.base import DataError
from psycopg2.extras import Json

from persistence.postgre import BaseSQLStore, url


class OnlineStore(object):

    def __init__(self, columns, time_field='timestamp', time_unit=None, duplicates_field=None):
        self._columns = columns
        self._df: pd.DataFrame = None
        self._duplicates_field = duplicates_field
        self._time_field = time_field
        self._time_unit = time_unit

    def save(self, data):
        prepared_data = self._prepare_data(data)

    def _prepare_data(self, data):
        """
        Returns a cleaned up unique-key dataframe according to _columns spec
        :param data:
        :return:
        """
        if isinstance(data, pd.Series):
            data = [data]
        df = pd.DataFrame(data, columns=self._columns).groupby(self._time_field)
        columns = []
        for column in self._columns:
            if column != 'volume':
                try:
                    series = df[column].mean()
                except DataError as e:
                    series = df[column].first()
            else:
                series = df[column].sum()
            columns.append(series)
        df = pd.concat(columns, axis=1)
        df['Time'] = pd.to_datetime(df[self._time_field], unit=self._time_unit)
        df.set_index('Time', inplace=True)
        del data, columns
        df.sort_index(inplace=True)
        return df

    def _internal_to_df(self, data):
        _df = pd.DataFrame.from_dict(data, orient='index')
        _df['Time'] = pd.to_datetime(_df.index, unit="s")
        _df.set_index('Time', inplace=True)
        return _df
