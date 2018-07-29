from datetime import timedelta, datetime
from sys import getsizeof

import pandas as pd
from pandas.core.base import DataError
from psycopg2.extras import Json

from persistence.postgre import BaseSQLStore, url


class TimeSeriesStore(BaseSQLStore):

    def __init__(self, name, columns, update_period=None, time_field='timestamp', time_unit=None, duplicates_field=None,
                 x_shift_hours=0):
        super().__init__(name)
        if update_period is None:
            update_period = timedelta(seconds=60)
        self._columns = columns
        self._persist_frequency = update_period # No persist frequency will result in updating DB on every run
        self._new_row_policy = 20000
        self._df: pd.DataFrame = None
        self._last_updated = datetime.utcnow()
        self._local_cache = None
        self._duplicates_field = duplicates_field
        self._time_field = time_field
        self._time_unit = time_unit
        self._current_trunk_id = None
        self._x_shift = None if x_shift_hours == 0 else timedelta(hours=x_shift_hours)
        self._trunk_opened_datetime = None
        self._table_spec = [
            'created_at',
            'collected_at',
            'data',
            'metadata',
            'id'
        ]

    def write(self, data):
        self._ensure_cache()
        df = self._prepare_data(data).truncate(before=self._trunk_opened_datetime)
        self._update_cache(df)

    def _get_initial_store(self):
        if not self._table_connected:
            self._connect_table()
        connection = self._ensure_connection()
        cur = connection.cursor()
        command = """
        select * from public.{name}
        order by id desc 
        limit 1
        ;
        """.format(name=self.name)
        cur.execute(command)
        res = cur.fetchall()
        prepared_result = [self._row(x) for x in res]
        result_dict, trunk_start_time, trunk_id = self._remote_to_dict(prepared_result)
        if trunk_id is None:
            self._start_new_trunk({})
        else:
            self._current_trunk_id = trunk_id
            self._trunk_opened_datetime = trunk_start_time
        return result_dict

    def _start_new_trunk(self, data):
        if not self._table_connected:
            self._connect_table()
        connection = self._ensure_connection()
        cur = connection.cursor()
        command = """
        insert into public.{name} (created_at, collected_at, data, metadata) values (%s, %s, %s, %s) returning id;
        """.format(name=self.name)
        created_at = datetime.utcnow()
        collected_at = datetime.utcnow()
        data = Json(data)
        metadata = Json({})
        try:
            cur.execute(command, (created_at, collected_at, data, metadata))
            connection.commit()
            new_id = cur.fetchone()[0]
            self._trunk_opened_datetime = created_at
            self._current_trunk_id = new_id
        except BaseException as e:
            print("[error] New trunk creation failed!")
            new_id = None
        finally:
            cur.close()
        return new_id

    def _prepare_data(self, data):
        """
        Returns a cleaned up unique-key dataframe according to _columns spec
        :param data:
        :return:
        """
        self._ensure_cache()
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

    def _update_cache(self, df):
        """
        Checks if there is need to write to a new slot
        :param df:
        :return:
        """
        self._ensure_cache()
        df = df.fillna(0)
        indexed = {k.to_pydatetime().timestamp(): v for k, v in df.to_dict(orient='index').items()}

        self._local_cache.update(indexed)
        current_time = datetime.utcnow()
        if any([not self._persist_frequency, current_time - self._last_updated > self._persist_frequency]):
            self._perform_persist()
        del df
        return None

    def _ensure_cache(self):
        if self._local_cache is None:
            self._local_cache = self._get_initial_store()

    def _remote_to_df(self, data):
        res = {}
        # currently, only trunk.data is interesting
        for trunk in data:
            res.update(trunk['data'])
        return self._internal_to_df(res)

    def _remote_to_dict(self, data):
        res = {}
        start_time = None
        trunk_id = None
        # currently, only trunk.data is interesting
        for trunk in data:
            res.update(trunk['data'])
            start_time = trunk['created_at']
            trunk_id = trunk['id']
        return res, start_time, trunk_id

    def _internal_to_df(self, data):
        _df = pd.DataFrame.from_dict(data, orient='index')
        if self._x_shift is None:
            _df['Time'] = pd.to_datetime(_df.index, unit="s")
        else:
            _df['Time'] = pd.to_datetime(_df.index, unit="s") - self._x_shift
        _df.set_index('Time', inplace=True)
        return _df

    def _row(self, data):
        return {spec: row for row, spec in zip(data, self._table_spec)}

    def _perform_persist(self):
        if not self._table_connected:
            self._connect_table()
        connection = self._ensure_connection()
        self._ensure_cache()
        cur = connection.cursor()
        command = """
        UPDATE public.{name} SET data=(%s) WHERE id=(%s)
        """.format(name=self.name)
        data = Json(self._local_cache)
        try:
            cur.execute(command, (data, self._current_trunk_id))
            connection.commit()
            self._last_updated = datetime.utcnow()
        except BaseException as e:
            print("[error] Cache flush failed!")
        finally:
            cur.close()
        if self._need_new_trunk():
            self._start_new_trunk({})
            self._local_cache = {}

    def _need_new_trunk(self):
        if type(self._new_row_policy) is int:
            if getsizeof(self._local_cache) > self._new_row_policy:
                return True
        return False