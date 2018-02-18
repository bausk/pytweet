import os
from urllib import parse
import psycopg2
from psycopg2.extras import Json
import json
import pandas as pd
from pandas.core.base import DataError
from datetime import datetime, timedelta
import numpy as np

from constants import formats

parse.uses_netloc.append("postgres")
url = parse.urlparse(os.environ["DATABASE_URL"])

def reconnect():
    return psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )

def ensure_connection(connection):
    try:
        connection.isolation_level
        return connection
    except BaseException as e:
        return reconnect()


conn = reconnect()


class TimeSeriesStore(object):

    def __init__(self, name, columns, update_period=None, time_field='timestamp', time_unit=None,
                 duplicates_field=None):
        if update_period is None:
            update_period = timedelta(seconds=60)
        self._username = url.username
        self.name = name
        self._columns = columns
        self._period = update_period
        self._df: pd.DataFrame = None
        self._last_updated = datetime.utcnow()
        self._local_cache = {}
        self._duplicates_field = duplicates_field
        self._time_field = time_field
        self._time_unit = time_unit
        self._table_connected = False
        self._table_spec = [
            ('created_at', self._fetch_datetime),
            ('collected_at', self._fetch_datetime),
            ('data', self._fetch_json),
            ('metadata', self._fetch_json),
            ('id', lambda x: x)]

    def _fetch_datetime(self, val):
        return pd.to_datetime(val)

    def _fetch_json(self, val):
        if type(val) == str:
            return json.loads(val)
        else:
            return val

    def init_table(self, name=None, user=None):
        if user is None:
            user = self._username
        if name is None:
            name = self.name

        command = """
            CREATE TABLE public.{name}
            (
                created_at timestamp with time zone NOT NULL,
                collected_at timestamp with time zone NOT NULL,
                data jsonb,
                metadata jsonb,
                id bigserial NOT NULL,
                PRIMARY KEY (id)
            )
            WITH (
                OIDS = FALSE
            );
            
            ALTER TABLE public.{name}
                OWNER to {user};
        """.format(name=name, user=user)

        connection = ensure_connection(conn)
        cur = connection.cursor()
        cur.execute(command)
        cur.close()
        self._table_connected = True
        return True

    def _connect_table(self, init=True):
        connection = ensure_connection(conn)
        cur = connection.cursor()
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (self.name,))
        result = cur.fetchone()[0]
        cur.close()
        if result:
            self._table_connected = True
            return True
        else:
            if init:
                return self.init_table()
            else:
                return False

    def init_dataframe(self):
        print("[info] Loading data for {}".format(self.name))
        self._df = self._perform_load()
        return self._df

    def append(self, data):
        df = self._prepare_data(data)
        self._update_cache(df)
        return self.dataframe

    def _prepare_data(self, data):
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
        return df

    def _update_cache(self, df):
        df = df.fillna(0)
        indexed = df.to_dict(orient='index')
        indexed = {k.to_pydatetime().timestamp(): v for k, v in indexed.items()}
        self._local_cache.update(indexed)
        current_time = datetime.utcnow()
        if any([not self._period, current_time - self._last_updated > self._period]):
            self._perform_persist()
        return self._update_df(df)

    def _remote_to_df(self, data):
        res = {}
        # currently, only trunk.data is interesting
        for trunk in data:
            res.update(trunk['data'])
        return self._internal_to_df(res)

    def _internal_to_df(self, data):
        _df = pd.DataFrame.from_dict(data, orient='index')
        _df['Time'] = pd.to_datetime(_df.index, unit="s")
        _df.set_index('Time', inplace=True)
        return _df

    def _perform_load(self):
        if not self._table_connected:
            self._connect_table()
        connection = ensure_connection(conn)
        cur = connection.cursor()
        command = """
        select * from public.{name};
        """.format(name=self.name)
        cur.execute(command)
        res = cur.fetchall()
        prepared_result = [self._row(x) for x in res]
        return self._remote_to_df(prepared_result)

    def _row(self, data):
        return {spec[0]: spec[1](row) for row, spec in zip(data, self._table_spec)}

    def _perform_persist(self):
        if not self._table_connected:
            self._connect_table()
        connection = ensure_connection(conn)
        cur = connection.cursor()
        command = """
        insert into public.{name} (created_at, collected_at, data, metadata) values (%s, %s, %s, %s)
        """.format(name=self.name)
        created_at = datetime.utcnow()
        collected_at = datetime.utcnow()
        data = Json(self._local_cache)
        metadata = Json({})
        try:
            cur.execute(command, (created_at, collected_at, data, metadata))
            connection.commit()
            self._local_cache = {}
        except BaseException as e:
            print("[error] Cache flush failed!")
        finally:
            cur.close()

    def _update_df(self, new_data):
        if len(new_data) == 0:
            return self.dataframe

        rv = pd.concat([self.dataframe, new_data])
        if self._duplicates_field:
            rv = rv.drop_duplicates(subset=self._duplicates_field)
        # rv.sort_index(inplace=True)
        self._df = rv
        return rv

    @property
    def dataframe(self):
        if self._df is None:
            self._df = self.init_dataframe()
        return self._df
