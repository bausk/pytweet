from datetime import datetime, timedelta
import dateutil.parser
from psycopg2.extras import Json
import pandas as pd
from .postgre import TimeSeriesStore


class PandasReader(TimeSeriesStore):

    def __init__(self, name, columns, **kw):
        super().__init__(name, columns, **kw)
        self._loaded_chunks = {}
        _df = pd.DataFrame(columns=['created_at', 'id', 'loaded'])
        _df['Time'] = pd.to_datetime(_df.created_at)
        _df.set_index('Time', inplace=True)
        self._segments_df = _df
        self._df = None
        self._last_queried = None

    def read_latest(self, start=None, end=None, trunks=None):
        if trunks:
            return self._load_latest_trunks(trunks)

        connection = self._ensure_connection()
        cur = connection.cursor()

        now = datetime.utcnow()
        if self._last_queried is None or now - self._last_queried > timedelta(minutes=5):
            self._refresh_segments(now, cur)


        # TODO: figure out which trunks to load
        if end == 0:
            pass

    def _refresh_segments(self, now, cur):
        command = """
        select id, created_at from public.{name}
        order by id desc 
        ;
        """.format(name=self.name)
        cur.execute(command)
        res = cur.fetchall()


    def _load_latest_trunks(self, trunks):
        result = self._perform_limited_pure_load(trunks=trunks)
        self._loaded_chunks.update({trunk['id']: dict(id=trunk['id'], created_at=trunk['created_at']) for trunk in result})
        result = self._remote_to_df(result)
        if self._df is not None:
            self._df = pd.concat([self._df, result]).drop_duplicates()
            self._df.sort_index(inplace=True)
        else:
            self._df = result
        return self._df

    def _get_last_trunk_id(self, cur):
        command = """
        select id from public.{name}
        order by id desc 
        limit 1
        ;
        """.format(name=self.name)
        cur.execute(command)
        res = cur.fetchall()
        return res[0][0]

    def _perform_limited_pure_load(self, since=None, trunks=2):
        connection = self._ensure_connection()
        cur = connection.cursor()

        if len(self._loaded_chunks) < trunks:
            # perform full load
            command = """
            select * from public.{name}
            order by id desc 
            limit {trunks}
            ;
            """.format(name=self.name, trunks=trunks)
        else:
            command = """
            select * from public.{name}
            order by id desc 
            limit 1
            ;
            """.format(name=self.name)

        cur.execute(command)
        res = cur.fetchall()
        prepared_result = [self._row(x) for x in res]
        return prepared_result


class Cleaner(TimeSeriesStore):

    def clean(self):
        connection = self._ensure_connection()
        cur = connection.cursor()
        command = """
        select * from public.{name}
        order by id desc ;
        """.format(name=self.name)
        cur.execute(command)
        res = cur.fetchall()
        prepared_result = [self._row(x) for x in res]
        for trunk in prepared_result:
            new_data = {}
            data = trunk['data']
            for key, item in data.items():
                created_at = item['created_at']
                checked_stamp = dateutil.parser.parse(created_at).timestamp()
                if float(key) == checked_stamp:
                    new_data[key] = item
                else:
                    print (key, checked_stamp)
            self.update_trunk(trunk['id'], new_data)

    def update_trunk(self, id, trunk_data):
        connection = self._ensure_connection()
        cur = connection.cursor()
        command = """
        UPDATE public.{name} SET data=(%s) WHERE id=(%s)
        """.format(name=self.name)
        data = Json(trunk_data)
        try:
            cur.execute(command, (data, id))
            connection.commit()
        except BaseException as e:
            print("[error] Cache flush failed!")
        finally:
            cur.close()