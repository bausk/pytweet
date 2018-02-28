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
        start = now - timedelta(hours=start)
        end = now - timedelta(hours=end)

        if self._last_queried is None or now - self._last_queried > timedelta(minutes=5):
            self._refresh_segments(now, cur)

        self._segments_df.sort_index(inplace=True)
        segments_to_query = list(self._segments_df.loc[
            (start <= self._segments_df.index)
            & (self._segments_df.index <= end)
            & (self._segments_df.loaded == False)
                                 ]['id'].tolist())
        print(segments_to_query)
        # TODO: figure out which additional trunks to load
        # TODO: 1) One previous trunk
        try:
            prev_segment = self._segments_df.loc[(self._segments_df.index < start)].iloc[-1]
            if not prev_segment['loaded']:
                prev_segment_id = int(prev_segment['id'])
            else:
                prev_segment_id = None
        except:
            prev_segment_id = None
        if prev_segment_id is not None:
            segments_to_query.append(prev_segment_id)
        # TODO: 2) One latest trunk
        if end == 0:
            last_segment_id = self._get_last_trunk_id(cur)
            if last_segment_id not in segments_to_query:
                segments_to_query.append(last_segment_id)
        if len(segments_to_query) > 0:
            command = """
            select * from public.{name}
            WHERE id IN %s;
            """.format(name=self.name)
            cur.execute(command, (tuple(segments_to_query), ))
            res = cur.fetchall()
            res = [self._row(x) for x in res]
            self._segments_df.loc[self._segments_df.id.isin(segments_to_query), 'loaded'] = True
            return self._update_internal_df(self._remote_to_df(res))
        else:
            return self._df


    def _refresh_segments(self, now, cur):
        command = """
        select id, created_at from public.{name}
        order by id desc 
        ;
        """.format(name=self.name)
        cur.execute(command)
        res = cur.fetchall()
        data = {x[1]: dict(id=x[0], created_at=x[1], loaded=False) for x in res}
        _df = pd.DataFrame.from_dict(data, orient='index')
        _df['Time'] = _df.index
        _df.set_index('Time', inplace=True)
        self._segments_df = _df
        self._last_queried = now
        return _df

    def _load_latest_trunks(self, trunks):
        result = self._perform_limited_pure_load(trunks=trunks)
        self._loaded_chunks.update({trunk['id']: dict(id=trunk['id'], created_at=trunk['created_at']) for trunk in result})
        result = self._remote_to_df(result)
        return self._update_internal_df(result)

    def _update_internal_df(self, result):
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