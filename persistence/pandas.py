from datetime import datetime, timedelta
import dateutil.parser
from psycopg2.extras import Json

from .postgre import TimeSeriesStore


class PandasReader(TimeSeriesStore):

    def read_latest(self, since=None, trunks=2):
        if isinstance(since, datetime):
            pass
        elif since is None:
            since = datetime.utcnow() - timedelta(hours=6.0)
        else:
            try:
                since = dateutil.parser.parse(since)
            except BaseException as e:
                raise TypeError("Bad `since` value in TimeSeriesStore.read_latest(...)!")

        result = self._perform_limited_pure_load(since=since, trunks=trunks)
        return self._remote_to_df(result)

    def _perform_limited_pure_load(self, since=None, trunks=2):
        connection = self._ensure_connection()
        cur = connection.cursor()
        command = """
        select * from public.{name}
        order by id desc 
        limit {trunks}
        ;
        """.format(name=self.name, trunks=trunks)
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