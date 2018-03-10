from persistence.postgre import BaseSQLStore
from constants.formats import status_records
from datetime import datetime


class StatusStore(BaseSQLStore):

    def __init__(self, name):
        super().__init__(name, format=status_records)

    def get_value(self, name):
        connection = self._ensure_connection(ensure_table=True)
        cur = connection.cursor()
        command = """
        select value from public.{name}
        WHERE name=(%s);
        """.format(name=self.name)
        try:
            cur.execute(command, (name, ))
            connection.commit()
            data = cur.fetchone()[0]

        except BaseException as e:
            data = None
        finally:
            cur.close()
        return data

    def set_value(self, name, value):
        connection = self._ensure_connection(ensure_table=True)
        cur = connection.cursor()
        command = """
        insert into public.{name} (created_at, name, value) values (%s, %s, %s)
        ON CONFLICT (name) DO UPDATE SET created_at=(%s), value=(%s);
        """.format(name=self.name)
        created_at = datetime.utcnow()
        try:
            cur.execute(command, (created_at, name, value, created_at, value))
            connection.commit()
            rv = value
        except BaseException as e:
            rv = None
        finally:
            cur.close()
        return rv
