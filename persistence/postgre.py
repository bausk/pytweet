import os
from urllib import parse
import psycopg2

from constants.formats import datastore_records

parse.uses_netloc.append("postgres")
url = parse.urlparse(os.environ["DATABASE_URL"])


def _reconnect():
    return psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )

_GLOBAL_CONNECTION = _reconnect()

class BaseSQLStore:
    def __init__(self, name, format=None):
        global _GLOBAL_CONNECTION
        if format is None:
            format = datastore_records
        self._connection  = _GLOBAL_CONNECTION
        self.name = name
        self._table_connected = False
        self._table_format = format
        self._username = url.username

    def _ensure_connection(self, ensure_table=False):
        global _GLOBAL_CONNECTION
        try:
            self._connection.isolation_level
        except BaseException:
            self._connection.close()
            _GLOBAL_CONNECTION = _reconnect()
            self._connection = _GLOBAL_CONNECTION

        if ensure_table:
            if not self._table_connected:
                self._connect_table()

        return self._connection

    def _connect_table(self, init=True):
        connection = self._ensure_connection()
        cur = connection.cursor()
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (self.name,))
        result = cur.fetchone()[0]
        cur.close()
        if result:
            self._table_connected = True
            return True
        else:
            if init:
                return self._init_table()
            else:
                return False

    def _init_table(self, name=None, user=None):
        if user is None:
            user = self._username
        if name is None:
            name = self.name

        table_spec = "\n".join(["{} {},".format(k, v) for k, v in self._table_format.items()])

        command = """
            CREATE TABLE public.{name}
            (
                {spec}
                PRIMARY KEY (id)
            )
            WITH (
                OIDS = FALSE
            );

            ALTER TABLE public.{name}
                OWNER to {user};
        """.format(spec=table_spec, name=name, user=user)

        connection = self._ensure_connection()
        cur = connection.cursor()
        cur.execute(command)
        connection.commit()
        cur.close()

        self._table_connected = True
        return True
