import psycopg

from config import CONN_STR


def with_connection(f):
    def with_connection_(*args, **kwargs):
        conn = psycopg.connect(CONN_STR)
        try:
            rv = f(*args, **kwargs, conn=conn)
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()
        finally:
            conn.close()

        return rv

    return with_connection_
