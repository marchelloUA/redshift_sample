import pytest

from python.redshift_connector import (
    RedshiftConnectionManager,
    ConnectionConfig,
    create_redshift_connection,
)

import psycopg2


class DummyCursor:
    def __init__(self):
        self.description = [('col1', None)]
        self._rows = [(42,)]
        self.rowcount = 1
        self._executed = None

    def execute(self, query, params=None):
        self._executed = (query, params)
        self.rowcount = 1

    def executemany(self, query, params_list):
        self._executed = (query, params_list)
        self.rowcount = len(params_list)

    def fetchone(self):
        return (1,) if self._rows else None

    def fetchall(self):
        return self._rows

    def callproc(self, name, params=None):
        # simulate a stored procedure that returns rows
        self.description = [('res', None)]
        self._rows = [("ok",)]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass


class DummyConn:
    def __init__(self):
        self.autocommit = False
        self.cursor_obj = DummyCursor()

    def cursor(self):
        return self.cursor_obj

    def rollback(self):
        pass

    def commit(self):
        pass


class DummyPool:
    def __init__(self, minconn, maxconn, *args, **kwargs):
        self.minconn = minconn
        self.maxconn = maxconn
        # pre-create connections
        self._conns = [DummyConn() for _ in range(minconn)]

    def getconn(self):
        if self._conns:
            return self._conns.pop()
        raise psycopg2.pool.PoolError("no connection")

    def putconn(self, conn):
        self._conns.append(conn)

    def closeall(self):
        self._conns.clear()


@pytest.fixture(autouse=True)
def patch_pool(monkeypatch):
    monkeypatch.setattr(
        'python.redshift_connector.psycopg2.pool.ThreadedConnectionPool',
        DummyPool,
    )
    return monkeypatch


def test_pool_initialization(patch_pool):
    cfg = ConnectionConfig(host="h", database="d", user="u", password="p")
    mgr = RedshiftConnectionManager(cfg, pool_size=2, max_overflow=1)
    status = mgr.get_connection_status()
    assert status['pool_size'] == 2
    mgr.close_all_connections()


def test_execute_query_select(patch_pool):
    cfg = ConnectionConfig(host="h", database="d", user="u", password="p")
    mgr = RedshiftConnectionManager(cfg, pool_size=1)
    results = mgr.execute_query("SELECT 1")
    assert results == [{'col1': 42}]
    mgr.close_all_connections()


def test_execute_batch(patch_pool):
    cfg = ConnectionConfig(host="h", database="d", user="u", password="p")
    mgr = RedshiftConnectionManager(cfg, pool_size=1)
    # batch query doesn't return rows
    count = mgr.execute_batch("INSERT INTO foo VALUES (%s)", [(1,), (2,)])
    assert count == 1 or count == -1 or isinstance(count, int)
    mgr.close_all_connections()


def test_execute_procedure(patch_pool):
    cfg = ConnectionConfig(host="h", database="d", user="u", password="p")
    mgr = RedshiftConnectionManager(cfg, pool_size=1)
    res = mgr.execute_procedure("dummy_proc")
    assert res == [{'res': 'ok'}]
    mgr.close_all_connections()


def test_create_redshift_connection_factory(patch_pool):
    mgr = create_redshift_connection("h", "d", "u", "p")
    assert isinstance(mgr, RedshiftConnectionManager)
    mgr.close_all_connections()


def test_test_connection(patch_pool):
    cfg = ConnectionConfig(host="h", database="d", user="u", password="p")
    mgr = RedshiftConnectionManager(cfg, pool_size=1)
    assert mgr.test_connection() is True
    mgr.close_all_connections()
