import pytest
import time
import psycopg2
from unittest.mock import patch, MagicMock

from python.redshift_connector import (
    RedshiftConnectionManager,
    ConnectionConfig,
    ConnectionStatus,
    create_redshift_connection,
)


class DummyCursor:
    def __init__(self):
        self.description = [('col1', None)]
        self._rows = [(42,)]
        self.rowcount = 1
        self._executed = None

    def execute(self, query, params=None):
        self._executed = (query, params)

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
        self.closed = False

    def getconn(self):
        if self.closed:
            raise Exception("Pool is closed")
        if self._conns:
            return self._conns.pop()
        raise psycopg2.pool.PoolError("no connection")

    def putconn(self, conn):
        if not self.closed:
            self._conns.append(conn)

    def closeall(self):
        self.closed = True
        self._conns.clear()


@pytest.fixture(autouse=True)
def patch_pool(monkeypatch):
    monkeypatch.setattr(
        'python.redshift_connector.psycopg2.pool.ThreadedConnectionPool',
        DummyPool,
    )
    return monkeypatch


def test_connection_config_creation():
    """Test ConnectionConfig dataclass creation and defaults"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    assert config.host == "test-host"
    assert config.database == "test-db"
    assert config.user == "test-user"
    assert config.password == "test-password"
    assert config.port == 5439
    assert config.sslmode == "require"
    assert config.connect_timeout == 30


def test_connection_config_with_custom_values():
    """Test ConnectionConfig with custom values"""
    config = ConnectionConfig(
        host="custom-host",
        database="custom-db",
        user="custom-user",
        password="custom-password",
        port=9999,
        sslmode="verify-ca",
        connect_timeout=60,
        keepalives=2,
        keepalives_idle=600,
        keepalives_interval=120,
        application_name="custom-app"
    )
    
    assert config.port == 9999
    assert config.sslmode == "verify-ca"
    assert config.connect_timeout == 60
    assert config.keepalives == 2
    assert config.keepalives_idle == 600
    assert config.keepalives_interval == 120
    assert config.application_name == "custom-app"


def test_connection_manager_initialization():
    """Test RedshiftConnectionManager initialization"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    manager = RedshiftConnectionManager(config, pool_size=3, max_overflow=5)
    
    assert manager.config == config
    assert manager.pool_size == 3
    assert manager.max_overflow == 5
    assert manager.pool is not None
    
    # Cleanup
    manager.close_all_connections()


def test_connection_manager_initialization_with_defaults():
    """Test RedshiftConnectionManager with default values"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    manager = RedshiftConnectionManager(config)
    
    assert manager.pool_size == 5
    assert manager.max_overflow == 10
    
    # Cleanup
    manager.close_all_connections()


def test_get_connection_timeout():
    """Test get_connection with timeout behavior"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    manager = RedshiftConnectionManager(config, pool_size=1)
    
    # Test normal timeout
    with manager.get_connection(timeout=30) as conn:
        assert conn is not None
    
    # Test timeout error: make pool.getconn always raise PoolError so get_connection loops until timeout
    with patch.object(manager.pool, 'getconn', side_effect=psycopg2.pool.PoolError("no connection")):
        with pytest.raises(TimeoutError, match=r"Failed to get connection within .* seconds"):
            # small timeout for test speed
            with manager.get_connection(timeout=0.1):
                time.sleep(0.2)
    
    # Cleanup
    manager.close_all_connections()


def test_get_connection_error_handling():
    """Test error handling in get_connection"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    manager = RedshiftConnectionManager(config, pool_size=1)
    
    # Test connection error
    with patch.object(manager.pool, 'getconn', side_effect=Exception("Connection failed")):
        with pytest.raises(Exception, match="Connection failed"):
            with manager.get_connection(timeout=30):
                pass
    
    # Cleanup
    manager.close_all_connections()


def test_execute_query_no_results():
    """Test execute_query with non-SELECT query"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    manager = RedshiftConnectionManager(config, pool_size=1)
    
    # Mock cursor with no description (INSERT/UPDATE/DELETE)
    class NoResultsCursor:
        def __init__(self):
            self.description = None
            self.rowcount = 1
        
        def execute(self, query, params=None):
            pass
        
        def __enter__(self):
            return self
        
        def __exit__(self, exc_type, exc, tb):
            pass
    
    class NoResultsConn:
        def __init__(self):
            self.autocommit = False
        
        def cursor(self):
            return NoResultsCursor()
        
        def rollback(self):
            pass
        
        def commit(self):
            pass
    
    with patch.object(manager.pool, 'getconn', return_value=NoResultsConn()):
        results = manager.execute_query("INSERT INTO test VALUES (1)")
        assert results == []
    
    # Cleanup
    manager.close_all_connections()


def test_execute_query_with_params():
    """Test execute_query with parameters"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    manager = RedshiftConnectionManager(config, pool_size=1)
    
    # Mock the pool to return a mock connection whose cursor behaves as a context manager
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value.fetchall.return_value = [(1, 'test'), (2, 'test2')]
    mock_cursor.__enter__.return_value.description = [('id', None), ('name', None)]
    mock_conn.cursor.return_value = mock_cursor

    with patch.object(manager.pool, 'getconn', return_value=mock_conn):
        results = manager.execute_query("SELECT * FROM test WHERE id = %s", (1,))

        assert len(results) == 2
        assert results[0]['id'] == 1
        assert results[0]['name'] == 'test'
        assert results[1]['id'] == 2
        assert results[1]['name'] == 'test2'
    
    # Cleanup
    manager.close_all_connections()


def test_execute_batch_error_handling():
    """Test error handling in execute_batch"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    manager = RedshiftConnectionManager(config, pool_size=1)
    
    # Test batch execution error
    with patch.object(manager.pool, 'getconn') as mock_getconn:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # executemany is called on the cursor returned by the context manager __enter__
        mock_cursor.__enter__.return_value.executemany.side_effect = Exception("Batch failed")
        mock_conn.cursor.return_value = mock_cursor
        mock_getconn.return_value = mock_conn

        with pytest.raises(Exception, match="Batch failed"):
            manager.execute_batch("INSERT INTO test VALUES (%s)", [(1,), (2,)])

        # Verify rollback was called on the connection
        mock_conn.rollback.assert_called_once()
    
    # Cleanup
    manager.close_all_connections()


def test_execute_procedure_no_results():
    """Test execute_procedure with no results"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    manager = RedshiftConnectionManager(config, pool_size=1)
    
    class NoResultsProcCursor:
        def __init__(self):
            self.description = None
        
        def callproc(self, name, params=None):
            pass
        
        def __enter__(self):
            return self
        
        def __exit__(self, exc_type, exc, tb):
            pass
    
    class NoResultsProcConn:
        def __init__(self):
            self.autocommit = False
        
        def cursor(self):
            return NoResultsProcCursor()
        
        def rollback(self):
            pass
        
        def commit(self):
            pass
    
    with patch.object(manager.pool, 'getconn', return_value=NoResultsProcConn()):
        results = manager.execute_procedure("test_procedure")
        assert results == []
    
    # Cleanup
    manager.close_all_connections()


def test_get_connection_status_not_initialized():
    """Test get_connection_status when pool is not initialized"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    manager = RedshiftConnectionManager(config)
    manager.pool = None  # Force not initialized state
    
    status = manager.get_connection_status()
    assert status == {"status": "not_initialized"}


def test_close_all_connections():
    """Test close_all_connections method"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    manager = RedshiftConnectionManager(config, pool_size=1)
    
    # Test closing connections
    manager.close_all_connections()
    
    # Verify pool is closed
    assert manager.pool.closed


def test_close_all_connections_no_pool():
    """Test close_all_connections when pool is None"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    manager = RedshiftConnectionManager(config)
    manager.pool = None
    
    # Should not raise exception
    manager.close_all_connections()


def test_create_redshift_connection_factory():
    """Test create_redshift_connection factory function"""
    manager = create_redshift_connection(
        host="factory-host",
        database="factory-db",
        user="factory-user",
        password="factory-password",
        port=8888,
        sslmode="verify-full"
    )
    
    assert isinstance(manager, RedshiftConnectionManager)
    assert manager.config.host == "factory-host"
    assert manager.config.database == "factory-db"
    assert manager.config.user == "factory-user"
    assert manager.config.password == "factory-password"
    assert manager.config.port == 8888
    assert manager.config.sslmode == "verify-full"
    
    # Cleanup
    manager.close_all_connections()


def test_connection_manager_logging():
    """Test that connection manager uses proper logging"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    with patch('python.redshift_connector.logging.getLogger') as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        manager = RedshiftConnectionManager(config, pool_size=1)
        
        # Verify logger was created with correct name
        mock_get_logger.assert_called_with('python.redshift_connector')
        assert manager.logger == mock_logger
        
        # Cleanup
        manager.close_all_connections()


def test_factory_function_with_kwargs():
    """Test factory function with additional kwargs"""
    manager = create_redshift_connection(
        host="kwargs-host",
        database="kwargs-db",
        user="kwargs-user",
        password="kwargs-password",
        connect_timeout=120,
        application_name="test-app"
    )
    
    assert manager.config.connect_timeout == 120
    assert manager.config.application_name == "test-app"
    
    # Cleanup
    manager.close_all_connections()


def test_connection_manager_destructor():
    """Test that __del__ method calls close_all_connections"""
    config = ConnectionConfig(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-password"
    )
    
    with patch.object(RedshiftConnectionManager, 'close_all_connections') as mock_close:
        manager = RedshiftConnectionManager(config, pool_size=1)
        del manager
        mock_close.assert_called_once()