"""
Redshift Connection Manager
Provides robust connection management for Redshift operations with retry logic and connection pooling.
"""

import psycopg2
import psycopg2.pool
import logging
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
import time
from dataclasses import dataclass
from enum import Enum

class ConnectionStatus(Enum):
    ACTIVE = "active"
    IDLE = "idle"
    ERROR = "error"

@dataclass
class ConnectionConfig:
    """Configuration for Redshift connection"""
    host: str
    database: str
    user: str
    password: str
    port: int = 5439
    sslmode: str = "require"
    connect_timeout: int = 30
    keepalives: int = 1
    keepalives_idle: int = 300
    keepalives_interval: int = 60
    application_name: str = "redshift_sample"

class RedshiftConnectionManager:
    """
    Manages Redshift connections with connection pooling and retry logic.
    Provides robust connection handling for production environments.
    """
    
    def __init__(self, config: ConnectionConfig, pool_size: int = 5, max_overflow: int = 10):
        """
        Initialize connection manager with configuration
        
        Args:
            config: Connection configuration
            pool_size: Number of connections in the pool
            max_overflow: Maximum number of additional connections
        """
        self.config = config
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool = None
        self.logger = logging.getLogger(__name__)
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize connection pool"""
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.pool_size,
                maxconn=self.pool_size + self.max_overflow,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                sslmode=self.config.sslmode,
                connect_timeout=self.config.connect_timeout,
                keepalives=self.config.keepalives,
                keepalives_idle=self.config.keepalives_idle,
                keepalives_interval=self.config.keepalives_interval,
                application_name=self.config.application_name
            )
            self.logger.info(f"Connection pool initialized with {self.pool_size} connections")
        except Exception as e:
            self.logger.error(f"Failed to initialize connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self, timeout: int = 30):
        """
        Get a connection from the pool with timeout
        
        Args:
            timeout: Timeout in seconds to get a connection
            
        Yields:
            Database connection
        """
        connection = None
        start_time = time.time()
        
        try:
            # Wait for connection with timeout
            while time.time() - start_time < timeout:
                try:
                    connection = self.pool.getconn()
                    connection.autocommit = False  # Enable transactions
                    self.logger.debug("Connection acquired from pool")
                    yield connection
                    break
                except psycopg2.pool.PoolError:
                    time.sleep(0.1)  # Wait before retrying
            else:
                raise TimeoutError(f"Failed to get connection within {timeout} seconds")
                
        except Exception as e:
            self.logger.error(f"Error getting connection: {e}")
            if connection:
                self.pool.putconn(connection)
            raise
        finally:
            if connection:
                self.pool.putconn(connection)
                self.logger.debug("Connection returned to pool")
    
    def execute_query(self, query: str, params: Optional[tuple] = None, 
                     timeout: int = 30) -> List[Dict[str, Any]]:
        """
        Execute a query and return results
        
        Args:
            query: SQL query to execute
            params: Query parameters
            timeout: Timeout in seconds
            
        Returns:
            List of result rows as dictionaries
        """
        with self.get_connection(timeout) as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, params or ())
                    
                    # Check if it's a SELECT query
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        results = []
                        for row in cursor.fetchall():
                            results.append(dict(zip(columns, row)))
                        return results
                    else:
                        conn.commit()
                        return []
                        
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Query execution failed: {e}")
                raise
    
    def execute_batch(self, query: str, params_list: List[tuple], 
                     timeout: int = 60) -> int:
        """
        Execute a batch of queries efficiently
        
        Args:
            query: SQL query with placeholders
            params_list: List of parameter tuples
            timeout: Timeout in seconds
            
        Returns:
            Number of rows affected
        """
        with self.get_connection(timeout) as conn:
            try:
                with conn.cursor() as cursor:
                    # Use executemany for batch operations
                    cursor.executemany(query, params_list)
                    conn.commit()
                    
                    # Get row count
                    if cursor.rowcount > 0:
                        self.logger.info(f"Batch operation affected {cursor.rowcount} rows")
                    return cursor.rowcount
                    
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Batch execution failed: {e}")
                raise
    
    def execute_procedure(self, procedure_name: str, params: Optional[tuple] = None,
                         timeout: int = 30) -> List[Dict[str, Any]]:
        """
        Execute a stored procedure
        
        Args:
            procedure_name: Name of the procedure
            params: Procedure parameters
            timeout: Timeout in seconds
            
        Returns:
            Procedure results as dictionaries
        """
        with self.get_connection(timeout) as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.callproc(procedure_name, params or ())
                    
                    # Try to get results
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        results = []
                        for row in cursor.fetchall():
                            results.append(dict(zip(columns, row)))
                        return results
                    else:
                        conn.commit()
                        return []
                        
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Procedure execution failed: {e}")
                raise
    
    def test_connection(self) -> bool:
        """
        Test database connection
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            with self.get_connection(10) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return cursor.fetchone()[0] == 1
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def get_connection_status(self) -> Dict[str, Any]:
        """
        Get connection pool status
        
        Returns:
            Dictionary with pool statistics
        """
        if not self.pool:
            return {"status": "not_initialized"}
        
        return {
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "status": "active",
            "minconn": self.pool.minconn,
            "maxconn": self.pool.maxconn
        }
    
    def close_all_connections(self):
        """Close all connections in the pool"""
        if self.pool:
            self.pool.closeall()
            self.logger.info("All connections closed")
    
    def __del__(self):
        """Cleanup when object is destroyed"""
        self.close_all_connections()

# Factory function for easy initialization
def create_redshift_connection(host: str, database: str, user: str, 
                              password: str, **kwargs) -> RedshiftConnectionManager:
    """
    Create a Redshift connection manager with default configuration
    
    Args:
        host: Redshift host
        database: Database name
        user: Database user
        password: Database password
        **kwargs: Additional configuration options
        
    Returns:
        Configured RedshiftConnectionManager instance
    """
    config = ConnectionConfig(
        host=host,
        database=database,
        user=user,
        password=password,
        **kwargs
    )
    return RedshiftConnectionManager(config)

# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Example configuration
    config = ConnectionConfig(
        host="your-redshift-cluster.redshift.amazonaws.com",
        database="dev",
        user="admin",
        password="your-password",
        sslmode="require"
    )
    
    # Create connection manager
    conn_manager = RedshiftConnectionManager(config)
    
    # Test connection
    if conn_manager.test_connection():
        print("Connection successful!")
        
        # Execute a simple query
        results = conn_manager.execute_query("SELECT version()")
        print(f"Redshift version: {results[0]['version']}")
        
        # Close connections
        conn_manager.close_all_connections()
    else:
        print("Connection failed!")