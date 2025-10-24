"""Cassandra connection management."""

from typing import List, Optional

from cassandra.cluster import Cluster, Session
from loguru import logger


class CassandraConnection:
    """
    Manages Cassandra cluster connections with context manager support.

    Usage:
        with CassandraConnection(['127.0.0.1']) as session:
            session.execute("SELECT * FROM table")
    """

    def __init__(self, hosts: List[str], port: int = 9042, keyspace: Optional[str] = None):
        """
        Initialize Cassandra connection.

        Args:
            hosts: List of Cassandra host addresses
            port: Cassandra port (default: 9042)
            keyspace: Keyspace to use (optional)
        """
        self.hosts = hosts
        self.port = port
        self.keyspace = keyspace
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None

    def connect(self) -> Session:
        """
        Establish connection to Cassandra cluster.

        Returns:
            Cassandra session

        Raises:
            Exception: If connection fails
        """
        try:
            logger.info(f"Connecting to Cassandra at {self.hosts}:{self.port}")
            self.cluster = Cluster(self.hosts, port=self.port)
            self.session = self.cluster.connect()

            if self.keyspace:
                self.session.set_keyspace(self.keyspace)
                logger.info(f"Using keyspace: {self.keyspace}")

            logger.success("Successfully connected to Cassandra")
            return self.session

        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise

    def close(self):
        """Close Cassandra connections gracefully."""
        if self.session:
            self.session.shutdown()
            logger.debug("Session closed")

        if self.cluster:
            self.cluster.shutdown()
            logger.debug("Cluster connection closed")

        logger.info("Cassandra connections closed")

    def __enter__(self):
        """Context manager entry."""
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
