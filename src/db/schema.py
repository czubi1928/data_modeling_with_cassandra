"""Cassandra schema definitions and table creation."""

from cassandra.cluster import Session
from loguru import logger


class CassandraSchema:
    """Manages Cassandra keyspace and table schemas."""

    def __init__(self, session: Session):
        self.session = session

    def create_keyspace(
        self, keyspace: str, replication_class: str = "SimpleStrategy", replication_factor: int = 1
    ):
        """
        Create keyspace if it doesn't exist.

        Args:
            keyspace: Keyspace name
            replication_class: Replication strategy class
            replication_factor: Number of replicas
        """
        query = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH REPLICATION = {{
                'class': '{replication_class}',
                'replication_factor': {replication_factor}
            }}
        """

        try:
            self.session.execute(query)
            logger.info(f"Keyspace '{keyspace}' created/verified")
        except Exception as e:
            logger.error(f"Failed to create keyspace '{keyspace}': {e}")
            raise

    def create_session_item_table(self):
        """
        Create session_item table for Query 1.

        Query: Get song details by sessionId and itemInSession
        Primary Key: (sessionId, itemInSession)
        """
        query = """
            CREATE TABLE IF NOT EXISTS session_item (
                sessionId int,
                itemInSession int,
                artist text,
                song text,
                length float,
                PRIMARY KEY (sessionId, itemInSession)
            )
        """

        try:
            self.session.execute(query)
            logger.info("Table 'session_item' created/verified")
        except Exception as e:
            logger.error(f"Failed to create table 'session_item': {e}")
            raise

    def create_user_session_table(self):
        """
        Create user_session table for Query 2.

        Query: Get user's session history sorted by itemInSession
        Primary Key: ((sessionId, userId), itemInSession)
        """
        query = """
            CREATE TABLE IF NOT EXISTS user_session (
                sessionId int,
                userId int,
                itemInSession int,
                artist text,
                song text,
                firstName text,
                lastName text,
                PRIMARY KEY ((sessionId, userId), itemInSession)
            ) WITH CLUSTERING ORDER BY (itemInSession ASC)
        """

        try:
            self.session.execute(query)
            logger.info("Table 'user_session' created/verified")
        except Exception as e:
            logger.error(f"Failed to create table 'user_session': {e}")
            raise

    def create_user_song_table(self):
        """
        Create user_song table for Query 3.

        Query: Get all users who listened to a specific song
        Primary Key: (song, userId)
        """
        query = """
            CREATE TABLE IF NOT EXISTS user_song (
                song text,
                userId int,
                firstName text,
                lastName text,
                PRIMARY KEY (song, userId)
            )
        """

        try:
            self.session.execute(query)
            logger.info("Table 'user_song' created/verified")
        except Exception as e:
            logger.error(f"Failed to create table 'user_song': {e}")
            raise

    def create_all_tables(self):
        """Create all required tables for the ETL pipeline."""
        logger.info("Creating all tables...")
        self.create_session_item_table()
        self.create_user_session_table()
        self.create_user_song_table()
        logger.success("All tables created successfully")

    def drop_all_tables(self):
        """Drop all tables (useful for cleanup)."""
        tables = ["session_item", "user_session", "user_song"]

        for table in tables:
            try:
                self.session.execute(f"DROP TABLE IF EXISTS {table}")
                logger.info(f"Table '{table}' dropped")
            except Exception as e:
                logger.error(f"Failed to drop table '{table}': {e}")
