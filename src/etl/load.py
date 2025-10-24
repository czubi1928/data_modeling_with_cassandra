"""Data loading into Cassandra tables."""

import csv
from pathlib import Path

from cassandra.cluster import Session
from loguru import logger


class EventDataLoader:
    """Load event data into Cassandra tables."""

    def __init__(self, session: Session, data_file: str):
        """
        Initialize loader.

        Args:
            session: Active Cassandra session
            data_file: Path to consolidated CSV file

        Raises:
            FileNotFoundError: If data file doesn't exist
        """
        self.session = session
        self.data_file = Path(data_file)

        if not self.data_file.exists():
            raise FileNotFoundError(f"Data file not found: {data_file}")

        logger.info(f"Initialized loader for file: {self.data_file}")

    def load_session_item_table(self) -> int:
        """
        Load data into session_item table.

        Table supports Query 1: Get song details by sessionId and itemInSession

        Returns:
            Number of rows inserted
        """
        insert_query = """
            INSERT INTO session_item (sessionId, itemInSession, artist, song, length)
            VALUES (%s, %s, %s, %s, %s)
        """

        rows_inserted = 0

        with open(self.data_file, encoding="utf8") as f:
            csv_reader = csv.reader(f)
            next(csv_reader)  # Skip header

            for line in csv_reader:
                try:
                    self.session.execute(
                        insert_query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5]))
                    )
                    rows_inserted += 1
                except Exception as e:
                    logger.error(f"Failed to insert row into session_item: {e}")
                    raise

        logger.info(f"Loaded {rows_inserted} rows into session_item table")
        return rows_inserted

    def load_user_session_table(self) -> int:
        """
        Load data into user_session table.

        Table supports Query 2: Get user's session history sorted by itemInSession

        Returns:
            Number of rows inserted
        """
        insert_query = """
            INSERT INTO user_session
                (sessionId, userId, itemInSession, artist, song, firstName, lastName)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        rows_inserted = 0

        with open(self.data_file, encoding="utf8") as f:
            csv_reader = csv.reader(f)
            next(csv_reader)  # Skip header

            for line in csv_reader:
                try:
                    self.session.execute(
                        insert_query,
                        (
                            int(line[8]),
                            int(line[10]),
                            int(line[3]),
                            line[0],
                            line[9],
                            line[1],
                            line[4],
                        ),
                    )
                    rows_inserted += 1
                except Exception as e:
                    logger.error(f"Failed to insert row into user_session: {e}")
                    raise

        logger.info(f"Loaded {rows_inserted} rows into user_session table")
        return rows_inserted

    def load_user_song_table(self) -> int:
        """
        Load data into user_song table.

        Table supports Query 3: Get all users who listened to a specific song

        Returns:
            Number of rows inserted
        """
        insert_query = """
            INSERT INTO user_song (song, userId, firstName, lastName)
            VALUES (%s, %s, %s, %s)
        """

        rows_inserted = 0

        with open(self.data_file, encoding="utf8") as f:
            csv_reader = csv.reader(f)
            next(csv_reader)  # Skip header

            for line in csv_reader:
                try:
                    self.session.execute(insert_query, (line[9], int(line[10]), line[1], line[4]))
                    rows_inserted += 1
                except Exception as e:
                    logger.error(f"Failed to insert row into user_song: {e}")
                    raise

        logger.info(f"Loaded {rows_inserted} rows into user_song table")
        return rows_inserted

    def load_all_tables(self) -> dict:
        """
        Load data into all Cassandra tables.

        Returns:
            Dictionary with row counts for each table
        """
        logger.info("Starting data load into Cassandra...")

        results = {
            "session_item": self.load_session_item_table(),
            "user_session": self.load_user_session_table(),
            "user_song": self.load_user_song_table(),
        }

        total_rows = sum(results.values())
        logger.success(f"Data load completed: {total_rows} total rows inserted")

        return results
