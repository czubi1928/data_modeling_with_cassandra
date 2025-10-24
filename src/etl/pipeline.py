"""Complete ETL pipeline orchestration."""

import time
from typing import Any, Dict

from loguru import logger

from src.db.connection import CassandraConnection
from src.db.schema import CassandraSchema
from src.etl.extract import EventDataExtractor
from src.etl.load import EventDataLoader
from src.etl.transform import EventDataTransformer


class ETLPipeline:
    """Orchestrates the complete ETL pipeline."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize ETL pipeline.

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.stats = {
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
            "rows_extracted": 0,
            "rows_transformed": 0,
            "rows_loaded": {},
        }

    def run(self) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.

        Returns:
            Dictionary with pipeline execution statistics
        """
        self.stats["start_time"] = time.time()
        logger.info("=" * 60)
        logger.info("STARTING ETL PIPELINE")
        logger.info("=" * 60)

        try:
            # Extract
            logger.info("PHASE 1: EXTRACTION")
            extractor = EventDataExtractor(self.config["data"]["raw_folder"])
            data_rows = extractor.extract()
            self.stats["rows_extracted"] = len(data_rows)

            # Transform
            logger.info("PHASE 2: TRANSFORMATION")
            transformer = EventDataTransformer(
                self.config["data"]["processed_file"],
                skip_empty_artist=self.config["etl"].get("skip_empty_artist", True),
            )
            output_file = transformer.transform(data_rows)
            self.stats["rows_transformed"] = len(data_rows) - transformer.rows_skipped

            # Load
            logger.info("PHASE 3: LOADING INTO CASSANDRA")

            # Connect to Cassandra
            cassandra_config = self.config["cassandra"]
            connection = CassandraConnection(
                hosts=cassandra_config["hosts"], port=cassandra_config.get("port", 9042)
            )

            with connection as session:
                # Create keyspace and tables
                logger.info("Creating keyspace and tables...")
                schema = CassandraSchema(session)
                schema.create_keyspace(
                    keyspace=cassandra_config["keyspace"],
                    replication_class=cassandra_config["replication"]["class"],
                    replication_factor=cassandra_config["replication"]["replication_factor"],
                )
                session.set_keyspace(cassandra_config["keyspace"])
                schema.create_all_tables()

                # Load data
                loader = EventDataLoader(session, output_file)
                load_results = loader.load_all_tables()
                self.stats["rows_loaded"] = load_results

            # Calculate statistics
            self.stats["end_time"] = time.time()
            self.stats["duration_seconds"] = round(
                self.stats["end_time"] - self.stats["start_time"], 2
            )

            # Log summary
            self._log_summary()

            logger.info("=" * 60)
            logger.success("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)

            return self.stats

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise

    def _log_summary(self):
        """Log pipeline execution summary."""
        logger.info("")
        logger.info("=" * 60)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Duration: {self.stats['duration_seconds']} seconds")
        logger.info(f"Rows Extracted: {self.stats['rows_extracted']}")
        logger.info(f"Rows Transformed: {self.stats['rows_transformed']}")
        logger.info("Rows Loaded:")
        for table, count in self.stats["rows_loaded"].items():
            logger.info(f"  - {table}: {count}")
        logger.info(f"Total Rows Loaded: {sum(self.stats['rows_loaded'].values())}")

        if self.stats["duration_seconds"] > 0:
            throughput = self.stats["rows_transformed"] / self.stats["duration_seconds"]
            logger.info(f"Throughput: {throughput:.2f} rows/second")

        logger.info("=" * 60)
