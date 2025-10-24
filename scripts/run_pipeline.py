"""CLI entry point for running the ETL pipeline."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import click
import yaml

from src.etl.pipeline import ETLPipeline
from src.utils.logger import setup_logger


@click.command()
@click.option(
    "--config",
    default="config/config.yaml",
    help="Path to configuration file",
    type=click.Path(exists=True),
)
@click.option(
    "--log-level",
    default="INFO",
    help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
)
@click.option("--dry-run", is_flag=True, help="Run pipeline without loading data into Cassandra")
def main(config: str, log_level: str, dry_run: bool):
    """
    Run the Cassandra ETL Pipeline.

    This script extracts event data from CSV files, transforms it,
    and loads it into Cassandra tables optimized for specific queries.

    Example:
        python scripts/run_pipeline.py
        python scripts/run_pipeline.py --config config/custom.yaml --log-level DEBUG
    """
    # Load configuration
    with open(config, "r") as f:
        config_data = yaml.safe_load(f)

    # Setup logging
    log_file = config_data.get("logging", {}).get("file", "logs/pipeline.log")
    logger = setup_logger(log_file=log_file, level=log_level.upper())

    logger.info(f"Configuration loaded from: {config}")

    if dry_run:
        logger.warning("DRY RUN MODE - Data will not be loaded into Cassandra")
        return

    try:
        # Run pipeline
        pipeline = ETLPipeline(config_data)

        pipeline.run()  # stats = pipeline.run()

        # Exit successfully
        sys.exit(0)

    except Exception as e:
        logger.exception(f"Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
