"""Data extraction from CSV files."""

import csv
from pathlib import Path
from typing import List

from loguru import logger


class EventDataExtractor:
    """Extract event data from multiple CSV files."""

    def __init__(self, data_folder: str):
        """
        Initialize extractor.

        Args:
            data_folder: Path to folder containing CSV files

        Raises:
            FileNotFoundError: If data folder doesn't exist
        """
        self.data_folder = Path(data_folder)

        if not self.data_folder.exists():
            raise FileNotFoundError(f"Data folder not found: {data_folder}")

        logger.info(f"Initialized extractor for folder: {self.data_folder}")

    def get_file_paths(self) -> List[Path]:
        """
        Discover all CSV files in the data folder.

        Returns:
            List of Path objects for CSV files
        """
        file_paths = list(self.data_folder.rglob("*.csv"))
        logger.info(f"Found {len(file_paths)} CSV files in {self.data_folder}")

        if not file_paths:
            logger.warning("No CSV files found in the data folder")

        return file_paths

    def extract_rows(self, file_paths: List[Path]) -> List[List[str]]:
        """
        Extract all data rows from CSV files.

        Args:
            file_paths: List of CSV file paths

        Returns:
            List of data rows (each row is a list of strings)

        Raises:
            Exception: If file reading fails
        """
        data_rows = []
        files_processed = 0

        for file_path in file_paths:
            try:
                with open(file_path, "r", encoding="utf8", newline="") as csv_file:
                    csv_reader = csv.reader(csv_file)
                    next(csv_reader)  # Skip header

                    file_row_count = 0
                    for line in csv_reader:
                        data_rows.append(line)
                        file_row_count += 1

                    files_processed += 1
                    logger.debug(f"Processed {file_path.name}: {file_row_count} rows")

            except Exception as e:
                logger.error(f"Failed to read {file_path}: {e}")
                raise

        logger.info(f"Extracted {len(data_rows)} total rows from {files_processed} files")
        return data_rows

    def extract(self) -> List[List[str]]:
        """
        Execute the complete extraction process.

        Returns:
            List of extracted data rows
        """
        logger.info("Starting data extraction...")
        file_paths = self.get_file_paths()
        data_rows = self.extract_rows(file_paths)
        logger.success(f"Data extraction completed: {len(data_rows)} rows")
        return data_rows
