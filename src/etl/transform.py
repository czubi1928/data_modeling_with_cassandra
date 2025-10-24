"""Data transformation and consolidation."""

import csv
from pathlib import Path
from typing import List

from loguru import logger


class EventDataTransformer:
    """Transform and consolidate event data."""

    # Column mapping from raw CSV to transformed output
    COLUMN_MAPPING = {
        "artist": 0,
        "firstName": 2,
        "gender": 3,
        "itemInSession": 4,
        "lastName": 5,
        "length": 6,
        "level": 7,
        "location": 8,
        "sessionId": 12,
        "song": 13,
        "userId": 16,
    }

    OUTPUT_COLUMNS = [
        "artist",
        "firstName",
        "gender",
        "itemInSession",
        "lastName",
        "length",
        "level",
        "location",
        "sessionId",
        "song",
        "userId",
    ]

    def __init__(self, output_file: str, skip_empty_artist: bool = True):
        """
        Initialize transformer.

        Args:
            output_file: Path to output CSV file
            skip_empty_artist: Whether to skip rows with empty artist field
        """
        self.output_file = Path(output_file)
        self.skip_empty_artist = skip_empty_artist
        self.rows_skipped = 0

        # Create output directory if it doesn't exist
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized transformer - Output: {self.output_file}")

    def transform_row(self, row: List[str]) -> List[str]:
        """
        Transform a single row according to the column mapping.

        Args:
            row: Input row data

        Returns:
            Transformed row
        """
        return [
            row[self.COLUMN_MAPPING["artist"]],
            row[self.COLUMN_MAPPING["firstName"]],
            row[self.COLUMN_MAPPING["gender"]],
            row[self.COLUMN_MAPPING["itemInSession"]],
            row[self.COLUMN_MAPPING["lastName"]],
            row[self.COLUMN_MAPPING["length"]],
            row[self.COLUMN_MAPPING["level"]],
            row[self.COLUMN_MAPPING["location"]],
            row[self.COLUMN_MAPPING["sessionId"]],
            row[self.COLUMN_MAPPING["song"]],
            row[self.COLUMN_MAPPING["userId"]],
        ]

    def should_skip_row(self, row: List[str]) -> bool:
        """
        Determine if a row should be skipped.

        Args:
            row: Input row data

        Returns:
            True if row should be skipped, False otherwise
        """
        if self.skip_empty_artist and row[self.COLUMN_MAPPING["artist"]] == "":
            return True
        return False

    def write_consolidated_csv(self, data_rows: List[List[str]]) -> int:
        """
        Write transformed data to consolidated CSV file.

        Args:
            data_rows: List of raw data rows

        Returns:
            Number of rows written
        """
        csv.register_dialect("myDialect", quoting=csv.QUOTE_ALL, skipinitialspace=True)

        rows_written = 0

        with open(self.output_file, "w", encoding="utf8", newline="") as f:
            writer = csv.writer(f, dialect="myDialect")

            # Write header
            writer.writerow(self.OUTPUT_COLUMNS)

            # Write data rows
            for row in data_rows:
                if self.should_skip_row(row):
                    self.rows_skipped += 1
                    continue

                transformed_row = self.transform_row(row)
                writer.writerow(transformed_row)
                rows_written += 1

        logger.info(f"Wrote {rows_written} rows to {self.output_file}")
        if self.rows_skipped > 0:
            logger.info(f"Skipped {self.rows_skipped} rows (empty artist)")

        return rows_written

    def transform(self, data_rows: List[List[str]]) -> str:
        """
        Execute the complete transformation process.

        Args:
            data_rows: Raw data rows to transform

        Returns:
            Path to output file
        """
        logger.info("Starting data transformation...")
        rows_written = self.write_consolidated_csv(data_rows)
        logger.success(f"Transformation completed: {rows_written} rows written")
        return str(self.output_file)
