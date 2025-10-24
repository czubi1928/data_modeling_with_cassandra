"""Tests for data transformation module."""

import csv
from pathlib import Path

from src.etl.transform import EventDataTransformer


def test_transformer_initialization(temp_output_file):
    """Test transformer initializes correctly."""
    transformer = EventDataTransformer(temp_output_file)
    assert transformer.output_file == Path(temp_output_file)
    assert transformer.skip_empty_artist is True


def test_transform_row_mapping(sample_event_data):
    """Test that transform_row correctly maps columns."""
    transformer = EventDataTransformer("dummy.csv")

    input_row = sample_event_data[0]
    transformed = transformer.transform_row(input_row)

    assert transformed[0] == input_row[0]  # artist
    assert transformed[1] == input_row[2]  # firstName
    assert len(transformed) == 11


def test_should_skip_row_with_empty_artist(sample_event_data):
    """Test that rows with empty artists are skipped."""
    transformer = EventDataTransformer("dummy.csv", skip_empty_artist=True)

    # Row with empty artist (3rd row in sample data)
    empty_artist_row = sample_event_data[2]
    assert transformer.should_skip_row(empty_artist_row) is True

    # Row with artist
    valid_row = sample_event_data[0]
    assert transformer.should_skip_row(valid_row) is False


def test_write_consolidated_csv(temp_output_file, sample_event_data):
    """Test writing consolidated CSV file."""
    transformer = EventDataTransformer(temp_output_file, skip_empty_artist=True)
    rows_written = transformer.write_consolidated_csv(sample_event_data)

    # Should write 2 rows (skipping 1 with empty artist)
    assert rows_written == 2
    assert transformer.rows_skipped == 1

    # Verify file exists and has correct content
    assert Path(temp_output_file).exists()

    with open(temp_output_file, "r", encoding="utf8") as f:
        reader = csv.reader(f)
        lines = list(reader)

        # Header + 2 data rows
        assert len(lines) == 3
        assert lines[0] == transformer.OUTPUT_COLUMNS


def test_transform_complete_process(temp_output_file, sample_event_data):
    """Test complete transformation process."""
    transformer = EventDataTransformer(temp_output_file)
    output_path = transformer.transform(sample_event_data)

    assert output_path == temp_output_file
    assert Path(output_path).exists()
