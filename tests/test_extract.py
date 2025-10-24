"""Tests for data extraction module."""

from pathlib import Path

import pytest

from src.etl.extract import EventDataExtractor


def test_extractor_initialization_with_valid_folder(temp_csv_folder):
    """Test that extractor initializes correctly with valid folder."""
    extractor = EventDataExtractor(temp_csv_folder)
    assert extractor.data_folder == Path(temp_csv_folder)


def test_extractor_raises_error_for_missing_folder():
    """Test that extractor raises error for non-existent folder."""
    with pytest.raises(FileNotFoundError):
        EventDataExtractor("non_existent_folder")


def test_get_file_paths_discovers_csv_files(temp_csv_folder):
    """Test that get_file_paths discovers all CSV files."""
    extractor = EventDataExtractor(temp_csv_folder)
    file_paths = extractor.get_file_paths()

    assert len(file_paths) == 3
    assert all(fp.suffix == ".csv" for fp in file_paths)


def test_extract_rows_returns_correct_count(temp_csv_folder):
    """Test that extract_rows returns correct number of rows."""
    extractor = EventDataExtractor(temp_csv_folder)
    file_paths = extractor.get_file_paths()
    data_rows = extractor.extract_rows(file_paths)

    # 3 files * 3 rows each = 9 total rows
    assert len(data_rows) == 9


def test_extract_complete_process(temp_csv_folder):
    """Test the complete extraction process."""
    extractor = EventDataExtractor(temp_csv_folder)
    data_rows = extractor.extract()

    assert len(data_rows) == 9
    assert isinstance(data_rows[0], list)


def test_extract_handles_empty_folder():
    """Test extraction with empty folder."""
    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        extractor = EventDataExtractor(temp_dir)
        file_paths = extractor.get_file_paths()
        assert len(file_paths) == 0
