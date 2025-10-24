"""Tests for data loading module."""

from pathlib import Path

import pytest

from src.etl.load import EventDataLoader


def test_loader_initialization_with_valid_file(mock_cassandra_session, temp_csv_file):
    """Test loader initializes correctly with valid file."""
    loader = EventDataLoader(mock_cassandra_session, temp_csv_file)
    assert loader.session == mock_cassandra_session
    assert loader.data_file == Path(temp_csv_file)


def test_loader_raises_error_for_missing_file(mock_cassandra_session):
    """Test that loader raises error for non-existent file."""
    with pytest.raises(FileNotFoundError):
        EventDataLoader(mock_cassandra_session, "non_existent_file.csv")


def test_load_session_item_table(mock_cassandra_session, temp_csv_file):
    """Test loading data into session_item table."""
    loader = EventDataLoader(mock_cassandra_session, temp_csv_file)
    rows_inserted = loader.load_session_item_table()

    # Should have called execute for each non-header row
    assert rows_inserted > 0
    assert mock_cassandra_session.execute.called


def test_load_user_session_table(mock_cassandra_session, temp_csv_file):
    """Test loading data into user_session table."""
    loader = EventDataLoader(mock_cassandra_session, temp_csv_file)
    rows_inserted = loader.load_user_session_table()

    assert rows_inserted > 0
    assert mock_cassandra_session.execute.called


def test_load_user_song_table(mock_cassandra_session, temp_csv_file):
    """Test loading data into user_song table."""
    loader = EventDataLoader(mock_cassandra_session, temp_csv_file)
    rows_inserted = loader.load_user_song_table()

    assert rows_inserted > 0
    assert mock_cassandra_session.execute.called


def test_load_all_tables(mock_cassandra_session, temp_csv_file):
    """Test loading data into all tables."""
    loader = EventDataLoader(mock_cassandra_session, temp_csv_file)
    results = loader.load_all_tables()

    assert "session_item" in results
    assert "user_session" in results
    assert "user_song" in results
    assert all(count > 0 for count in results.values())
