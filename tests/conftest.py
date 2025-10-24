"""Pytest fixtures and configuration."""

import csv
import tempfile
from pathlib import Path
from unittest.mock import Mock

import pytest


@pytest.fixture
def sample_event_data():
    """Sample event data for testing."""
    return [
        ["Artist1", "John", "M", "1", "Doe", "200.5", "free", "NYC", "100", "Song1", "1"],
        ["Artist2", "Jane", "F", "2", "Smith", "180.3", "paid", "LA", "100", "Song2", "2"],
        ["", "Bob", "M", "3", "Wilson", "150.0", "free", "SF", "101", "Song3", "3"],  # Empty artist
    ]


@pytest.fixture
def temp_csv_file(sample_event_data):
    """Create temporary CSV file with sample data."""
    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".csv", newline="", encoding="utf8"
    ) as f:
        writer = csv.writer(f)
        writer.writerow(
            [
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
        )
        writer.writerows(sample_event_data)
        temp_path = f.name

    yield temp_path

    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def temp_csv_folder(sample_event_data):
    """Create temporary folder with multiple CSV files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create multiple CSV files
        for i in range(3):
            file_path = temp_path / f"event_{i}.csv"
            with open(file_path, "w", newline="", encoding="utf8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
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
                )
                writer.writerows(sample_event_data)

        yield str(temp_path)


@pytest.fixture
def temp_output_file():
    """Create temporary output file path."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
        temp_path = f.name

    yield temp_path

    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def mock_cassandra_session():
    """Mock Cassandra session for testing."""
    session = Mock()
    session.execute = Mock()
    session.set_keyspace = Mock()
    session.shutdown = Mock()
    return session


@pytest.fixture
def mock_cassandra_cluster():
    """Mock Cassandra cluster for testing."""
    cluster = Mock()
    cluster.connect = Mock()
    cluster.shutdown = Mock()
    return cluster


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "cassandra": {
            "hosts": ["127.0.0.1"],
            "port": 9042,
            "keyspace": "test_sparkify",
            "replication": {"class": "SimpleStrategy", "replication_factor": 1},
        },
        "data": {"raw_folder": "data/raw/event_data", "processed_file": "data/events.csv"},
        "etl": {"batch_size": 1000, "skip_empty_artist": True},
        "logging": {"level": "INFO", "file": "logs/test_pipeline.log"},
    }
