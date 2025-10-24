# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-10-24

### Added
- **Modular Python Architecture**: Refactored monolithic notebook into reusable modules
  - `src/etl/`: Extract, Transform, Load modules
  - `src/db/`: Connection and schema management
  - `src/utils/`: Configuration and logging utilities
- **Comprehensive Testing Suite**: pytest-based tests with fixtures and mocks
  - Unit tests for extraction, transformation, and loading
  - Test coverage for error handling
  - Mock Cassandra sessions for isolated testing
- **CLI Interface**: Command-line tool with Click for pipeline execution
  - Configurable log levels
  - Custom configuration file support
  - Dry-run mode
- **Structured Logging**: loguru-based logging system
  - Console and file output
  - Log rotation (10MB) and retention (30 days)
  - Execution metrics and performance tracking
- **Configuration Management**: YAML-based configuration
  - Centralized settings for Cassandra, data paths, and ETL options
  - Environment-specific configurations
- **Development Tools**:
  - pre-commit hooks for code quality
  - pytest.ini for test configuration
  - requirements-dev.txt for development dependencies
- **Enhanced Documentation**:
  - Problem statement and business context
  - Performance benchmarks
  - Architecture decisions section
  - Detailed lessons learned
  - Future enhancements roadmap

### Changed
- **requirements.txt**: Cleaned up and categorized dependencies
- **README.md**: Complete rewrite with:
  - Project highlights section
  - Performance metrics
  - Query use cases with business context
  - Development setup instructions
  - Project structure overview

### Fixed
- **Query 3 Bug**: Fixed notebook cell that was executing `select_query_2` instead of `select_query_3`
- **Requirements File**: Regenerated clean requirements.txt without encoding issues

## [0.1.0] - 2025-10-20

### Added
- Initial project structure
- Jupyter notebook with ETL pipeline
- Docker Compose configuration for Cassandra
- Basic README with setup instructions
- Three Cassandra table designs for specific queries
- CSV data extraction and transformation logic
