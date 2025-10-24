# Makefile for Cassandra ETL Pipeline
# Usage: make <target>

.PHONY: help install install-dev test test-cov lint format clean run docker-up docker-down

help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	pip install -r requirements.txt

install-dev: ## Install development dependencies
	pip install -r requirements-dev.txt
	pre-commit install

test: ## Run tests
	pytest tests/ -v

test-cov: ## Run tests with coverage report
	pytest tests/ --cov=src --cov-report=html --cov-report=term-missing

lint: ## Run linters
	ruff check src/ tests/
	black --check src/ tests/

format: ## Format code
	black src/ tests/ scripts/
	isort src/ tests/ scripts/
	ruff check --fix src/ tests/

clean: ## Clean temporary files
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov/
	rm -f .coverage

run: ## Run the ETL pipeline
	python scripts/run_pipeline.py

run-debug: ## Run pipeline with debug logging
	python scripts/run_pipeline.py --log-level DEBUG

docker-up: ## Start Cassandra container
	docker compose up -d

docker-down: ## Stop Cassandra container
	docker compose down

docker-logs: ## View Cassandra logs
	docker compose logs -f cassandra

setup: install-dev docker-up ## Complete setup (install deps + start Docker)
	@echo "Setup complete! Run 'make run' to execute the pipeline."
