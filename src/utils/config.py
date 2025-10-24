"""Configuration management utilities."""

from pathlib import Path
from typing import Any, Dict

import yaml
from loguru import logger


class Config:
    """Configuration loader and manager."""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self):
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, "r") as f:
            self._config = yaml.safe_load(f)

        logger.info(f"Configuration loaded from {self.config_path}")

    def get(self, key: str, default=None):
        """
        Get configuration value by dot-separated key.

        Args:
            key: Dot-separated key (e.g., 'cassandra.hosts')
            default: Default value if key not found

        Returns:
            Configuration value
        """
        keys = key.split(".")
        value = self._config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default

        return value

    @property
    def cassandra(self) -> Dict[str, Any]:
        """Get Cassandra configuration."""
        return self._config.get("cassandra", {})

    @property
    def data(self) -> Dict[str, Any]:
        """Get data path configuration."""
        return self._config.get("data", {})

    @property
    def etl(self) -> Dict[str, Any]:
        """Get ETL settings."""
        return self._config.get("etl", {})
