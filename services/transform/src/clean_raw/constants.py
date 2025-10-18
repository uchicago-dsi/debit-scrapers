"""Constants used across the package."""

# Standard library imports
from pathlib import Path

# File paths
PACKAGE_DIR = Path(__file__).resolve().parent

CONFIG_DIR = PACKAGE_DIR / "config"
COUNTRY_MAP_FPATH = CONFIG_DIR / "country_map.json"
CURRENCY_COUNTRY_MAP_FPATH = CONFIG_DIR / "currency_country_map.json"
CURRENCY_MAP_FPATH = CONFIG_DIR / "currency_map.json"
FINANCE_TYPE_MAP_FPATH = CONFIG_DIR / "finance_type_map.json"
SECTOR_MAP_FPATH = CONFIG_DIR / "sector_map.json"
STATUS_MAP_FPATH = CONFIG_DIR / "status_map.json"

RUNTIME_DIR = Path.cwd()
INPUT_DIR = RUNTIME_DIR / "input"
OUTPUT_DIR = RUNTIME_DIR / "output"

# Cloud providers
GOOGLE_CLOUD_URI_SCHEME = "gs://"

# Environment
ENV = "ENV"
DEV = "dev"
