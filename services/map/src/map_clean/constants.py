"""Constants used across the package."""

# Standard library imports
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = Path.cwd()
INPUT_DIR = RUNTIME_DIR / "input"
OUTPUT_DIR = RUNTIME_DIR / "output"
