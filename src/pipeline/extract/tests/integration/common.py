"""Provides fixtures common to all tests."""

# Standard library imports
from collections.abc import Generator
from pathlib import Path

# Third-party imports
import pytest
from django.conf import settings

# Application imports
from common.http import DataRequestClient


class ScraperFixturesMixin:
    """A mixin providing fixtures for scraper tests."""

    @pytest.fixture(scope="session", autouse=True)
    def setup_once(self) -> Generator[None, None, None]:
        """This runs once before all tests in the session"""
        Path.mkdir(settings.EXTRACT_TEST_RESULT_DIR, exist_ok=True)
        yield

    @pytest.fixture()
    def data_request_client(self) -> DataRequestClient:
        """Client to make HTTP requests while rotating headers."""
        return DataRequestClient()
