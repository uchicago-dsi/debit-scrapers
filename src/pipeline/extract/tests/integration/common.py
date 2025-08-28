"""Provides fixtures common to all tests."""

# Standard library imports
import json
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
    def user_agent_headers(self) -> list[str]:
        """User agent HTTP headers to use to avoid throttling."""
        with open(settings.USER_AGENT_HEADERS_FPATH) as stream:
            try:
                return json.load(stream)
            except json.JSONDecodeError as e:
                raise RuntimeError(
                    f"Failed to open user agent headers configuration file. {e}"
                ) from None

    @pytest.fixture()
    def data_request_client(self, user_agent_headers: list[str]) -> DataRequestClient:
        """Client to make HTTP requests while rotating headers."""
        return DataRequestClient(user_agent_headers)
