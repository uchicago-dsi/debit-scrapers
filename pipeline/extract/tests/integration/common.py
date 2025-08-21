"""Provides fixtures common to all tests."""

# Standard library imports
import json
from typing import List

# Third-party imports
import pytest
from django.conf import settings

# Application imports
from common.web import DataRequestClient


class ScraperFixturesMixin:
    """A mixin providing fixtures for scraper tests."""

    @pytest.fixture()
    def user_agent_headers(self) -> List[str]:
        """User agent HTTP headers to use to avoid throttling."""
        with open(settings.USER_AGENT_HEADERS_FPATH) as stream:
            try:
                return json.load(stream)
            except json.JSONDecodeError as e:
                raise RuntimeError(
                    f"Failed to open user agent headers configuration file. {e}"
                ) from None

    @pytest.fixture()
    def data_request_client(self, user_agent_headers: List[str]) -> DataRequestClient:
        """Client to make HTTP requests while rotating headers."""
        return DataRequestClient(user_agent_headers)
