"""Provides fixtures common to all tests.
"""

import json
import pytest
from pipeline.constants import CONFIG_DIR_PATH
from pipeline.services.web import DataRequestClient
from typing import List

  
class ScraperFixturesMixin:
    """A mixin providing fixtures for scraper tests.
    """

    @pytest.fixture()
    def user_agent_headers(self) -> List[str]:
        """User agent HTTP headers to use to avoid throttling.
        """
        with open(f"{CONFIG_DIR_PATH}/user_agent_headers.json", "r") as stream:
            try:
                return json.load(stream)
            except json.JSONDecodeError as e:
                raise RuntimeError("Failed to open user agent headers "
                                   f"configuration file. {e}") from None

    @pytest.fixture()
    def data_request_client(
        self, 
        user_agent_headers: List[str]) -> DataRequestClient:
        """Client to make HTTP requests while rotating headers.
        """
        return DataRequestClient(user_agent_headers)
  